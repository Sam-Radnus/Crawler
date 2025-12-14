from enum import Enum
from typing import Callable, List, Any, Optional
from dataclasses import dataclass, field
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TransactionStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    RETRY_SCHEDULED = "retry_scheduled"


@dataclass
class TransactionStep:
    name: str
    execute: Callable
    rollback: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    result: Any = None
    error: Optional[Exception] = None
    executed: bool = False


@dataclass
class TransactionResult:
    status: TransactionStatus
    completed_steps: List[str]
    failed_step: Optional[str]
    error: Optional[Exception]
    transaction_id: str
    timestamp: datetime
    retry_count: int = 0


class TransactionManager:
    def __init__(self, redis_service, transaction_id: str):
        self.redis_service = redis_service
        self.transaction_id = transaction_id
        self.steps: List[TransactionStep] = []
        self.completed_steps: List[TransactionStep] = []
        self.status = TransactionStatus.PENDING
        
    def add_step(
        self,
        name: str,
        execute: Callable,
        rollback: Callable,
        *args,
        **kwargs
    ):
        step = TransactionStep(
            name=name,
            execute=execute,
            rollback=rollback,
            args=args,
            kwargs=kwargs
        )
        self.steps.append(step)
        return self
    
    def execute(self) -> TransactionResult:
        self.status = TransactionStatus.IN_PROGRESS
        self._update_redis_status()
        
        failed_step = None
        error = None
        
        try:
            for step in self.steps:
                logger.info(f"Executing step: {step.name}")
                try:
                    step.result = step.execute(*step.args, **step.kwargs)
                    step.executed = True
                    self.completed_steps.append(step)
                    self._log_step_completion(step.name)
                except Exception as e:
                    logger.error(f"Step {step.name} failed: {str(e)}")
                    step.error = e
                    failed_step = step.name
                    error = e
                    raise
            
            self.status = TransactionStatus.COMPLETED
            self._update_redis_status()
            
            return TransactionResult(
                status=TransactionStatus.COMPLETED,
                completed_steps=[s.name for s in self.completed_steps],
                failed_step=None,
                error=None,
                transaction_id=self.transaction_id,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Transaction failed, rolling back. Error: {str(e)}")
            self._rollback()
            retry_count = self._schedule_retry()
            
            return TransactionResult(
                status=TransactionStatus.RETRY_SCHEDULED,
                completed_steps=[s.name for s in self.completed_steps],
                failed_step=failed_step,
                error=error,
                transaction_id=self.transaction_id,
                timestamp=datetime.now(),
                retry_count=retry_count
            )
    
    def _rollback(self):
        self.status = TransactionStatus.FAILED
        logger.info(f"Rolling back {len(self.completed_steps)} steps")
        
        for step in reversed(self.completed_steps):
            try:
                logger.info(f"Rolling back step: {step.name}")
                step.rollback(*step.args, **step.kwargs)
                self._log_rollback_completion(step.name)
            except Exception as e:
                logger.error(f"Rollback failed for {step.name}: {str(e)}")
        
        self.status = TransactionStatus.ROLLED_BACK
        self._update_redis_status()
    
    def _schedule_retry(self) -> int:
        retry_key = f"transaction:retry:{self.transaction_id}"
        current_retries = self.redis_service.client.get(retry_key)
        retry_count = int(current_retries) + 1 if current_retries else 1
        
        self.redis_service.client.set(retry_key, retry_count)
        
        retry_queue_key = "transaction:retry_queue"
        self.redis_service.enqueue_url(retry_queue_key, self.transaction_id, priority=retry_count)
        
        self.status = TransactionStatus.RETRY_SCHEDULED
        self._update_redis_status()
        
        logger.info(f"Scheduled retry {retry_count} for transaction {self.transaction_id}")
        return retry_count
    
    def _update_redis_status(self):
        status_key = f"transaction:status:{self.transaction_id}"
        self.redis_service.client.set(status_key, self.status.value)
        
        metadata = {
            "status": self.status.value,
            "completed_steps": ",".join([s.name for s in self.completed_steps]),
            "total_steps": str(len(self.steps)),
            "last_update": datetime.now().isoformat()
        }
        meta_key = f"transaction:meta:{self.transaction_id}"
        self.redis_service.client.hset(meta_key, mapping=metadata)
    
    def _log_step_completion(self, step_name: str):
        log_key = f"transaction:log:{self.transaction_id}"
        log_entry = f"{datetime.now().isoformat()} - COMPLETED: {step_name}"
        self.redis_service.client.rpush(log_key, log_entry)
    
    def _log_rollback_completion(self, step_name: str):
        log_key = f"transaction:log:{self.transaction_id}"
        log_entry = f"{datetime.now().isoformat()} - ROLLED_BACK: {step_name}"
        self.redis_service.client.rpush(log_key, log_entry)