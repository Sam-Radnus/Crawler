import random
from typing import Literal


Priority = Literal[1, 2, 3, 4, 5]


class Prioritizer:
    """Assigns priorities to URLs. For now, random 1-5 (5 = highest)."""

    def assign_priority(self, url: str) -> Priority:
        return random.randint(1, 5)  # type: ignore[return-value]


