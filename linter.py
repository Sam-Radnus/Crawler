import subprocess
import sys
from pathlib import Path


def lint_all_files(base_dir="."):
    base_path = Path(base_dir)
    python_files = [
        f for f in base_path.rglob("*.py")
        if "venv" not in f.parts
    ]

    if not python_files:
        print("No Python files found.")
        return

    print(f"Checking lint for {len(python_files)} files...")
    result = subprocess.run(
        ["flake8", *map(str, python_files)],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("✅ No linting issues found.")
    else:
        print("⚠️ Linting issues detected:")
        print(result.stdout)
        print(result.stderr, file=sys.stderr)


if __name__ == "__main__":
    lint_all_files(".")
