import subprocess
import sys
import pathlib

project_root = pathlib.Path(__file__).resolve().parent.parent
target_directory = pathlib.Path("src")
full_target_directory = project_root / target_directory

def run_lint():
    try:
        subprocess.run(["ruff", "check", full_target_directory], check=True)
        print("Linting passed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Linting failed: {e}")
        sys.exit(e.returncode)

def run_ruff_format():
    try:
        subprocess.run([sys.executable, "-m", "ruff", "format", full_target_directory], check=True)
        print("Ruff formatting completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Ruff formatting failed: {e}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    run_lint()
    run_ruff_format()