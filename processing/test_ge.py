import sys
from processing.utils.quality_checks import run_silver_suite

if __name__ == "__main__":
    try:
        run_silver_suite("2026-04-26")
        print("SUCCESS")
    except Exception as e:
        print(f"FAILED: {e}")
        sys.exit(1)
