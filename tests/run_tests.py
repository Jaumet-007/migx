#!/usr/bin/env python3
"""Run test suite producing a report.

Behavior:
- If `pytest` is installed, run `pytest -q --junitxml=tests/results.xml` and exit with pytest code.
- Otherwise, run tests with `unittest` loader and write a short summary to `tests/results.txt`.

Usage:
    python tests/run_tests.py
"""
import sys
import subprocess
from pathlib import Path


def run_pytest():
    # Prefer pytest if available
    try:
        import pytest  # type: ignore
    except Exception:
        return None

    args = ["-q", "--junitxml=tests/results.xml"]
    # run pytest programmatically
    return pytest.main(args)


def run_unittest_fallback():
    import unittest
    from io import StringIO

    loader = unittest.TestLoader()
    # load tests from the tests package (this file is inside tests/)
    suite = loader.discover(start_dir=str(Path(__file__).parent), pattern="test_*.py")
    stream = StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=2)
    result = runner.run(suite)

    report_path = Path(__file__).parent / "results.txt"
    with report_path.open("w", encoding="utf-8") as fh:
        fh.write(stream.getvalue())
        fh.write("\nSummary:\n")
        fh.write(f"tests_run={result.testsRun}\n")
        fh.write(f"failures={len(result.failures)}\n")
        fh.write(f"errors={len(result.errors)}\n")

    return 0 if result.wasSuccessful() else 1


def main():
    # Ensure we run from repository root so paths align
    repo_root = Path(__file__).resolve().parents[1]
    try:
        # change cwd to repo root
        Path.cwd()
        import os
        os.chdir(repo_root)
    except Exception:
        pass

    code = run_pytest()
    if code is None:
        print("pytest no está disponible — usando unittest de fallback. Generando tests/results.txt")
        rc = run_unittest_fallback()
        sys.exit(rc)
    else:
        # pytest returned an exit code
        if code == 0:
            print("pytest OK — results saved to tests/results.xml")
        else:
            print(f"pytest finished with exit code {code}. See tests/results.xml and pytest output for details.")
        sys.exit(int(code))


if __name__ == "__main__":
    main()
