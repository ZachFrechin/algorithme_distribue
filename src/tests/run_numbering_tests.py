#!/usr/bin/env python3
"""
Test runner script for the automatic process numbering sub-phase validation.

This script demonstrates how to run the comprehensive test suite for Com.py
message handling and Process.py sequential ID assignment functionality.

Usage:
    python run_numbering_tests.py

or in virtual environment:
    source ../venv.project/bin/activate
    python run_numbering_tests.py
"""

import sys
import os
import subprocess

def main():
    """Run the numbering test suite"""
    # Get the directory of this script
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(test_dir, '2025-09-18_com_numbering_test.py')

    print("=" * 70)
    print("Automatic Process Numbering Sub-phase Validation Test Suite")
    print("=" * 70)
    print(f"Running tests from: {test_file}")
    print("")

    try:
        # Run the test file
        result = subprocess.run([sys.executable, test_file],
                              capture_output=True, text=True)

        # Print stdout (test results)
        print(result.stdout)

        # Print stderr if there are errors
        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        # Return appropriate exit code
        return result.returncode

    except Exception as e:
        print(f"Error running tests: {e}")
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)