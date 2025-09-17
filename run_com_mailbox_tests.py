#!/usr/bin/env python3
"""
Test runner script for Com mailbox validation tests.

Usage:
    python run_com_mailbox_tests.py [test_class_name]

Examples:
    python run_com_mailbox_tests.py                                    # Run all tests
    python run_com_mailbox_tests.py TestComMailboxThreadSafety         # Run specific test class
    python run_com_mailbox_tests.py TestComMailboxConcurrentAccess     # Run concurrent tests
"""

import sys
import os
import unittest

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Main test runner function."""

    if len(sys.argv) > 1:
        # Run specific test class
        test_class = sys.argv[1]
        if not test_class.startswith('tests.2025-09-17_com_mailbox_test.'):
            test_class = f'tests.2025-09-17_com_mailbox_test.{test_class}'
        suite = unittest.TestLoader().loadTestsFromName(test_class)
    else:
        # Run all tests in the mailbox test module
        suite = unittest.TestLoader().loadTestsFromName('tests.2025-09-17_com_mailbox_test')

    # Configure test runner
    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        buffer=True
    )

    print("=" * 70)
    print("COM MAILBOX VALIDATION TEST SUITE")
    print("=" * 70)
    print(f"Running tests from: {os.path.abspath('src/tests/2025-09-17_com_mailbox_test.py')}")
    print("=" * 70)

    # Run tests
    result = runner.run(suite)

    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.splitlines()[-1]}")

    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.splitlines()[-1]}")

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == '__main__':
    main()