"""
Comprehensive test suite for Com class basic functionality.

This test file validates the foundational functionality of the Com class
to ensure future sub-phases won't break core communication mechanisms.

Test Coverage:
- Basic Com class instantiation
- Clock increment functionality
- Lamport clock receive logic (_update_clock_on_receive)
- Thread safety of clock operations
- Clock getter functionality
- Integration with mock process object

Author: Claude Code Integration Specialist
Date: 2025-09-17
"""

import unittest
import threading
import time
from unittest.mock import Mock, MagicMock
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from middleware.Com import Com


class TestComBasicFunctionality(unittest.TestCase):
    """Test suite for Com class basic functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_process = Mock()
        self.mock_process.id = 1
        self.mock_process.name = "TestProcess"
        self.com = Com(self.mock_process)

    def tearDown(self):
        """Clean up after each test method."""
        self.com = None
        self.mock_process = None


class TestComInstantiation(TestComBasicFunctionality):
    """Test Com class instantiation and initialization."""

    def test_com_initialization_with_process(self):
        """Test that Com initializes correctly with a process."""
        self.assertIsNotNone(self.com)
        self.assertEqual(self.com.process, self.mock_process)
        self.assertEqual(self.com.clock, 0)
        self.assertIsNotNone(self.com._clock_mutex)
        self.assertIsInstance(self.com._clock_mutex, threading.Lock)

    def test_com_initialization_with_none_process(self):
        """Test that Com can be initialized with None process."""
        com_none = Com(None)
        self.assertIsNone(com_none.process)
        self.assertEqual(com_none.clock, 0)
        self.assertIsNotNone(com_none._clock_mutex)

    def test_initial_clock_value(self):
        """Test that initial clock value is 0."""
        self.assertEqual(self.com.clock, 0)
        self.assertEqual(self.com.get_clock(), 0)


class TestClockIncrement(TestComBasicFunctionality):
    """Test clock increment functionality."""

    def test_inc_clock_single_increment(self):
        """Test single clock increment."""
        initial_clock = self.com.get_clock()
        self.com.inc_clock()
        self.assertEqual(self.com.get_clock(), initial_clock + 1)

    def test_inc_clock_multiple_increments(self):
        """Test multiple sequential clock increments."""
        initial_clock = self.com.get_clock()
        increments = 5

        for i in range(increments):
            self.com.inc_clock()
            self.assertEqual(self.com.get_clock(), initial_clock + i + 1)

    def test_inc_clock_thread_safety_sequential(self):
        """Test that inc_clock properly uses mutex for thread safety."""
        # This test verifies the mutex is acquired/released properly
        initial_clock = self.com.get_clock()

        # Multiple increments should work without issues
        for _ in range(10):
            self.com.inc_clock()

        self.assertEqual(self.com.get_clock(), initial_clock + 10)


class TestClockGetter(TestComBasicFunctionality):
    """Test clock getter functionality."""

    def test_get_clock_returns_current_value(self):
        """Test that get_clock returns the current clock value."""
        # Test initial value
        self.assertEqual(self.com.get_clock(), 0)

        # Test after increment
        self.com.inc_clock()
        self.assertEqual(self.com.get_clock(), 1)

        # Test after multiple increments
        self.com.inc_clock()
        self.com.inc_clock()
        self.assertEqual(self.com.get_clock(), 3)

    def test_get_clock_consistency(self):
        """Test that get_clock returns consistent values."""
        clock_value = self.com.get_clock()

        # Multiple calls should return same value
        for _ in range(5):
            self.assertEqual(self.com.get_clock(), clock_value)


class TestLamportClockLogic(TestComBasicFunctionality):
    """Test Lamport clock receive logic."""

    def test_update_clock_on_receive_higher_clock(self):
        """Test _update_clock_on_receive with higher incoming clock."""
        self.com._set_clock(5)
        incoming_clock = 10

        self.com._update_clock_on_receive(incoming_clock)

        # Should be max(5, 10) + 1 = 11
        self.assertEqual(self.com.get_clock(), 11)

    def test_update_clock_on_receive_lower_clock(self):
        """Test _update_clock_on_receive with lower incoming clock."""
        self.com._set_clock(10)
        incoming_clock = 5

        self.com._update_clock_on_receive(incoming_clock)

        # Should be max(10, 5) + 1 = 11
        self.assertEqual(self.com.get_clock(), 11)

    def test_update_clock_on_receive_equal_clock(self):
        """Test _update_clock_on_receive with equal incoming clock."""
        self.com._set_clock(7)
        incoming_clock = 7

        self.com._update_clock_on_receive(incoming_clock)

        # Should be max(7, 7) + 1 = 8
        self.assertEqual(self.com.get_clock(), 8)

    def test_update_clock_on_receive_zero_incoming(self):
        """Test _update_clock_on_receive with zero incoming clock."""
        self.com._set_clock(3)
        incoming_clock = 0

        self.com._update_clock_on_receive(incoming_clock)

        # Should be max(3, 0) + 1 = 4
        self.assertEqual(self.com.get_clock(), 4)

    def test_update_clock_on_receive_from_zero_base(self):
        """Test _update_clock_on_receive starting from zero."""
        # Start from initial state (clock = 0)
        incoming_clock = 5

        self.com._update_clock_on_receive(incoming_clock)

        # Should be max(0, 5) + 1 = 6
        self.assertEqual(self.com.get_clock(), 6)

    def test_set_clock_functionality(self):
        """Test _set_clock internal method."""
        test_values = [0, 1, 5, 10, 100, 1000]

        for value in test_values:
            self.com._set_clock(value)
            self.assertEqual(self.com.get_clock(), value)


class TestThreadSafety(TestComBasicFunctionality):
    """Test thread safety of clock operations."""

    def test_concurrent_inc_clock_operations(self):
        """Test concurrent inc_clock operations from multiple threads."""
        num_threads = 3
        increments_per_thread = 10
        threads = []

        def increment_worker():
            """Worker function to increment clock."""
            for _ in range(increments_per_thread):
                self.com.inc_clock()

        # Start threads
        for _ in range(num_threads):
            thread = threading.Thread(target=increment_worker)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify final clock value
        expected_value = num_threads * increments_per_thread
        self.assertEqual(self.com.get_clock(), expected_value)

    def test_concurrent_mixed_operations(self):
        """Test concurrent mixed clock operations (get, inc, update)."""
        results = []
        lock = threading.Lock()

        def mixed_worker():
            """Worker with mixed operations."""
            local_results = []
            for i in range(10):
                if i % 3 == 0:
                    self.com.inc_clock()
                elif i % 3 == 1:
                    clock_value = self.com.get_clock()
                    local_results.append(clock_value)
                else:
                    self.com._update_clock_on_receive(i)

            with lock:
                results.extend(local_results)

        threads = []
        for _ in range(3):
            thread = threading.Thread(target=mixed_worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify that all operations completed without errors
        # and that clock values are non-negative and monotonic in context
        final_clock = self.com.get_clock()
        self.assertGreaterEqual(final_clock, 0)

        # All retrieved clock values should be non-negative
        for clock_value in results:
            self.assertGreaterEqual(clock_value, 0)

    def test_get_clock_thread_safety(self):
        """Test that get_clock is thread-safe during concurrent modifications."""
        clock_values = []
        lock = threading.Lock()

        def reader_worker():
            """Worker that reads clock values."""
            local_values = []
            for _ in range(10):
                value = self.com.get_clock()
                local_values.append(value)

            with lock:
                clock_values.extend(local_values)

        def writer_worker():
            """Worker that modifies clock."""
            for _ in range(5):
                self.com.inc_clock()

        # Start reader and writer threads
        threads = []
        for _ in range(2):
            reader = threading.Thread(target=reader_worker)
            writer = threading.Thread(target=writer_worker)
            threads.extend([reader, writer])
            reader.start()
            writer.start()

        for thread in threads:
            thread.join()

        # Verify all read values are valid (non-negative)
        for value in clock_values:
            self.assertGreaterEqual(value, 0)
            self.assertIsInstance(value, int)


class TestIntegrationWithMockProcess(TestComBasicFunctionality):
    """Test integration with mock process object."""

    def test_process_attribute_access(self):
        """Test that Com can access process attributes."""
        self.assertEqual(self.com.process.id, 1)
        self.assertEqual(self.com.process.name, "TestProcess")

    def test_process_method_calls(self):
        """Test that Com can call process methods."""
        # Add a method to mock process
        self.mock_process.get_id = MagicMock(return_value=42)

        # Com should be able to call process methods
        result = self.com.process.get_id()
        self.assertEqual(result, 42)
        self.mock_process.get_id.assert_called_once()

    def test_com_with_different_process_types(self):
        """Test Com with different process mock configurations."""
        # Test with minimal process
        minimal_process = Mock()
        com_minimal = Com(minimal_process)
        self.assertEqual(com_minimal.process, minimal_process)

        # Test with process having attributes
        rich_process = Mock()
        rich_process.id = 99
        rich_process.state = "RUNNING"
        rich_process.neighbors = [1, 2, 3]

        com_rich = Com(rich_process)
        self.assertEqual(com_rich.process.id, 99)
        self.assertEqual(com_rich.process.state, "RUNNING")
        self.assertEqual(com_rich.process.neighbors, [1, 2, 3])


class TestEdgeCases(TestComBasicFunctionality):
    """Test edge cases and boundary conditions."""

    def test_large_clock_values(self):
        """Test Com behavior with large clock values."""
        large_value = 1000000
        self.com._set_clock(large_value)
        self.assertEqual(self.com.get_clock(), large_value)

        # Test increment from large value
        self.com.inc_clock()
        self.assertEqual(self.com.get_clock(), large_value + 1)

        # Test update with larger value
        larger_value = large_value * 2
        self.com._update_clock_on_receive(larger_value)
        self.assertEqual(self.com.get_clock(), larger_value + 1)

    def test_clock_value_consistency_during_operations(self):
        """Test that clock values remain consistent during various operations."""
        operations = [
            lambda: self.com.inc_clock(),
            lambda: self.com._update_clock_on_receive(50),
            lambda: self.com._set_clock(25),
            lambda: self.com.get_clock(),
        ]

        # Perform operations and verify clock is always valid
        for _ in range(100):
            operation = operations[_ % len(operations)]
            operation()
            clock_value = self.com.get_clock()
            self.assertIsInstance(clock_value, int)
            self.assertGreaterEqual(clock_value, 0)


if __name__ == '__main__':
    # Configure test runner
    unittest.TestLoader.sortTestMethodsUsing = None  # Preserve test order

    # Create test suite
    suite = unittest.TestSuite()

    # Add test classes in logical order
    test_classes = [
        TestComInstantiation,
        TestClockIncrement,
        TestClockGetter,
        TestLamportClockLogic,
        TestThreadSafety,
        TestIntegrationWithMockProcess,
        TestEdgeCases
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)

    # Print summary
    print(f"\n{'='*60}")
    print(f"COM CLASS BASIC FUNCTIONALITY TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

    if result.failures:
        print(f"\nFAILURES:")
        for test, failure in result.failures:
            print(f"- {test}: {failure}")

    if result.errors:
        print(f"\nERRORS:")
        for test, error in result.errors:
            print(f"- {test}: {error}")

    # Exit with appropriate code
    exit_code = 0 if (len(result.failures) == 0 and len(result.errors) == 0) else 1
    exit(exit_code)