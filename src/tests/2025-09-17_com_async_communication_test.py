#!/usr/bin/env python3
"""
Comprehensive test suite for Com class async communication methods.

This test file validates the async communication functionality of the Com class,
including broadcast and send_to methods, their integration with PyEventBus3,
Lamport clock management, and Message hierarchy interaction.

Test Categories:
- Broadcast method functionality and message creation
- SendTo method functionality with destination handling
- Lamport clock increment on message sending
- Process validation (error when process is None)
- Message timestamp consistency
- PyEventBus3 integration and message posting
- Integration with BroadcastMessage and DedicatedMessage types
- Source attribution (messages contain correct process name)
- Error handling for invalid inputs
- Mock PyEventBus testing to verify messages are posted correctly

Author: Claude Code Integration Specialist
Date: 2025-09-17
"""

import unittest
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call
from threading import Thread
import time

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from middleware.Com import Com
from Message import Message, BroadcastMessage, DedicatedMessage


class MockProcess:
    """Mock process class to simulate a distributed process."""

    def __init__(self, name):
        self.name = name
        self.com = Com(self)


class TestComAsyncCommunication(unittest.TestCase):
    """
    Test suite for Com class async communication methods.

    Tests the integration of broadcast and send_to methods with PyEventBus3,
    Lamport clock management, and message hierarchy.
    """

    def setUp(self):
        """Set up test fixtures with mock processes and Com instances."""
        self.process_p0 = MockProcess("P0")
        self.process_p1 = MockProcess("P1")
        self.process_p2 = MockProcess("P2")

        self.com_p0 = self.process_p0.com
        self.com_p1 = self.process_p1.com
        self.com_p2 = self.process_p2.com

        # Set initial clock values for predictable testing
        self.com_p0._set_clock(0)
        self.com_p1._set_clock(5)
        self.com_p2._set_clock(10)

    def tearDown(self):
        """Clean up after each test."""
        pass

    # =========================================================================
    # Broadcast Method Tests
    # =========================================================================

    def test_broadcast_basic_functionality(self):
        """Test basic broadcast method functionality."""
        payload = "Hello World"
        initial_clock = self.com_p0.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p0.broadcast(payload)

            # Verify clock was incremented
            self.assertEqual(self.com_p0.get_clock(), initial_clock + 1)

            # Verify PyBus.Instance() was called
            mock_pybus_instance.assert_called_once()

            # Verify message was posted to the bus
            mock_bus.post.assert_called_once()

            # Get the posted message
            posted_message = mock_bus.post.call_args[0][0]

            # Verify message properties
            self.assertIsInstance(posted_message, BroadcastMessage)
            self.assertEqual(posted_message.payload, payload)
            self.assertEqual(posted_message.stamp, initial_clock + 1)
            self.assertEqual(posted_message.source, "P0")
            self.assertTrue(posted_message.USER_MESSAGE)
            self.assertFalse(posted_message.SYSTEM_MESSAGE)

    def test_broadcast_clock_increment_sequence(self):
        """Test that multiple broadcasts increment clock sequentially."""
        payloads = ["Message1", "Message2", "Message3"]
        initial_clock = self.com_p1.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            timestamps = []
            for i, payload in enumerate(payloads):
                self.com_p1.broadcast(payload)
                posted_message = mock_bus.post.call_args[0][0]
                timestamps.append(posted_message.stamp)

                # Verify clock increments correctly
                expected_clock = initial_clock + i + 1
                self.assertEqual(self.com_p1.get_clock(), expected_clock)
                self.assertEqual(posted_message.stamp, expected_clock)

            # Verify timestamps are sequential
            self.assertEqual(timestamps, [initial_clock + 1, initial_clock + 2, initial_clock + 3])

            # Verify all messages were posted
            self.assertEqual(mock_bus.post.call_count, 3)

    def test_broadcast_without_process_raises_error(self):
        """Test that broadcast raises ValueError when process is None."""
        com_no_process = Com()  # No process set

        with self.assertRaises(ValueError) as context:
            com_no_process.broadcast("test payload")

        self.assertEqual(str(context.exception), "Process is not set")

        # Verify clock was not incremented
        self.assertEqual(com_no_process.get_clock(), 0)

    def test_broadcast_with_none_payload(self):
        """Test broadcast with None payload."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p0.broadcast(None)

            posted_message = mock_bus.post.call_args[0][0]
            self.assertIsNone(posted_message.payload)
            self.assertEqual(posted_message.source, "P0")

    def test_broadcast_with_complex_payload(self):
        """Test broadcast with complex payload types."""
        payloads = [
            {"type": "data", "value": 42},
            [1, 2, 3, 4, 5],
            ("tuple", "data"),
            123.456
        ]

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            for payload in payloads:
                mock_bus.reset_mock()
                self.com_p0.broadcast(payload)

                posted_message = mock_bus.post.call_args[0][0]
                self.assertEqual(posted_message.payload, payload)
                self.assertIsInstance(posted_message, BroadcastMessage)

    # =========================================================================
    # Send To Method Tests
    # =========================================================================

    def test_send_to_basic_functionality(self):
        """Test basic send_to method functionality."""
        payload = "Hello P1"
        destination = "P1"
        initial_clock = self.com_p0.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p0.send_to(payload, destination)

            # Verify clock was incremented
            self.assertEqual(self.com_p0.get_clock(), initial_clock + 1)

            # Verify PyBus.Instance() was called
            mock_pybus_instance.assert_called_once()

            # Verify message was posted to the bus
            mock_bus.post.assert_called_once()

            # Get the posted message
            posted_message = mock_bus.post.call_args[0][0]

            # Verify message properties
            self.assertIsInstance(posted_message, DedicatedMessage)
            self.assertEqual(posted_message.payload, payload)
            self.assertEqual(posted_message.stamp, initial_clock + 1)
            self.assertEqual(posted_message.source, "P0")
            self.assertEqual(posted_message.dest, destination)
            self.assertTrue(posted_message.USER_MESSAGE)
            self.assertFalse(posted_message.SYSTEM_MESSAGE)

    def test_send_to_multiple_destinations(self):
        """Test send_to with multiple different destinations."""
        destinations = ["P1", "P2", "P3", "P0"]
        payload_base = "Message to "
        initial_clock = self.com_p2.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            for i, dest in enumerate(destinations):
                mock_bus.reset_mock()
                payload = f"{payload_base}{dest}"

                self.com_p2.send_to(payload, dest)

                posted_message = mock_bus.post.call_args[0][0]

                # Verify message properties
                self.assertEqual(posted_message.payload, payload)
                self.assertEqual(posted_message.dest, dest)
                self.assertEqual(posted_message.source, "P2")
                self.assertEqual(posted_message.stamp, initial_clock + i + 1)

    def test_send_to_without_process_raises_error(self):
        """Test that send_to raises ValueError when process is None."""
        com_no_process = Com()  # No process set

        with self.assertRaises(ValueError) as context:
            com_no_process.send_to("test payload", "P1")

        self.assertEqual(str(context.exception), "Process is not set")

        # Verify clock was not incremented
        self.assertEqual(com_no_process.get_clock(), 0)

    def test_send_to_with_none_destination(self):
        """Test send_to with None destination."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p0.send_to("test", None)

            posted_message = mock_bus.post.call_args[0][0]
            self.assertIsNone(posted_message.dest)
            self.assertEqual(posted_message.payload, "test")

    def test_send_to_clock_increment_sequence(self):
        """Test that multiple send_to calls increment clock sequentially."""
        messages = [
            ("Msg1", "P1"),
            ("Msg2", "P2"),
            ("Msg3", "P1"),
            ("Msg4", "P3")
        ]
        initial_clock = self.com_p0.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            timestamps = []
            for i, (payload, dest) in enumerate(messages):
                self.com_p0.send_to(payload, dest)
                posted_message = mock_bus.post.call_args[0][0]
                timestamps.append(posted_message.stamp)

                # Verify clock increments correctly
                expected_clock = initial_clock + i + 1
                self.assertEqual(self.com_p0.get_clock(), expected_clock)
                self.assertEqual(posted_message.stamp, expected_clock)

            # Verify timestamps are sequential
            expected_timestamps = [initial_clock + i + 1 for i in range(len(messages))]
            self.assertEqual(timestamps, expected_timestamps)

    # =========================================================================
    # Lamport Clock Tests
    # =========================================================================

    def test_lamport_clock_consistency_across_methods(self):
        """Test Lamport clock consistency across broadcast and send_to methods."""
        initial_clock = self.com_p1.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Mix of broadcast and send_to operations
            operations = [
                ("broadcast", "Broadcast1"),
                ("send_to", "Message1", "P0"),
                ("broadcast", "Broadcast2"),
                ("send_to", "Message2", "P2"),
                ("send_to", "Message3", "P1")
            ]

            expected_clock = initial_clock
            for operation in operations:
                expected_clock += 1

                if operation[0] == "broadcast":
                    self.com_p1.broadcast(operation[1])
                else:  # send_to
                    self.com_p1.send_to(operation[1], operation[2])

                # Verify clock incremented
                self.assertEqual(self.com_p1.get_clock(), expected_clock)

                # Verify message timestamp
                posted_message = mock_bus.post.call_args[0][0]
                self.assertEqual(posted_message.stamp, expected_clock)

    def test_concurrent_message_sending_clock_safety(self):
        """Test clock safety under concurrent message sending."""
        num_messages = 10
        initial_clock = self.com_p0.get_clock()

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            def send_messages():
                for i in range(num_messages // 2):
                    self.com_p0.broadcast(f"broadcast_{i}")
                    self.com_p0.send_to(f"message_{i}", "P1")

            # Create multiple threads to test concurrent access
            threads = []
            for _ in range(2):
                thread = Thread(target=send_messages)
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify final clock value
            expected_final_clock = initial_clock + (num_messages * 2)
            self.assertEqual(self.com_p0.get_clock(), expected_final_clock)

            # Verify all messages were posted
            self.assertEqual(mock_bus.post.call_count, num_messages * 2)

    # =========================================================================
    # PyEventBus3 Integration Tests
    # =========================================================================

    def test_pyeventbus_integration_broadcast(self):
        """Test PyEventBus3 integration for broadcast messages."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Test that PyBus.Instance() is called correctly
            self.com_p0.broadcast("test")

            mock_pybus_instance.assert_called_once()
            mock_bus.post.assert_called_once()

            # Verify the message posted has the correct broadcast method
            posted_message = mock_bus.post.call_args[0][0]
            self.assertTrue(hasattr(posted_message, 'broadcast'))

    def test_pyeventbus_integration_send_to(self):
        """Test PyEventBus3 integration for dedicated messages."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Test that PyBus.Instance() is called correctly
            self.com_p0.send_to("test", "P1")

            mock_pybus_instance.assert_called_once()
            mock_bus.post.assert_called_once()

            # Verify the message posted has the correct send method
            posted_message = mock_bus.post.call_args[0][0]
            self.assertTrue(hasattr(posted_message, 'send'))

    def test_multiple_pybus_instance_calls(self):
        """Test that PyBus.Instance() is called for each message operation."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Send multiple messages
            self.com_p0.broadcast("msg1")
            self.com_p0.send_to("msg2", "P1")
            self.com_p0.broadcast("msg3")

            # Verify PyBus.Instance() was called for each operation
            self.assertEqual(mock_pybus_instance.call_count, 3)
            self.assertEqual(mock_bus.post.call_count, 3)

    # =========================================================================
    # Message Type Integration Tests
    # =========================================================================

    def test_broadcast_message_creation_and_properties(self):
        """Test BroadcastMessage creation and properties."""
        payload = {"data": "test", "value": 123}

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p1.broadcast(payload)

            posted_message = mock_bus.post.call_args[0][0]

            # Test message type and inheritance
            self.assertIsInstance(posted_message, BroadcastMessage)
            self.assertIsInstance(posted_message, Message)

            # Test message properties
            self.assertEqual(posted_message.get_payload(), payload)
            self.assertEqual(posted_message.get_stamp(), posted_message.stamp)
            self.assertTrue(posted_message.USER_MESSAGE)
            self.assertFalse(posted_message.SYSTEM_MESSAGE)

            # Test BroadcastMessage specific behavior
            self.assertTrue(hasattr(posted_message, 'broadcast'))

    def test_dedicated_message_creation_and_properties(self):
        """Test DedicatedMessage creation and properties."""
        payload = "Direct message"
        destination = "P2"

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p1.send_to(payload, destination)

            posted_message = mock_bus.post.call_args[0][0]

            # Test message type and inheritance
            self.assertIsInstance(posted_message, DedicatedMessage)
            self.assertIsInstance(posted_message, Message)

            # Test message properties
            self.assertEqual(posted_message.get_payload(), payload)
            self.assertEqual(posted_message.get_stamp(), posted_message.stamp)
            self.assertEqual(posted_message.get_dest(), destination)
            self.assertTrue(posted_message.USER_MESSAGE)
            self.assertFalse(posted_message.SYSTEM_MESSAGE)

            # Test DedicatedMessage specific behavior
            self.assertTrue(hasattr(posted_message, 'send'))
            self.assertTrue(hasattr(posted_message, 'dest'))

    # =========================================================================
    # Source Attribution Tests
    # =========================================================================

    def test_source_attribution_broadcast(self):
        """Test that broadcast messages have correct source attribution."""
        processes = [self.com_p0, self.com_p1, self.com_p2]
        expected_sources = ["P0", "P1", "P2"]

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            for com, expected_source in zip(processes, expected_sources):
                mock_bus.reset_mock()
                com.broadcast(f"Message from {expected_source}")

                posted_message = mock_bus.post.call_args[0][0]
                self.assertEqual(posted_message.source, expected_source)

    def test_source_attribution_send_to(self):
        """Test that dedicated messages have correct source attribution."""
        processes = [self.com_p0, self.com_p1, self.com_p2]
        expected_sources = ["P0", "P1", "P2"]

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            for com, expected_source in zip(processes, expected_sources):
                mock_bus.reset_mock()
                com.send_to(f"Message from {expected_source}", "TARGET")

                posted_message = mock_bus.post.call_args[0][0]
                self.assertEqual(posted_message.source, expected_source)

    # =========================================================================
    # Error Handling Tests
    # =========================================================================

    def test_broadcast_process_validation_timing(self):
        """Test that process validation happens before clock increment."""
        com_no_process = Com()
        initial_clock = com_no_process.get_clock()

        with self.assertRaises(ValueError):
            com_no_process.broadcast("test")

        # Clock should not have been incremented due to early validation
        self.assertEqual(com_no_process.get_clock(), initial_clock)

    def test_send_to_process_validation_timing(self):
        """Test that process validation happens before clock increment."""
        com_no_process = Com()
        initial_clock = com_no_process.get_clock()

        with self.assertRaises(ValueError):
            com_no_process.send_to("test", "P1")

        # Clock should not have been incremented due to early validation
        self.assertEqual(com_no_process.get_clock(), initial_clock)

    def test_pybus_exception_handling(self):
        """Test behavior when PyBus.Instance() raises exception."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_pybus_instance.side_effect = Exception("PyBus error")

            # The exception should propagate
            with self.assertRaises(Exception) as context:
                self.com_p0.broadcast("test")

            self.assertEqual(str(context.exception), "PyBus error")

            # Clock should still have been incremented before PyBus call
            self.assertEqual(self.com_p0.get_clock(), 1)

    # =========================================================================
    # Integration Scenario Tests
    # =========================================================================

    def test_mixed_communication_scenario(self):
        """Test a realistic mixed communication scenario."""
        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Simulate a distributed algorithm scenario

            # P0 broadcasts initialization message
            self.com_p0.broadcast("ALGORITHM_START")
            init_msg = mock_bus.post.call_args[0][0]
            self.assertIsInstance(init_msg, BroadcastMessage)
            self.assertEqual(init_msg.source, "P0")

            # P1 sends acknowledgment to P0
            mock_bus.reset_mock()
            self.com_p1.send_to("ACK", "P0")
            ack_msg = mock_bus.post.call_args[0][0]
            self.assertIsInstance(ack_msg, DedicatedMessage)
            self.assertEqual(ack_msg.dest, "P0")
            self.assertEqual(ack_msg.source, "P1")

            # P2 broadcasts ready signal
            mock_bus.reset_mock()
            self.com_p2.broadcast("READY")
            ready_msg = mock_bus.post.call_args[0][0]
            self.assertIsInstance(ready_msg, BroadcastMessage)
            self.assertEqual(ready_msg.source, "P2")

            # Verify all operations completed successfully
            self.assertEqual(mock_pybus_instance.call_count, 3)

    def test_timestamp_ordering_across_processes(self):
        """Test timestamp ordering across different processes."""
        # Set different initial clocks to test ordering
        self.com_p0._set_clock(5)
        self.com_p1._set_clock(3)
        self.com_p2._set_clock(8)

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            messages_and_stamps = []

            # Send messages from different processes
            self.com_p0.broadcast("From P0")
            messages_and_stamps.append(("P0", mock_bus.post.call_args[0][0].stamp))

            mock_bus.reset_mock()
            self.com_p1.send_to("From P1", "P2")
            messages_and_stamps.append(("P1", mock_bus.post.call_args[0][0].stamp))

            mock_bus.reset_mock()
            self.com_p2.broadcast("From P2")
            messages_and_stamps.append(("P2", mock_bus.post.call_args[0][0].stamp))

            # Verify timestamps reflect local clocks correctly
            expected_stamps = [6, 4, 9]  # Initial + 1 for each process
            actual_stamps = [stamp for _, stamp in messages_and_stamps]
            self.assertEqual(actual_stamps, expected_stamps)

    # =========================================================================
    # Performance and Edge Case Tests
    # =========================================================================

    def test_large_payload_handling(self):
        """Test handling of large payloads."""
        large_payload = "x" * 10000  # 10KB string

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            # Test broadcast with large payload
            self.com_p0.broadcast(large_payload)
            posted_message = mock_bus.post.call_args[0][0]
            self.assertEqual(posted_message.payload, large_payload)
            self.assertEqual(len(posted_message.payload), 10000)

            # Test send_to with large payload
            mock_bus.reset_mock()
            self.com_p0.send_to(large_payload, "P1")
            posted_message = mock_bus.post.call_args[0][0]
            self.assertEqual(posted_message.payload, large_payload)

    def test_empty_string_payload(self):
        """Test handling of empty string payloads."""
        empty_payload = ""

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            self.com_p0.broadcast(empty_payload)
            posted_message = mock_bus.post.call_args[0][0]
            self.assertEqual(posted_message.payload, "")

            mock_bus.reset_mock()
            self.com_p0.send_to(empty_payload, "P1")
            posted_message = mock_bus.post.call_args[0][0]
            self.assertEqual(posted_message.payload, "")

    def test_unicode_payload_handling(self):
        """Test handling of Unicode payloads."""
        unicode_payloads = [
            "Hello 世界",
            "émojis: 🚀🌟💻",
            "математика",
            "العربية"
        ]

        with patch('pyeventbus3.pyeventbus3.PyBus.Instance') as mock_pybus_instance:
            mock_bus = MagicMock()
            mock_pybus_instance.return_value = mock_bus

            for payload in unicode_payloads:
                mock_bus.reset_mock()
                self.com_p0.broadcast(payload)
                posted_message = mock_bus.post.call_args[0][0]
                self.assertEqual(posted_message.payload, payload)


# =========================================================================
# Test Suite Runner
# =========================================================================

def run_async_communication_tests():
    """
    Run the complete test suite for Com async communication methods.

    Returns:
        unittest.TestResult: Test results with success/failure counts
    """
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestComAsyncCommunication)

    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)

    return result


if __name__ == '__main__':
    print("=" * 80)
    print("COM ASYNC COMMUNICATION METHODS - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print(f"Testing Com class async communication integration")
    print(f"Location: {os.path.abspath(__file__)}")
    print(f"Target: Com.broadcast() and Com.send_to() methods")
    print("=" * 80)

    # Run the tests
    result = run_async_communication_tests()

    # Print summary
    print("\n" + "=" * 80)
    print("TEST EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")

    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.splitlines()[-1] if traceback else 'Unknown failure'}")

    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.splitlines()[-1] if traceback else 'Unknown error'}")

    print("=" * 80)

    # Exit with appropriate code
    exit_code = 0 if result.wasSuccessful() else 1
    sys.exit(exit_code)