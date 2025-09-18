#!/usr/bin/env python3
"""
Comprehensive Phase 1 Complete Test Suite for Com Middleware Project

Tests the complete integration of:
- Com middleware with thread-safe Lamport clocks and mailbox operations
- Message hierarchy and classification system (User/System messages)
- Thread-safe operations under concurrent access
- PyEventBus3 integration and message routing
- Lamport clock synchronization across distributed processes
- Async communication methods (broadcast, send_to, register)
- Process integration with Com middleware
- Foundation readiness for Phase 2 distributed services

Author: Claude Code
Date: 2025-09-18
"""

import unittest
import threading
import time
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Message import (
    Message, UserMessage, SystemMessage,
    BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage,
    RegistrationMessage, RegisterMessage, RequestMaxIdMessage, ResponseMaxIdMessage
)
from middleware.Com import Com
from Process import Process
from Token import Token


class TestPhase1Complete(unittest.TestCase):
    """Comprehensive Phase 1 integration test suite"""

    def setUp(self):
        """Setup test environment with mocked PyEventBus"""
        # Mock PyEventBus3 to avoid actual event bus registration
        self.mock_pybus_patcher = patch('middleware.Com.PyBus')
        self.mock_pybus_class = self.mock_pybus_patcher.start()
        self.mock_pybus_instance = Mock()
        self.mock_pybus_class.Instance.return_value = self.mock_pybus_instance

        # Mock Process to avoid thread creation
        self.mock_process = Mock()
        self.mock_process.name = "TestProcess"
        self.mock_process.myId = 1

    def tearDown(self):
        """Clean up test environment"""
        self.mock_pybus_patcher.stop()

    def test_complete_com_instantiation_and_integration(self):
        """Test complete Com instantiation with all components"""
        com = Com(self.mock_process)

        # Verify initialization
        self.assertEqual(com.process, self.mock_process)
        self.assertEqual(com.clock, 0)
        self.assertIsInstance(com._clock_mutex, threading.Lock)
        self.assertIsInstance(com._mail_box_mutex, threading.Lock)
        self.assertEqual(com.mail_box, [])

        # Verify PyEventBus registration
        self.mock_pybus_class.Instance.assert_called()
        self.mock_pybus_instance.register.assert_called_once_with(com, com)

    def test_lamport_clock_thread_safety_under_high_concurrency(self):
        """Test Lamport clock operations under high concurrent access"""
        com = Com(self.mock_process)

        # Test concurrent clock increments
        def increment_clock():
            return com.inc_clock()

        def update_clock_on_receive(clock_value):
            return com._update_clock_on_receive(clock_value)

        # Simulate high concurrency with many threads
        num_threads = 50
        results = []

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit mix of increment and update operations
            futures = []

            for i in range(num_threads):
                if i % 2 == 0:
                    futures.append(executor.submit(increment_clock))
                else:
                    futures.append(executor.submit(update_clock_on_receive, i * 10))

            # Collect all results
            for future in as_completed(futures):
                results.append(future.result())

        # Verify final clock value is consistent
        final_clock = com.get_clock()
        self.assertGreater(final_clock, 0)

        # Verify all operations returned valid clock values
        for result in results:
            self.assertIsInstance(result, int)
            self.assertGreater(result, 0)

    def test_complete_message_hierarchy_classification(self):
        """Test complete message hierarchy and classification system"""

        # Test UserMessage classification
        broadcast_msg = BroadcastMessage.new_broadcast_message("test", 10, "P1")
        self.assertTrue(broadcast_msg.USER_MESSAGE)
        self.assertFalse(broadcast_msg.SYSTEM_MESSAGE)

        dedicated_msg = DedicatedMessage.new_dedicated_message("test", 15, "P2", "P1")
        self.assertTrue(dedicated_msg.USER_MESSAGE)
        self.assertFalse(dedicated_msg.SYSTEM_MESSAGE)

        sync_msg = SyncMessage.new_sync_message("READY", 20, "P1")
        self.assertTrue(sync_msg.USER_MESSAGE)
        self.assertFalse(sync_msg.SYSTEM_MESSAGE)

        registration_msg = RegistrationMessage.new_registration_message(25, "P1")
        self.assertTrue(registration_msg.USER_MESSAGE)
        self.assertFalse(registration_msg.SYSTEM_MESSAGE)

        register_msg = RegisterMessage.new_register_message(1, 30, "P1")
        self.assertTrue(register_msg.USER_MESSAGE)
        self.assertFalse(register_msg.SYSTEM_MESSAGE)

        request_max_id_msg = RequestMaxIdMessage.new_request_max_id_message(35, "P1")
        self.assertTrue(request_max_id_msg.USER_MESSAGE)
        self.assertFalse(request_max_id_msg.SYSTEM_MESSAGE)

        response_max_id_msg = ResponseMaxIdMessage.new_response_max_id_message(5, 40, "P1")
        self.assertTrue(response_max_id_msg.USER_MESSAGE)
        self.assertFalse(response_max_id_msg.SYSTEM_MESSAGE)

        # Test SystemMessage classification
        token = Token(1)
        token_msg = TokenMessage.new_token_message(token, 45, "P2", "P1")
        self.assertTrue(token_msg.SYSTEM_MESSAGE)
        self.assertFalse(token_msg.USER_MESSAGE)

    def test_mailbox_operations_comprehensive_thread_safety(self):
        """Test comprehensive mailbox operations under concurrent access"""
        com = Com(self.mock_process)

        # Create test messages
        messages = []
        for i in range(20):
            if i % 4 == 0:
                msg = BroadcastMessage.new_broadcast_message(f"broadcast_{i}", i, f"P{i%3}")
            elif i % 4 == 1:
                msg = DedicatedMessage.new_dedicated_message(f"dedicated_{i}", i, "P0", f"P{i%3}")
            elif i % 4 == 2:
                msg = RegisterMessage.new_register_message(i, i, f"P{i%3}")
            else:
                msg = ResponseMaxIdMessage.new_response_max_id_message(i, i, f"P{i%3}")
            messages.append(msg)

        # Test concurrent put operations
        def put_messages(msg_list):
            for msg in msg_list:
                com.put_message(msg)

        def get_messages_concurrently():
            results = []
            while com.has_messages():
                try:
                    msg = com.get_message()
                    results.append(msg)
                except IndexError:
                    break
            return results

        # Split messages for concurrent insertion
        msg_chunks = [messages[:10], messages[10:]]

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Put messages concurrently
            put_futures = [executor.submit(put_messages, chunk) for chunk in msg_chunks]

            # Wait for all puts to complete
            for future in as_completed(put_futures):
                future.result()

            # Verify all messages were added
            self.assertEqual(len(com.mail_box), 20)

            # Test concurrent get operations
            get_futures = [executor.submit(get_messages_concurrently) for _ in range(2)]

            retrieved_messages = []
            for future in as_completed(get_futures):
                retrieved_messages.extend(future.result())

        # Verify all messages were retrieved exactly once
        self.assertEqual(len(retrieved_messages), 20)
        self.assertEqual(len(com.mail_box), 0)

    def test_message_type_filtering_comprehensive(self):
        """Test comprehensive message type filtering functionality"""
        com = Com(self.mock_process)

        # Add diverse message types
        broadcast_msg1 = BroadcastMessage.new_broadcast_message("broadcast1", 10, "P1")
        broadcast_msg2 = BroadcastMessage.new_broadcast_message("broadcast2", 15, "P2")
        dedicated_msg = DedicatedMessage.new_dedicated_message("dedicated", 20, "P0", "P1")
        register_msg = RegisterMessage.new_register_message(1, 25, "P1")
        response_msg = ResponseMaxIdMessage.new_response_max_id_message(5, 30, "P2")

        com.put_message(broadcast_msg1)
        com.put_message(dedicated_msg)
        com.put_message(broadcast_msg2)
        com.put_message(register_msg)
        com.put_message(response_msg)

        # Test filtering by type
        broadcast_messages = com.get_messages_type("broadcast")
        self.assertEqual(len(broadcast_messages), 2)
        self.assertIn(broadcast_msg1, broadcast_messages)
        self.assertIn(broadcast_msg2, broadcast_messages)

        dedicated_messages = com.get_messages_type("dedicated")
        self.assertEqual(len(dedicated_messages), 1)
        self.assertEqual(dedicated_messages[0], dedicated_msg)

        register_messages = com.get_messages_type("register")
        self.assertEqual(len(register_messages), 1)
        self.assertEqual(register_messages[0], register_msg)

        response_messages = com.get_messages_type("max_id_response")
        self.assertEqual(len(response_messages), 1)
        self.assertEqual(response_messages[0], response_msg)

        # Test invalid type
        invalid_messages = com.get_messages_type("invalid")
        self.assertIsNone(invalid_messages)

        # Test get_message_type for single message retrieval
        single_broadcast = com.get_message_type("broadcast")
        self.assertIsNotNone(single_broadcast)
        self.assertIsInstance(single_broadcast, BroadcastMessage)

    def test_source_based_message_filtering(self):
        """Test source-based message filtering functionality"""
        com = Com(self.mock_process)

        # Add messages from different sources
        msg1_p1 = BroadcastMessage.new_broadcast_message("msg1", 10, "P1")
        msg2_p1 = BroadcastMessage.new_broadcast_message("msg2", 15, "P1")
        msg1_p2 = DedicatedMessage.new_dedicated_message("msg1", 20, "P0", "P2")
        msg2_p2 = RegisterMessage.new_register_message(2, 25, "P2")
        msg1_p3 = ResponseMaxIdMessage.new_response_max_id_message(3, 30, "P3")

        for msg in [msg1_p1, msg2_p1, msg1_p2, msg2_p2, msg1_p3]:
            com.put_message(msg)

        # Test get_message_from
        p1_message = com.get_message_from("P1")
        self.assertIn(p1_message, [msg1_p1, msg2_p1])

        # Test get_messages_from
        p2_messages = com.get_messages_from("P2")
        self.assertEqual(len(p2_messages), 2)
        self.assertIn(msg1_p2, p2_messages)
        self.assertIn(msg2_p2, p2_messages)

        # Verify messages were removed from mailbox
        remaining_messages = com.get_messages()
        self.assertEqual(len(remaining_messages), 2)  # One P1 message + P3 message

        # Test get_message_from with non-existent source
        no_message = com.get_message_from("P99")
        self.assertIsNone(no_message)

        # Test get_messages_from with non-existent source
        no_messages = com.get_messages_from("P99")
        self.assertEqual(no_messages, [])

    def test_async_communication_methods_integration(self):
        """Test async communication methods with PyEventBus integration"""
        com = Com(self.mock_process)

        # Test broadcast
        com.broadcast("test_broadcast")

        # Verify clock was incremented
        self.assertEqual(com.get_clock(), 1)

        # Verify message was posted to PyBus
        self.mock_pybus_instance.post.assert_called()
        posted_message = self.mock_pybus_instance.post.call_args[0][0]
        self.assertIsInstance(posted_message, BroadcastMessage)
        self.assertEqual(posted_message.payload, "test_broadcast")
        self.assertEqual(posted_message.stamp, 1)
        self.assertEqual(posted_message.source, 1)  # myId

        # Reset mock
        self.mock_pybus_instance.reset_mock()

        # Test send_to
        com.send_to("test_dedicated", "P2")

        # Verify clock was incremented again
        self.assertEqual(com.get_clock(), 2)

        # Verify dedicated message was posted
        self.mock_pybus_instance.post.assert_called()
        posted_message = self.mock_pybus_instance.post.call_args[0][0]
        self.assertIsInstance(posted_message, DedicatedMessage)
        self.assertEqual(posted_message.payload, "test_dedicated")
        self.assertEqual(posted_message.dest, "P2")
        self.assertEqual(posted_message.stamp, 2)
        self.assertEqual(posted_message.source, 1)  # myId

        # Reset mock
        self.mock_pybus_instance.reset_mock()

        # Test register
        com.register("test_register")

        # Verify clock was incremented
        self.assertEqual(com.get_clock(), 3)

        # Verify register message was posted
        self.mock_pybus_instance.post.assert_called()
        posted_message = self.mock_pybus_instance.post.call_args[0][0]
        self.assertIsInstance(posted_message, RegisterMessage)
        self.assertEqual(posted_message.payload, "test_register")
        self.assertEqual(posted_message.stamp, 3)
        self.assertEqual(posted_message.source, 1)  # myId

    def test_pyeventbus_message_handler_integration(self):
        """Test PyEventBus message handler integration with source filtering"""
        com = Com(self.mock_process)

        # Test message from same source (should be filtered)
        same_source_msg = BroadcastMessage.new_broadcast_message("self_msg", 10, 1)  # Same myId
        com.on_broadcast(same_source_msg)

        # Verify message was not added to mailbox
        self.assertFalse(com.has_messages())

        # Test message from different source (should be processed)
        different_source_msg = BroadcastMessage.new_broadcast_message("other_msg", 15, 2)  # Different myId
        com.on_broadcast(different_source_msg)

        # Verify message was added to mailbox
        self.assertTrue(com.has_messages())
        retrieved_msg = com.get_message()
        self.assertEqual(retrieved_msg, different_source_msg)

        # Verify clock was updated using Lamport algorithm
        self.assertEqual(com.get_clock(), 16)  # max(0, 15) + 1

    def test_error_handling_and_validation(self):
        """Test comprehensive error handling and validation"""
        com = Com(self.mock_process)

        # Test putting system message in mailbox (should raise TypeError)
        token = Token(1)
        system_msg = TokenMessage.new_token_message(token, 10, "P1", "P0")

        with self.assertRaises(TypeError) as context:
            com.put_message(system_msg)

        self.assertIn("Only user messages can be put in the mail box", str(context.exception))

        # Test communication methods without process (should raise ValueError)
        com_no_process = Com(None)

        with self.assertRaises(ValueError) as context:
            com_no_process.broadcast("test")
        self.assertIn("Process is not set", str(context.exception))

        with self.assertRaises(ValueError) as context:
            com_no_process.send_to("test", "P1")
        self.assertIn("Process is not set", str(context.exception))

        with self.assertRaises(ValueError) as context:
            com_no_process.register("test")
        self.assertIn("Process is not set", str(context.exception))

        # Test getting message from empty mailbox
        with self.assertRaises(IndexError):
            com.get_message()

    def test_lamport_clock_synchronization_protocol(self):
        """Test Lamport clock synchronization across message exchanges"""
        com = Com(self.mock_process)

        # Initial clock
        self.assertEqual(com.get_clock(), 0)

        # Simulate receiving message with higher timestamp
        high_timestamp_msg = BroadcastMessage.new_broadcast_message("high_ts", 50, "P2")
        com.on_broadcast(high_timestamp_msg)

        # Clock should be updated to max(local, received) + 1
        self.assertEqual(com.get_clock(), 51)

        # Simulate receiving message with lower timestamp
        low_timestamp_msg = BroadcastMessage.new_broadcast_message("low_ts", 30, "P3")
        com.on_broadcast(low_timestamp_msg)

        # Clock should be updated to max(51, 30) + 1 = 52
        self.assertEqual(com.get_clock(), 52)

        # Test local event (broadcast)
        com.broadcast("local_event")

        # Clock should increment to 53
        self.assertEqual(com.get_clock(), 53)

    def test_concurrent_lamport_clock_updates(self):
        """Test concurrent Lamport clock updates maintain consistency"""
        com = Com(self.mock_process)

        def simulate_message_reception(timestamp):
            msg = BroadcastMessage.new_broadcast_message(f"msg_{timestamp}", timestamp, f"P{timestamp%3}")
            com.on_broadcast(msg)
            return com.get_clock()

        def simulate_local_events():
            results = []
            for _ in range(10):
                com.inc_clock()
                results.append(com.get_clock())
            return results

        # Simulate concurrent message receptions and local events
        timestamps = [25, 15, 35, 5, 45, 20, 30, 10, 40, 50]

        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit concurrent operations
            futures = []

            # Simulate message receptions
            for ts in timestamps:
                futures.append(executor.submit(simulate_message_reception, ts))

            # Simulate local events
            futures.append(executor.submit(simulate_local_events))

            # Collect results
            results = []
            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)

        # Verify final clock is consistent and monotonic
        final_clock = com.get_clock()
        self.assertGreater(final_clock, max(timestamps))

        # Verify all intermediate results are positive
        for result in results:
            self.assertGreater(result, 0)

    @patch('Process.Process.start')
    def test_process_com_integration_foundation(self, mock_start):
        """Test Process-Com integration provides foundation for Phase 2"""
        # Create process with Com integration
        process = Process("TestProcess", 3)

        # Verify Com was instantiated and integrated
        self.assertIsInstance(process.com, Com)
        self.assertEqual(process.com.process, process)

        # Verify process attributes for distributed algorithms
        self.assertEqual(process.myId, 0)  # First process created
        self.assertEqual(process.npProcess, 3)
        self.assertEqual(process.myProcessName, "TestProcess")
        self.assertIsInstance(process.id_assignment_mutex, threading.Lock)
        self.assertFalse(process.is_max_id)

        # Verify critical section variables are initialized
        self.assertEqual(process.SC, 0)
        self.assertIsNone(process.token)

        # Verify synchronization variables are initialized
        self.assertEqual(process.sync_ready_count, 0)
        self.assertFalse(process.sync_barrier)

        # Test that Com can be used for communication
        # This should not raise errors and clock should advance
        initial_clock = process.com.get_clock()

        # These should work without errors (demonstrating Phase 2 readiness)
        try:
            process.com.broadcast("test_phase2_readiness")
            process.com.send_to("test_dedicated", "P1")
            process.com.register("test_registration")
        except Exception as e:
            self.fail(f"Communication methods failed: {e}")

        # Verify clock advanced with each operation
        final_clock = process.com.get_clock()
        self.assertEqual(final_clock, initial_clock + 3)

    def test_complete_message_lifecycle_integration(self):
        """Test complete message lifecycle from creation to processing"""
        com = Com(self.mock_process)

        # Test complete BroadcastMessage lifecycle
        com.broadcast("lifecycle_test")

        # Verify message was created and sent through PyBus
        self.mock_pybus_instance.post.assert_called()
        sent_message = self.mock_pybus_instance.post.call_args[0][0]

        # Simulate message reception from another process
        received_message = BroadcastMessage.new_broadcast_message(
            "external_message", 25, 2
        )
        com.on_broadcast(received_message)

        # Verify message was processed and stored
        self.assertTrue(com.has_messages())
        retrieved_message = com.get_message()
        self.assertEqual(retrieved_message.payload, "external_message")
        self.assertEqual(retrieved_message.source, 2)

        # Verify Lamport clock was properly updated
        self.assertEqual(com.get_clock(), 26)  # max(1, 25) + 1

    def test_phase1_integration_readiness_for_phase2(self):
        """Test that Phase 1 implementation provides solid foundation for Phase 2"""
        com = Com(self.mock_process)

        # Test 1: Verify all required communication primitives are available
        communication_methods = ['broadcast', 'send_to', 'register']
        for method in communication_methods:
            self.assertTrue(hasattr(com, method), f"Missing communication method: {method}")

        # Test 2: Verify all required message types are properly classified
        message_types = [
            BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage,
            RegistrationMessage, RegisterMessage, RequestMaxIdMessage, ResponseMaxIdMessage
        ]

        for msg_type in message_types:
            # Create instance
            if msg_type == TokenMessage:
                token = Token(1)
                msg = msg_type.new_token_message(token, 10, "P1", "P0")
            elif msg_type == DedicatedMessage:
                msg = msg_type.new_dedicated_message("test", 10, "P1", "P0")
            elif msg_type == SyncMessage:
                msg = msg_type.new_sync_message("READY", 10, "P0")
            elif msg_type == RegistrationMessage:
                msg = msg_type.new_registration_message(10, "P0")
            elif msg_type == RegisterMessage:
                msg = msg_type.new_register_message(1, 10, "P0")
            elif msg_type == RequestMaxIdMessage:
                msg = msg_type.new_request_max_id_message(10, "P0")
            elif msg_type == ResponseMaxIdMessage:
                msg = msg_type.new_response_max_id_message(5, 10, "P0")
            else:
                msg = msg_type.new_broadcast_message("test", 10, "P0")

            # Verify proper classification
            if msg_type == TokenMessage:
                self.assertTrue(msg.SYSTEM_MESSAGE, f"{msg_type} should be system message")
            else:
                self.assertTrue(msg.USER_MESSAGE, f"{msg_type} should be user message")

        # Test 3: Verify thread safety mechanisms are in place
        self.assertIsInstance(com._clock_mutex, threading.Lock)
        self.assertIsInstance(com._mail_box_mutex, threading.Lock)

        # Test 4: Verify Lamport clock is properly implemented
        self.assertEqual(com.get_clock(), 0)
        com.inc_clock()
        self.assertEqual(com.get_clock(), 1)

        # Test 5: Verify mailbox can handle all user message types
        user_messages = [
            BroadcastMessage.new_broadcast_message("test", 10, "P1"),
            DedicatedMessage.new_dedicated_message("test", 15, "P1", "P0"),
            SyncMessage.new_sync_message("READY", 20, "P1"),
            RegistrationMessage.new_registration_message(25, "P1"),
            RegisterMessage.new_register_message(1, 30, "P1"),
            RequestMaxIdMessage.new_request_max_id_message(35, "P1"),
            ResponseMaxIdMessage.new_response_max_id_message(5, 40, "P1")
        ]

        for msg in user_messages:
            com.put_message(msg)

        self.assertEqual(len(com.mail_box), len(user_messages))

        # Test 6: Verify PyEventBus integration is working
        self.mock_pybus_instance.register.assert_called_once()

        # Test 7: Verify message filtering capabilities
        broadcast_msgs = com.get_messages_type("broadcast")
        dedicated_msgs = com.get_messages_type("dedicated")
        register_msgs = com.get_messages_type("register")
        max_id_response_msgs = com.get_messages_type("max_id_response")

        self.assertEqual(len(broadcast_msgs), 1)
        self.assertEqual(len(dedicated_msgs), 1)
        self.assertEqual(len(register_msgs), 1)
        self.assertEqual(len(max_id_response_msgs), 1)

    def test_distributed_system_simulation_readiness(self):
        """Test readiness for distributed system simulation"""
        # Create multiple Com instances simulating distributed processes
        processes = []
        coms = []

        for i in range(3):
            mock_proc = Mock()
            mock_proc.name = f"P{i}"
            mock_proc.myId = i
            processes.append(mock_proc)
            coms.append(Com(mock_proc))

        # Simulate distributed message exchange
        # P0 broadcasts to all
        coms[0].broadcast("distributed_test")

        # Simulate P1 and P2 receiving the broadcast
        broadcast_msg = BroadcastMessage.new_broadcast_message("distributed_test", 1, 0)

        coms[1].on_broadcast(broadcast_msg)
        coms[2].on_broadcast(broadcast_msg)

        # Verify message was received by P1 and P2
        self.assertTrue(coms[1].has_messages())
        self.assertTrue(coms[2].has_messages())

        # Verify clocks are synchronized
        self.assertEqual(coms[1].get_clock(), 2)  # max(0, 1) + 1
        self.assertEqual(coms[2].get_clock(), 2)  # max(0, 1) + 1

        # Simulate P1 sending dedicated message to P2
        coms[1].send_to("p1_to_p2", "P2")

        # Verify P1's clock advanced
        self.assertEqual(coms[1].get_clock(), 3)

        # This demonstrates that the Phase 1 implementation provides
        # the necessary foundation for distributed algorithms


class TestPhase1ErrorScenarios(unittest.TestCase):
    """Test error scenarios and edge cases for Phase 1"""

    def setUp(self):
        """Setup test environment"""
        self.mock_pybus_patcher = patch('middleware.Com.PyBus')
        self.mock_pybus_class = self.mock_pybus_patcher.start()
        self.mock_pybus_instance = Mock()
        self.mock_pybus_class.Instance.return_value = self.mock_pybus_instance

        self.mock_process = Mock()
        self.mock_process.name = "TestProcess"
        self.mock_process.myId = 1

    def tearDown(self):
        """Clean up test environment"""
        self.mock_pybus_patcher.stop()

    def test_mailbox_edge_cases(self):
        """Test mailbox edge cases and error conditions"""
        com = Com(self.mock_process)

        # Test empty mailbox operations
        self.assertFalse(com.has_messages())

        with self.assertRaises(IndexError):
            com.get_message()

        empty_messages = com.get_messages()
        self.assertEqual(empty_messages, [])

        # Test source filtering on empty mailbox
        no_message = com.get_message_from("P1")
        self.assertIsNone(no_message)

        no_messages = com.get_messages_from("P1")
        self.assertEqual(no_messages, [])

        # Test type filtering on empty mailbox
        no_broadcast = com.get_message_type("broadcast")
        self.assertIsNone(no_broadcast)

        no_broadcasts = com.get_messages_type("broadcast")
        self.assertEqual(no_broadcasts, [])

    def test_invalid_message_operations(self):
        """Test invalid message operations"""
        com = Com(self.mock_process)

        # Test invalid type filtering
        invalid_result = com.get_messages_type("invalid_type")
        self.assertIsNone(invalid_result)

        invalid_single = com.get_message_type("invalid_type")
        self.assertIsNone(invalid_single)

        # Test None type filtering
        none_result = com.get_messages_type(None)
        self.assertIsNone(none_result)

        none_single = com.get_message_type(None)
        self.assertIsNone(none_single)


def run_comprehensive_phase1_tests():
    """Run all comprehensive Phase 1 tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test cases
    suite.addTest(loader.loadTestsFromTestCase(TestPhase1Complete))
    suite.addTest(loader.loadTestsFromTestCase(TestPhase1ErrorScenarios))

    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == '__main__':
    print("=" * 80)
    print("PHASE 1 COMPLETE - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print("Testing Com middleware project Phase 1 completion:")
    print("- Complete Com middleware functionality")
    print("- Message hierarchy and classification system")
    print("- Thread-safe operations under concurrent access")
    print("- PyEventBus3 integration and message routing")
    print("- Lamport clock synchronization")
    print("- Mailbox operations and message filtering")
    print("- Async communication methods")
    print("- Integration readiness for Phase 2")
    print("=" * 80)

    success = run_comprehensive_phase1_tests()

    if success:
        print("\n" + "=" * 80)
        print("✓ ALL PHASE 1 TESTS PASSED")
        print("✓ Com middleware Phase 1 implementation is COMPLETE")
        print("✓ System ready for Phase 2 distributed services implementation")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("✗ SOME PHASE 1 TESTS FAILED")
        print("✗ Please review and fix failing components before proceeding to Phase 2")
        print("=" * 80)
        sys.exit(1)