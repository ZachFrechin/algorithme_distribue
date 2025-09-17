#!/usr/bin/env python3
"""
Comprehensive test suite for Message.py hierarchy validation

Tests the message hierarchy for distributed systems communication:
- Message classification (USER_MESSAGE/SYSTEM_MESSAGE attributes)
- Inheritance hierarchy correctness
- Message creation and factory methods
- Payload and timestamp handling
- Specific message types (Broadcast, Dedicated, Token, Sync)
- PyEventBus3 integration compatibility
- Distributed systems semantics (causal ordering classification)

Created: 2025-09-17
"""

import unittest
import sys
import os
from unittest.mock import Mock, MagicMock, patch

# Add src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Message import (
    Message, UserMessage, SystemMessage,
    BroadcastMessage, DedicatedMessage,
    TokenMessage, SyncMessage
)
from Token import Token


class TestMessageHierarchy(unittest.TestCase):
    """Test suite for Message hierarchy structure and inheritance"""

    def setUp(self):
        """Set up common test data"""
        self.test_payload = {"data": "test_message", "value": 42}
        self.test_stamp = 12345
        self.test_source = "process_1"
        self.test_dest = "process_2"
        self.test_token = Token("TEST_TOKEN")

    def test_message_is_abstract_base_class(self):
        """Test that Message is properly defined as abstract base class"""
        from abc import ABC
        self.assertTrue(issubclass(Message, ABC))

        # Should not be able to instantiate Message directly if it's truly abstract
        # But since it's not marked with @abstractmethod, it can be instantiated
        # This tests the current implementation
        message = Message(self.test_payload, self.test_stamp, self.test_source)
        self.assertIsInstance(message, Message)


class TestMessageClassification(unittest.TestCase):
    """Test USER_MESSAGE and SYSTEM_MESSAGE classification attributes"""

    def setUp(self):
        self.test_payload = "test"
        self.test_stamp = 100
        self.test_source = "proc_0"

    def test_user_message_classification(self):
        """Test UserMessage has correct classification attributes"""
        user_msg = UserMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertTrue(user_msg.USER_MESSAGE)
        self.assertFalse(user_msg.SYSTEM_MESSAGE)
        self.assertTrue(hasattr(user_msg, 'USER_MESSAGE'))
        self.assertTrue(hasattr(user_msg, 'SYSTEM_MESSAGE'))

    def test_system_message_classification(self):
        """Test SystemMessage has correct classification attributes"""
        system_msg = SystemMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertTrue(system_msg.SYSTEM_MESSAGE)
        self.assertFalse(system_msg.USER_MESSAGE)
        self.assertTrue(hasattr(system_msg, 'USER_MESSAGE'))
        self.assertTrue(hasattr(system_msg, 'SYSTEM_MESSAGE'))

    def test_broadcast_message_classification(self):
        """Test BroadcastMessage inherits USER_MESSAGE classification"""
        broadcast_msg = BroadcastMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertTrue(broadcast_msg.USER_MESSAGE)
        self.assertFalse(broadcast_msg.SYSTEM_MESSAGE)

    def test_dedicated_message_classification(self):
        """Test DedicatedMessage inherits USER_MESSAGE classification"""
        dedicated_msg = DedicatedMessage(self.test_payload, self.test_stamp, "dest", self.test_source)

        self.assertTrue(dedicated_msg.USER_MESSAGE)
        self.assertFalse(dedicated_msg.SYSTEM_MESSAGE)

    def test_token_message_classification(self):
        """Test TokenMessage inherits SYSTEM_MESSAGE classification"""
        token = Token("TEST")
        # Note: TokenMessage constructor is broken due to dest parameter issue
        # This test documents the current broken behavior
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(token, self.test_stamp, "dest", self.test_source)

    def test_sync_message_classification(self):
        """Test SyncMessage inherits USER_MESSAGE classification"""
        sync_msg = SyncMessage("READY", self.test_stamp, self.test_source)

        self.assertTrue(sync_msg.USER_MESSAGE)
        self.assertFalse(sync_msg.SYSTEM_MESSAGE)


class TestInheritanceHierarchy(unittest.TestCase):
    """Test inheritance hierarchy correctness"""

    def setUp(self):
        self.test_payload = "test"
        self.test_stamp = 200
        self.test_source = "proc_1"

    def test_user_message_inheritance(self):
        """Test UserMessage properly inherits from Message"""
        user_msg = UserMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertIsInstance(user_msg, Message)
        self.assertIsInstance(user_msg, UserMessage)
        self.assertTrue(issubclass(UserMessage, Message))

    def test_system_message_inheritance(self):
        """Test SystemMessage properly inherits from Message"""
        system_msg = SystemMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertIsInstance(system_msg, Message)
        self.assertIsInstance(system_msg, SystemMessage)
        self.assertTrue(issubclass(SystemMessage, Message))

    def test_broadcast_message_inheritance(self):
        """Test BroadcastMessage inheritance chain: Message -> UserMessage -> BroadcastMessage"""
        broadcast_msg = BroadcastMessage(self.test_payload, self.test_stamp, self.test_source)

        self.assertIsInstance(broadcast_msg, Message)
        self.assertIsInstance(broadcast_msg, UserMessage)
        self.assertIsInstance(broadcast_msg, BroadcastMessage)
        self.assertTrue(issubclass(BroadcastMessage, UserMessage))
        self.assertTrue(issubclass(BroadcastMessage, Message))

    def test_dedicated_message_inheritance(self):
        """Test DedicatedMessage inheritance chain: Message -> UserMessage -> DedicatedMessage"""
        dedicated_msg = DedicatedMessage(self.test_payload, self.test_stamp, "dest", self.test_source)

        self.assertIsInstance(dedicated_msg, Message)
        self.assertIsInstance(dedicated_msg, UserMessage)
        self.assertIsInstance(dedicated_msg, DedicatedMessage)
        self.assertTrue(issubclass(DedicatedMessage, UserMessage))
        self.assertTrue(issubclass(DedicatedMessage, Message))

    def test_token_message_inheritance(self):
        """Test TokenMessage inheritance chain: Message -> SystemMessage -> TokenMessage"""
        # Test class hierarchy without instantiating (due to constructor bug)
        self.assertTrue(issubclass(TokenMessage, SystemMessage))
        self.assertTrue(issubclass(TokenMessage, Message))

        # Document the constructor issue
        token = Token("TEST")
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(token, self.test_stamp, "dest", self.test_source)

    def test_sync_message_inheritance(self):
        """Test SyncMessage inheritance chain: Message -> UserMessage -> SyncMessage"""
        sync_msg = SyncMessage("READY", self.test_stamp, self.test_source)

        self.assertIsInstance(sync_msg, Message)
        self.assertIsInstance(sync_msg, UserMessage)
        self.assertIsInstance(sync_msg, SyncMessage)
        self.assertTrue(issubclass(SyncMessage, UserMessage))
        self.assertTrue(issubclass(SyncMessage, Message))


class TestMessageCreationAndFactoryMethods(unittest.TestCase):
    """Test message creation and factory methods"""

    def setUp(self):
        self.test_payload = {"key": "value"}
        self.test_stamp = 300
        self.test_source = "proc_2"
        self.test_dest = "proc_3"

    def test_message_constructor(self):
        """Test basic Message constructor"""
        msg = Message(self.test_payload, self.test_stamp, self.test_source)

        self.assertEqual(msg.payload, self.test_payload)
        self.assertEqual(msg.stamp, self.test_stamp)
        self.assertEqual(msg.source, self.test_source)

    def test_message_new_message_factory(self):
        """Test Message.new_message class method"""
        msg = Message.new_message(self.test_payload, self.test_stamp, self.test_source)

        self.assertIsInstance(msg, Message)
        self.assertEqual(msg.payload, self.test_payload)
        self.assertEqual(msg.stamp, self.test_stamp)
        self.assertEqual(msg.source, self.test_source)

    def test_message_from_payload_factory(self):
        """Test Message.from_payload class method"""
        msg = Message.from_payload(self.test_payload, self.test_stamp, self.test_source)

        self.assertIsInstance(msg, Message)
        self.assertEqual(msg.payload, self.test_payload)
        self.assertEqual(msg.stamp, self.test_stamp)
        self.assertEqual(msg.source, self.test_source)

    def test_message_empty_payload_factory(self):
        """Test Message.empty_payload class method"""
        msg = Message.empty_payload(self.test_stamp, self.test_source)

        self.assertIsInstance(msg, Message)
        self.assertIsNone(msg.payload)
        self.assertEqual(msg.stamp, self.test_stamp)
        self.assertEqual(msg.source, self.test_source)

    def test_broadcast_message_factory(self):
        """Test BroadcastMessage.new_broadcast_message factory method"""
        broadcast_msg = BroadcastMessage.new_broadcast_message(
            self.test_payload, self.test_stamp, self.test_source
        )

        self.assertIsInstance(broadcast_msg, BroadcastMessage)
        self.assertEqual(broadcast_msg.payload, self.test_payload)
        self.assertEqual(broadcast_msg.stamp, self.test_stamp)
        self.assertEqual(broadcast_msg.source, self.test_source)

    def test_dedicated_message_factory(self):
        """Test DedicatedMessage.new_dedicated_message factory method"""
        dedicated_msg = DedicatedMessage.new_dedicated_message(
            self.test_payload, self.test_stamp, self.test_dest, self.test_source
        )

        self.assertIsInstance(dedicated_msg, DedicatedMessage)
        self.assertEqual(dedicated_msg.payload, self.test_payload)
        self.assertEqual(dedicated_msg.stamp, self.test_stamp)
        self.assertEqual(dedicated_msg.dest, self.test_dest)
        self.assertEqual(dedicated_msg.source, self.test_source)

    def test_token_message_factory(self):
        """Test TokenMessage.new_token_message factory method"""
        token = Token("TEST_TOKEN")
        # Note: TokenMessage has implementation bug - dest is passed to SystemMessage constructor
        # which doesn't accept it. This test documents the current broken behavior.
        with self.assertRaises(TypeError):
            token_msg = TokenMessage.new_token_message(
                token, self.test_stamp, self.test_dest, self.test_source
            )

    def test_sync_message_factory(self):
        """Test SyncMessage.new_sync_message factory method"""
        msg_type = "READY"
        sync_msg = SyncMessage.new_sync_message(msg_type, self.test_stamp, self.test_source)

        self.assertIsInstance(sync_msg, SyncMessage)
        self.assertEqual(sync_msg.msg_type, msg_type)
        self.assertEqual(sync_msg.payload, msg_type)  # payload should be msg_type
        self.assertEqual(sync_msg.stamp, self.test_stamp)
        self.assertEqual(sync_msg.source, self.test_source)


class TestPayloadAndTimestampHandling(unittest.TestCase):
    """Test payload and timestamp handling across message types"""

    def test_message_getters(self):
        """Test basic message getter methods"""
        payload = {"data": "test"}
        stamp = 400
        source = "proc_0"

        msg = Message(payload, stamp, source)

        self.assertEqual(msg.get_payload(), payload)
        self.assertEqual(msg.get_stamp(), stamp)

    def test_dedicated_message_destination_handling(self):
        """Test DedicatedMessage destination handling"""
        dest = "target_process"
        dedicated_msg = DedicatedMessage("payload", 500, dest, "source")

        self.assertEqual(dedicated_msg.get_dest(), dest)
        self.assertEqual(dedicated_msg.dest, dest)

    def test_token_message_token_handling(self):
        """Test TokenMessage token and destination handling"""
        token = Token("MUTEX_TOKEN")
        dest = "next_process"
        # Note: TokenMessage constructor is broken due to implementation bug
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(token, 600, dest, "source")

    def test_sync_message_type_handling(self):
        """Test SyncMessage type handling"""
        msg_type = "SYNC_RELEASE"
        sync_msg = SyncMessage(msg_type, 700, "source")

        self.assertEqual(sync_msg.get_type(), msg_type)
        self.assertEqual(sync_msg.msg_type, msg_type)
        self.assertEqual(sync_msg.payload, msg_type)  # payload should equal msg_type

    def test_timestamp_consistency(self):
        """Test timestamp consistency across message types"""
        stamp = 12345678

        user_msg = UserMessage("payload", stamp, "source")
        system_msg = SystemMessage("payload", stamp, "source")
        broadcast_msg = BroadcastMessage("payload", stamp, "source")
        dedicated_msg = DedicatedMessage("payload", stamp, "dest", "source")
        # Skip TokenMessage due to constructor bug
        sync_msg = SyncMessage("READY", stamp, "source")

        messages = [user_msg, system_msg, broadcast_msg, dedicated_msg, sync_msg]

        for msg in messages:
            self.assertEqual(msg.get_stamp(), stamp)
            self.assertEqual(msg.stamp, stamp)


class TestSpecificMessageTypes(unittest.TestCase):
    """Test specific message type behaviors and semantics"""

    def test_broadcast_message_semantics(self):
        """Test BroadcastMessage for application-level broadcasts"""
        payload = {"announcement": "system update", "version": "1.2.3"}
        broadcast_msg = BroadcastMessage(payload, 800, "coordinator")

        # Should be a user message for application-level communication
        self.assertTrue(broadcast_msg.USER_MESSAGE)
        self.assertFalse(broadcast_msg.SYSTEM_MESSAGE)

        # Should preserve payload structure
        self.assertEqual(broadcast_msg.payload, payload)

    def test_dedicated_message_point_to_point(self):
        """Test DedicatedMessage for point-to-point communication"""
        payload = {"task": "compute_result", "data": [1, 2, 3, 4, 5]}
        dest = "worker_process_2"
        dedicated_msg = DedicatedMessage(payload, 900, dest, "master")

        # Should be a user message for application communication
        self.assertTrue(dedicated_msg.USER_MESSAGE)
        self.assertFalse(dedicated_msg.SYSTEM_MESSAGE)

        # Should have destination information
        self.assertEqual(dedicated_msg.get_dest(), dest)
        self.assertEqual(dedicated_msg.dest, dest)

    def test_token_message_mutual_exclusion(self):
        """Test TokenMessage for mutual exclusion protocol"""
        token = Token("CRITICAL_SECTION_TOKEN")
        next_process = "process_1"

        # Note: TokenMessage constructor is broken, document the intended behavior
        # and the actual broken state
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(token, 1000, next_process, "process_0")

        # Test class hierarchy is correct even if constructor is broken
        self.assertTrue(issubclass(TokenMessage, SystemMessage))
        self.assertTrue(issubclass(TokenMessage, Message))

    def test_sync_message_barrier_coordination(self):
        """Test SyncMessage for synchronization barriers"""
        # Test READY message
        ready_msg = SyncMessage("READY", 1100, "worker_1")
        self.assertEqual(ready_msg.get_type(), "READY")
        self.assertTrue(ready_msg.USER_MESSAGE)

        # Test SYNC_RELEASE message
        release_msg = SyncMessage("SYNC_RELEASE", 1200, "coordinator")
        self.assertEqual(release_msg.get_type(), "SYNC_RELEASE")
        self.assertTrue(release_msg.USER_MESSAGE)

        # Both should be user messages as they're application-level coordination
        self.assertFalse(ready_msg.SYSTEM_MESSAGE)
        self.assertFalse(release_msg.SYSTEM_MESSAGE)


class TestPyEventBus3Integration(unittest.TestCase):
    """Test PyEventBus3 integration compatibility"""

    def setUp(self):
        self.mock_pybus = Mock()

    def test_message_send_method(self):
        """Test Message.send() method for PyEventBus3 integration"""
        msg = Message("payload", 1300, "source")
        msg.send(self.mock_pybus)

        # Should call pybus.post() with the message
        self.mock_pybus.post.assert_called_once_with(msg)

    def test_message_broadcast_method(self):
        """Test Message.broadcast() method for PyEventBus3 integration"""
        msg = Message("payload", 1400, "source")
        msg.broadcast(self.mock_pybus)

        # Should call pybus.post() with the message
        self.mock_pybus.post.assert_called_once_with(msg)

    def test_all_message_types_can_be_posted(self):
        """Test that all message types can be posted to PyEventBus3"""
        messages = [
            UserMessage("payload", 1500, "source"),
            SystemMessage("payload", 1501, "source"),
            BroadcastMessage("payload", 1502, "source"),
            DedicatedMessage("payload", 1503, "dest", "source"),
            # Skip TokenMessage due to constructor bug
            SyncMessage("READY", 1505, "source")
        ]

        for msg in messages:
            mock_bus = Mock()
            msg.send(mock_bus)
            mock_bus.post.assert_called_once_with(msg)

            mock_bus.reset_mock()
            msg.broadcast(mock_bus)
            mock_bus.post.assert_called_once_with(msg)


class TestDistributedSystemsSemantics(unittest.TestCase):
    """Test distributed systems semantics and causal ordering classification"""

    def test_causal_ordering_by_message_type(self):
        """Test causal ordering implications by message classification"""

        # System messages (like tokens) typically don't participate in causal ordering
        # for application events - they're infrastructure
        # Note: TokenMessage constructor is broken, but we can test the class hierarchy
        self.assertTrue(issubclass(TokenMessage, SystemMessage))

        # User messages participate in application-level causal ordering
        user_messages = [
            BroadcastMessage("app_event", 1601, "source"),
            DedicatedMessage("app_request", 1602, "dest", "source"),
            SyncMessage("READY", 1603, "source")
        ]

        for msg in user_messages:
            self.assertTrue(msg.USER_MESSAGE)
            self.assertFalse(msg.SYSTEM_MESSAGE)

    def test_lamport_clock_compatibility(self):
        """Test compatibility with Lamport logical clock management"""
        # All messages should carry timestamp information for logical clocks

        messages = [
            UserMessage("event", 1001, "proc_0"),
            SystemMessage("system_event", 1002, "proc_1"),
            BroadcastMessage("broadcast_event", 1003, "proc_2"),
            DedicatedMessage("direct_event", 1004, "proc_3", "proc_2"),
            # Skip TokenMessage due to constructor bug
            SyncMessage("READY", 1006, "proc_3")
        ]

        for msg in messages:
            # All messages should have timestamp accessible via get_stamp()
            self.assertTrue(hasattr(msg, 'stamp'))
            self.assertTrue(hasattr(msg, 'get_stamp'))
            self.assertIsInstance(msg.get_stamp(), int)

            # All messages should have source information for clock updates
            self.assertTrue(hasattr(msg, 'source'))

        # Test that TokenMessage class has the required methods (even if constructor is broken)
        # Note: stamp is an instance attribute, not a class attribute
        self.assertTrue(hasattr(TokenMessage, 'get_stamp'))
        # Verify the method exists by checking it's callable
        self.assertTrue(callable(getattr(TokenMessage, 'get_stamp')))

    def test_message_routing_information(self):
        """Test message routing information for distributed communication"""

        # Point-to-point messages should have destination
        dedicated_msg = DedicatedMessage("data", 1700, "target", "sender")

        self.assertTrue(hasattr(dedicated_msg, 'dest'))
        self.assertTrue(hasattr(dedicated_msg, 'get_dest'))

        # TokenMessage should have get_dest method (even if constructor is broken)
        self.assertTrue(hasattr(TokenMessage, 'get_dest'))

        # Broadcast messages don't need explicit destination
        broadcast_msg = BroadcastMessage("announcement", 1702, "broadcaster")
        self.assertFalse(hasattr(broadcast_msg, 'dest'))

        # Sync messages may or may not have destination (coordination pattern dependent)
        sync_msg = SyncMessage("READY", 1703, "worker")
        self.assertFalse(hasattr(sync_msg, 'dest'))

    def test_token_ring_semantics(self):
        """Test TokenMessage semantics for token ring mutual exclusion"""
        token = Token("MUTEX")
        current_proc = "process_0"
        next_proc = "process_1"

        # Note: TokenMessage constructor is broken, document intended semantics
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(token, 1800, next_proc, current_proc)

        # Verify class hierarchy is correct for intended usage
        self.assertTrue(issubclass(TokenMessage, SystemMessage))
        self.assertFalse(issubclass(TokenMessage, UserMessage))

    def test_synchronization_barrier_semantics(self):
        """Test SyncMessage semantics for distributed synchronization barriers"""

        # Worker sending READY signal
        ready_msg = SyncMessage("READY", 1900, "worker_2")
        self.assertEqual(ready_msg.get_type(), "READY")

        # Coordinator sending SYNC_RELEASE
        release_msg = SyncMessage("SYNC_RELEASE", 1901, "coordinator")
        self.assertEqual(release_msg.get_type(), "SYNC_RELEASE")

        # Both should be user messages (application-level coordination)
        self.assertTrue(ready_msg.USER_MESSAGE)
        self.assertTrue(release_msg.USER_MESSAGE)

        # Message type should be accessible and match payload
        self.assertEqual(ready_msg.msg_type, ready_msg.payload)
        self.assertEqual(release_msg.msg_type, release_msg.payload)


class TestImplementationIssues(unittest.TestCase):
    """Test suite documenting implementation issues in the Message hierarchy"""

    def test_token_message_constructor_bug(self):
        """Document TokenMessage constructor bug and expected behavior"""
        token = Token("TEST_TOKEN")

        # Current implementation is broken - dest parameter passed to SystemMessage
        # which only accepts (payload, stamp, source)
        with self.assertRaises(TypeError) as context:
            TokenMessage(token, 1000, "next_process", "current_process")

        # The error should indicate wrong number of arguments
        self.assertIn("takes from 3 to 4 positional arguments but 5 were given", str(context.exception))

        # Document expected behavior: TokenMessage should handle dest separately
        # Expected interface: TokenMessage(token, stamp, dest, source)
        # Should set self.dest = dest and call super().__init__(token, stamp, source)

    def test_token_message_should_work_like_this(self):
        """Document how TokenMessage should work when fixed"""
        # This is how the TokenMessage SHOULD work once the bug is fixed:
        # 1. Accept (token, stamp, dest, source) parameters
        # 2. Store dest as self.dest (separate from parent constructor)
        # 3. Call SystemMessage.__init__(token, stamp, source)
        # 4. Provide get_dest() and get_token() methods

        # For now, we can only test the class hierarchy
        self.assertTrue(issubclass(TokenMessage, SystemMessage))
        self.assertTrue(hasattr(TokenMessage, 'get_dest'))
        self.assertTrue(hasattr(TokenMessage, 'get_token'))
        self.assertTrue(hasattr(TokenMessage, 'new_token_message'))


class TestEdgeCasesAndErrorHandling(unittest.TestCase):
    """Test edge cases and error handling scenarios"""

    def test_none_payload_handling(self):
        """Test handling of None payloads"""
        msg = Message(None, 2000, "source")
        self.assertIsNone(msg.get_payload())

        empty_msg = Message.empty_payload(2001, "source")
        self.assertIsNone(empty_msg.get_payload())

    def test_none_source_handling(self):
        """Test handling of None source"""
        msg = Message("payload", 2002)  # source defaults to None
        self.assertIsNone(msg.source)

        msg_explicit = Message("payload", 2003, None)
        self.assertIsNone(msg_explicit.source)

    def test_token_message_with_none_token(self):
        """Test TokenMessage behavior with None token"""
        # This might be an edge case in error scenarios
        # But constructor is broken, so document this
        with self.assertRaises(TypeError):
            token_msg = TokenMessage(None, 2004, "dest", "source")

    def test_sync_message_empty_type(self):
        """Test SyncMessage with empty or None message type"""
        sync_msg_empty = SyncMessage("", 2005, "source")
        self.assertEqual(sync_msg_empty.get_type(), "")

        sync_msg_none = SyncMessage(None, 2006, "source")
        self.assertIsNone(sync_msg_none.get_type())


if __name__ == '__main__':
    # Configure test runner
    unittest.main(verbosity=2, buffer=True)