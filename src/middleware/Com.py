from threading import Lock
from Message import Message, BroadcastMessage, DedicatedMessage
from pyeventbus3.pyeventbus3 import PyBus

class Com:
    def __init__(self, process = None):
        self.process = process
        self.clock = 0
        self._clock_mutex = Lock()
        self._mail_box_mutex = Lock()
        self.mail_box = []

    def inc_clock(self):
        with self._clock_mutex:
            self.clock += 1

    def get_clock(self):
        with self._clock_mutex:
            return self.clock

    def _update_clock_on_receive(self, clock):
        with self._clock_mutex:
            self.clock = max(self.clock, clock) + 1

    def _set_clock(self, clock):
        with self._clock_mutex:
            self.clock = clock

    def put_message(self, message : Message):
        if not message.USER_MESSAGE:
            raise TypeError("Only user messages can be put in the mail box")
        with self._mail_box_mutex:
            self.mail_box.append(message)

    def get_message(self):
        with self._mail_box_mutex:
            return self.mail_box.pop(0)

    def get_messages(self):
        with self._mail_box_mutex:
            messages = self.mail_box
            self.mail_box = []
            return messages

    def get_message_from(self, source):
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return None
            self.mail_box.remove(messages[0])
            return messages[0]

    def get_messages_from(self, source):
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return []
            self.mail_box = [message for message in self.mail_box if message.source != source]
            return messages

    def has_messages(self):
        with self._mail_box_mutex:
            return len(self.mail_box) > 0

    def broadcast(self, payload):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = BroadcastMessage.new_broadcast_message(payload, self.get_clock(), self.process.name)
        message.broadcast(PyBus.Instance())

    def send_to(self, payload, dest):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = DedicatedMessage.new_dedicated_message(payload, self.get_clock(), dest, self.process.name)
        message.send(PyBus.Instance())