from threading import Lock
from Message import Message, BroadcastMessage, DedicatedMessage, RegisterMessage, RegistrationMessage, RequestMaxIdMessage, ResponseMaxIdMessage
from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode


class Com:
    def __init__(self, process = None):
        self.process = process
        self.clock = 0
        self._clock_mutex = Lock()
        self._mail_box_mutex = Lock()
        self.mail_box = []

        PyBus.Instance().register(self, self)

    def inc_clock(self):
        with self._clock_mutex:
            self.clock += 1
            return self.clock

    def get_clock(self):
        with self._clock_mutex:
            return self.clock

    def _update_clock_on_receive(self, clock):
        with self._clock_mutex:
            self.clock = max(self.clock, clock) + 1
            return self.clock

    def _set_clock(self, clock):
        with self._clock_mutex:
            self.clock = clock

    def put_message(self, message : Message):
        if not message.USER_MESSAGE:
            raise TypeError("Only user messages can be put in the mail box")
        with self._mail_box_mutex:
            self.mail_box.append(message)
            self._update_clock_on_receive(message.stamp)

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

    def get_messages_type(self, type = None):
        if type is None:
            return None

        with self._mail_box_mutex:
            match type:
                case "broadcast" :
                    messages = [message for message in self.mail_box if isinstance(message, BroadcastMessage)]
                case "register" :
                    messages = [message for message in self.mail_box if isinstance(message, RegisterMessage)]
                case "dedicated" :
                    messages = [message for message in self.mail_box if isinstance(message, DedicatedMessage)]
                case "max_id_response" :
                    messages = [message for message in self.mail_box if isinstance(message, ResponseMaxIdMessage)]
                case _:
                    return None
            return messages

    def get_message_type(self, type = None):
        if type is None:
            return None
        messages = self.get_messages_type(type)
        if messages is None or len(messages) == 0:
            return None
        return messages[0]


    def broadcast(self, payload):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = BroadcastMessage.new_broadcast_message(payload, self.get_clock(), self.process.myId)
        message.broadcast(PyBus.Instance())

    def send_to(self, payload, dest):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = DedicatedMessage.new_dedicated_message(payload, self.get_clock(), dest, self.process.myId)
        message.send(PyBus.Instance())

    def register(self, payload):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = RegisterMessage.new_register_message(payload, self.get_clock(), self.process.myId)
        message.broadcast(PyBus.Instance())

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastMessage)
    def on_broadcast(self, event):
        if event.source == self.process.myId:
            return

        self.put_message(event)
