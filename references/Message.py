from abc import ABC
from Token import Token

class Message(ABC):
    def __init__(self, payload, stamp, source=None):
        self.payload = payload
        self.stamp = stamp
        self.source = source

    @classmethod
    def new_message(cls, payload, stamp, source):
        return cls(payload, stamp, source)

    @classmethod
    def from_payload(cls, payload, process):
        return cls(payload, process.clock, process.name)

    @classmethod
    def empty_payload(cls, process):
        return cls.from_payload("", process)

    def get_payload(self):
        return self.payload
    
    def get_stamp(self):
        return self.stamp

class BroadcastMessage(Message):
    @classmethod
    def new_broadcast_message(cls, payload, process):
        return cls.from_payload(payload, process)

    def broadcast(self, pybus, process):
        pybus.post(self)
        process.clock += 1

class DedicatedMessage(Message):
    def __init__(self, payload, process, dest):
        super().__init__(payload, process.clock, process.name)
        self.dest = dest

    @classmethod
    def new_dedicated_message(cls, payload, process, dest):
        return cls(payload, process, dest)

    def send(self, pybus, process):
        pybus.post(self)
        process.clock += 1

    def get_dest(self):
        return self.dest

class TokenMessage(DedicatedMessage):
    def __init__(self, process, dest):
        self.token = Token()
        super().__init__(self.token, process, dest)

    @classmethod
    def new_token_message(cls, process, dest):
        return cls(process, dest)

    def send(self, pybus, process):
        pybus.post(self)

class SyncMessage(Message):
    def __init__(self, msg_type, process):
        super().__init__(msg_type, process.clock, process.myId)
        self.msg_type = msg_type

    @classmethod
    def new_sync_message(cls, msg_type, process):
        return cls(msg_type, process)

    def send(self, pybus, process):
        pybus.post(self)
        process.clock += 1

    def get_type(self):
        return self.msg_type

class RegisterMessage(BroadcastMessage):
    
    @classmethod
    def new_register_message(cls, process):
        return cls(process)