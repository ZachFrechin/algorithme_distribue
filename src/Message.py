from abc import ABC
from Token import Token

class Message(ABC):
    def __init__(self, payload, stamp, source = None):
        self.payload = payload
        self.stamp = stamp
        self.source = source

    @classmethod
    def new_message(cls, payload, stamp, source = None):
        return cls(payload, stamp, source)

    @classmethod
    def from_payload(cls, payload, stamp, source = None):
        return cls(payload, stamp, source)

    @classmethod
    def empty_payload(cls, stamp, source):
        return cls.from_payload(None, stamp, source)

    def send(self, pybus):
        pybus.post(self)

    def broadcast(self, pybus):
        pybus.post(self)

    def get_payload(self):
        return self.payload
    
    def get_stamp(self):
        return self.stamp

class UserMessage(Message):
    def __init__(self, payload, stamp, source = None):
        super().__init__(payload, stamp, source)
        self.USER_MESSAGE = True
        self.SYSTEM_MESSAGE = False

class SystemMessage(Message):
    def __init__(self, payload, stamp, source = None):
        super().__init__(payload, stamp, source)
        self.SYSTEM_MESSAGE = True
        self.USER_MESSAGE = False

class BroadcastMessage(UserMessage):
    @classmethod
    def new_broadcast_message(cls, payload, stamp, source = None):
        return cls.from_payload(payload, stamp, source)

class DedicatedMessage(UserMessage):
    def __init__(self, payload, stamp, dest, source = None):
        super().__init__(payload, stamp, source)
        self.dest = dest    

    @classmethod
    def new_dedicated_message(cls, payload, stamp, dest, source = None):
        return cls(payload, stamp, dest, source)  

    def get_dest(self):
        return self.dest

class TokenMessage(SystemMessage):
    def __init__(self, token, stamp, dest, source = None):
        self.token = token
        super().__init__(self.token, stamp, source)
        self.dest = dest

    @classmethod
    def new_token_message(cls, token, stamp, dest, source = None):
        return cls(token, stamp, dest, source)

    def get_dest(self):
        return self.dest

    def get_token(self):
        return self.token

class SyncMessage(UserMessage):
    def __init__(self, msg_type, stamp, source = None):
        super().__init__(msg_type, stamp, source)
        self.msg_type = msg_type

    @classmethod
    def new_sync_message(cls, msg_type, stamp, source = None):
        return cls(msg_type, stamp, source)

    def get_type(self):
        return self.msg_type