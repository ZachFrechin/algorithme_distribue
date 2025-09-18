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

class RegistrationMessage(UserMessage):
    def __init__(self, stamp, source = None):
        super().__init__("ID_REQUEST", stamp, source)

    @classmethod
    def new_registration_message(cls, stamp, source = None):
        return cls(stamp, source)

class RegisterMessage(UserMessage):
    def __init__(self, process_id, stamp, source = None):
        super().__init__(process_id, stamp, source)
        self.process_id = process_id

    @classmethod
    def new_register_message(cls, process_id, stamp, source = None):
        return cls(process_id, stamp, source)

    def get_process_id(self):
        return self.process_id

class RequestMaxIdMessage(UserMessage):
    """Message pour demander qui a l'ID maximum"""
    def __init__(self, stamp, source = None):
        super().__init__("REQUEST_MAX_ID", stamp, source)

    @classmethod
    def new_request_max_id_message(cls, stamp, source = None):
        return cls(stamp, source)

class ResponseMaxIdMessage(UserMessage):
    """Message pour répondre avec son ID maximum"""
    def __init__(self, current_max_id, stamp, source = None):
        super().__init__(current_max_id, stamp, source)
        self.current_max_id = current_max_id

    @classmethod
    def new_response_max_id_message(cls, current_max_id, stamp, source = None):
        return cls(current_max_id, stamp, source)

    def get_max_id(self):
        return self.current_max_id

