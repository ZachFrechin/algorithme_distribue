class MiddlewareException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class NotUserMessageException(MiddlewareException):
    def __init__(self, id, clock):
        message = f"Process {id} with clock {clock} tried to put a non-user message in the mail box"
        super().__init__(message)

class ProcessNotSetException(MiddlewareException):
    def __init__(self, message):
        super().__init__(message)

class ProcessAlreadySetException(MiddlewareException):
    def __init__(self, message):
        super().__init__(message)