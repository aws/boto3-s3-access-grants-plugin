class IllegalArgumentException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class UnsupportedOperationError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)
