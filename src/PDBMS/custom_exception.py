from typing import TypeVar

T = TypeVar('T')


class EmptyNameError(Exception):
    def __init__(self, obj: T):
        self.object = obj

    def __str__(self):
        return f"The name of the object '{type(self.object)}' is an empty string"
