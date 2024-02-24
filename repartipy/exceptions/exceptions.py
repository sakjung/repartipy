from typing import Any


class RepartipyException(BaseException):
    """RepartiPy Base Exception."""


class NotFullyInitializedException(RepartipyException):
    """Raise this when SizeEstimator has not been initialized with context manager."""

    def __init__(self, this: Any) -> None:
        super().__init__(f"Given object {this} has not been fully initialized.")
