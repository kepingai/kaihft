from enum import Enum

class AlertLevel(Enum):
    """ The levels of alert pre-defined for slack message. """
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    def __str__(self) -> str:
        return str(self.value)