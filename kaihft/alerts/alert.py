from enum import Enum

class AlertLevel(Enum):
    """ The levels of alert pre-defined for slack message. """
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    def __str__(self) -> str:
        return str(self.value)
    
    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str): 
            return str(self.value).upper() == __o.upper()
        return super().__eq__(__o)