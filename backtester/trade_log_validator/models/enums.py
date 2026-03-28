from enum import Enum

class IssueLevel(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"

class ValidationSegment(Enum):
    UNIVERSAL = "UNIVERSAL"
    OPTIONS = "OPTIONS"
