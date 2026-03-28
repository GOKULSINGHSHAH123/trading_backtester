from dataclasses import dataclass, field
from typing import Any, Dict
from .enums import IssueLevel

@dataclass
class CheckResult:
    """Immutable validation result."""
    row_idx: int
    check_name: str
    issue_type: str
    issue_level: IssueLevel
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame insertion."""
        return {
            "_row_idx": self.row_idx,
            "CheckName": self.check_name,
            "IssueType": self.issue_type,
            "IssueLevel": self.issue_level.value,
            "Message": self.message,
            **self.metadata
        }
