from typing import Callable, List, Dict
from ..models.check_result import CheckResult
import polars as pl # type: ignore

class CheckRegistry:
    """Central registry for all validation checks."""
    
    def __init__(self):
        self._universal_checks: List[Callable[[pl.DataFrame, Dict], List[CheckResult]]] = []
        self._options_checks: List[Callable[[pl.DataFrame, Dict], List[CheckResult]]] = []
        
    def register_universal(self, check_fn):
        """Register check that runs on all segments."""
        self._universal_checks.append(check_fn)
        return check_fn
    
    def register_options(self, check_fn):
        """Register OPTIONS-specific check."""
        self._options_checks.append(check_fn)
        return check_fn
    
    def get_checks(self, segment: str) -> List[Callable]:
        """Get all applicable checks for a segment."""
        checks = self._universal_checks.copy()
        
        if segment == "OPTIONS":
            checks.extend(self._options_checks)
        
        return checks

# Global registry instance
registry = CheckRegistry()
