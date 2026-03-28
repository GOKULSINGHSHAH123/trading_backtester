from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class LTPRequest(BaseModel):
    symbol: str
    timestamp: datetime
    
    # Allow using this object in sets/dicts
    class Config:
        frozen = True

class LTPResponse(BaseModel):
    symbol: str
    timestamp: datetime
    ltp: float
    status: str  # "SUCCESS", "NOT_FOUND", "ERROR"
    error_msg: Optional[str] = None
