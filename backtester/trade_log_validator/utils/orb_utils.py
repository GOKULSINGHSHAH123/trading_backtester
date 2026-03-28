import re
from datetime import datetime

class OrbUtils:
    # ---------- REGEX PATTERNS ----------
    INDEX_OPTION_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX|BANKEX)")
    
    INDEX_FUTURE_PATTERN  = re.compile(r"^(NIFTY|BANKNIFTY|FINNIFTY)(\d{2}[A-Z]{3}FUT|-I)")
    
    INDEX_SPOT_PATTERN    = re.compile(
        r"^(NIFTY 50|NIFTY BANK|NIFTY FIN SERVICE|NIFTY AUTO|NIFTY REALTY|"
        r"NIFTY PHARMA|NIFTY OIL AND GAS|NIFTY MEDIA|NIFTY IT|NIFTY FMCG|SENSEX)"
    )
    
    # OPTION ex: NIFTY25FEB2119500CE
    OPTION_PATTERN        = re.compile(
        r"^([A-Z0-9.&_\-]+)"          # symbol
        r"(\d{1,2}[A-Z]{3}\d{2})"     # expiry (fixed: \"d{2})
        r"(\d{1,7})"                  # strike
        r"(CE|PE)$"                   # option type
    )

    FUTURE_PATTERN        = re.compile(
        r"(\d{2}[A-Z]{3}FUT|-(I|II|III))$"
    )

    @classmethod
    def get_db_name(cls, sym: str) -> str:
        if cls.OPTION_PATTERN.search(sym):
            if cls.INDEX_OPTION_PATTERN.search(sym):
                return "index_options_db"   
            else:
                return "stock_options_db"   

        elif cls.FUTURE_PATTERN.search(sym):
            if cls.INDEX_FUTURE_PATTERN.search(sym):
                return "index_futures_db"   
            else:
                return "stock_futures_db"   

        else:
            if cls.INDEX_SPOT_PATTERN.search(sym):
                return "index_db"    
            else:
                return "stocks_db"    

    @classmethod
    def get_collection_name(cls, ti: int, fmt: str = "%Y") -> str:
        return datetime.fromtimestamp(ti).strftime(fmt)
