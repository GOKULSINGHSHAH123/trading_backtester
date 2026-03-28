import logging
import time
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from datetime import datetime

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type # type: ignore

from ..core.exceptions import OrbAPIError
from ..models.api_models import LTPRequest, LTPResponse
from ..utils.orb_utils import OrbUtils

logger = logging.getLogger(__name__)
logger.propagate = False

class OrbService:
    """
    Optimized Orb API client with batching and retry logic.
    """
    
    BATCH_SIZE = 500  # Max items per query.$or clause
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.token = None
        self.token_expiry = 0

    def _ensure_auth(self):
        """Ensure valid authentication token exists."""
        if self.token and time.time() < self.token_expiry:
            return

        logger.info("Authenticating with Orb API...")
        try:
            payload = {"username": self.username, "password": self.password}
            resp = self.session.post(f"{self.base_url}/api/auth/token", data=payload, timeout=10)
            resp.raise_for_status()
            
            data = resp.json()
            self.token = data.get("access_token")
            # Assume 1 hour expiry if not provided, buffer by 5 minutes
            self.token_expiry = time.time() + 3500 
            self.session.headers.update({'Authorization': f'Bearer {self.token}'})
            logger.info("Authentication successful.")
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise OrbAPIError(f"Authentication failed: {e}") from e

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, OrbAPIError)),
        reraise=True
    )
    def _execute_query(self, db: str, collection: str, criteria: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute a single batch query against a specific DB/Collection.
        """
        self._ensure_auth()
        
        payload = {
            "db": db,
            "collection": collection,
            "query": {
                "$or": criteria
            }
        }
        
        try:
            resp = self.session.post(f"{self.base_url}/api/data/find", json=payload, timeout=30)
            resp.raise_for_status()
            
            data = resp.json()
            if "data" not in data:
                raise OrbAPIError(f"Invalid API response format: missing 'data' field. Response: {str(data)[:200]}")
                
            return data["data"]
            
        except requests.RequestException as e:
            logger.warning(f"API request failed (retrying): {e}")
            raise OrbAPIError(f"API request failed: {e}") from e

    def fetch_ltp_data(self, requests_list: List[LTPRequest]) -> List[LTPResponse]:
        """
        Fetch LTP data for a list of requests.
        Groups requests by DB and Collection, then batches them.
        """
        if not requests_list:
            return []

        
        grouped_requests: Dict[Tuple[str, str], List[Tuple[int, LTPRequest]]] = defaultdict(list)
        
        for req in requests_list:
            
            ti = int(req.timestamp.timestamp())
            
            db = OrbUtils.get_db_name(req.symbol)
            collection = OrbUtils.get_collection_name(ti)
            
            grouped_requests[(db, collection)].append((ti, req))

        results: List[LTPResponse] = []
        
        total_groups = len(grouped_requests)
        logger.info(f"Fetching LTP data: {len(requests_list)} requests in {total_groups} db/coll groups.")

        for (db, collection), items in grouped_requests.items():
            # Batching within group
            for i in range(0, len(items), self.BATCH_SIZE):
                batch_items = items[i : i + self.BATCH_SIZE]
                
                # Construct query criteria
                criteria = [{"sym": req.symbol, "ti": ti} for ti, req in batch_items]
                
                try:
                    # Execute
                    raw_data = self._execute_query(db, collection, criteria)
                    
                    data_map = {} # (sym, ti) -> price
                    for row in raw_data:
                        if "sym" in row and "ti" in row and "c" in row:
                            data_map[(row["sym"], row["ti"])] = float(row["c"])
                    
                    # Create responses
                    for ti, req in batch_items:
                        key = (req.symbol, ti)
                        if key in data_map:
                            results.append(LTPResponse(
                                symbol=req.symbol,
                                timestamp=req.timestamp,
                                ltp=data_map[key],
                                status="SUCCESS"
                            ))
                        else:
                            results.append(LTPResponse(
                                symbol=req.symbol,
                                timestamp=req.timestamp,
                                ltp=0.0,
                                status="NOT_FOUND",
                                error_msg="Data not present in DB"
                            ))
                            
                except Exception as e:
                    logger.error(f"Failed to fetch batch for {db}.{collection}: {e}")
                    # Fail the whole batch
                    for ti, req in batch_items:
                        results.append(LTPResponse(
                            symbol=req.symbol,
                            timestamp=req.timestamp,
                            ltp=0.0,
                            status="ERROR",
                            error_msg=str(e)
                        ))

        return results
