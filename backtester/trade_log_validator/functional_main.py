from .core.orchestrator import ValidationOrchestrator
from typing import Optional

def main(
    ORB_URL: str,
    ORB_USERNAME: str,
    ORB_PASSWORD: str,
    algo_name: str,
    lot_size_file_path: Optional[str],
    trade_log_path: str,
    segment: str,
    is_commodity_futures: bool,
    output_path: str
) -> None:
    """
    Entry point for the Trade Log Validator.
    Strictly adheres to the mandate interface.
    """
    orchestrator = ValidationOrchestrator(
        output_path=output_path,
        algo_name=algo_name
    )
    
    orb_config = {
        "url": ORB_URL,
        "username": ORB_USERNAME,
        "password": ORB_PASSWORD
    }
    
    orchestrator.run(
        trade_log_path=trade_log_path,
        lot_size_path=lot_size_file_path,
        segment=segment,
        is_commodity_futures=is_commodity_futures,
        orb_config=orb_config
    )