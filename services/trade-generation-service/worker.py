
import logging
import time
import trade_intentions_service
import os
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

audit_logger = AuditLogger("trade-generation-service")

def trade_generation_worker():
    logging.info("Trade generation worker started.")
    audit_logger.info(EventType.WORKER_STARTED, "Trade generation worker started")
    while trade_intentions_service.generator_state["is_running"]:
        trade_intentions_service.generate_and_send_to_trade_action_service()
            
        time.sleep(int(os.getenv("TICK_INTERVAL_MS", 500)) / 1000.0)
        
    logging.info("Trade generation worker stopped.")
    audit_logger.info(EventType.WORKER_STOPPED, "Trade generation worker stopped")