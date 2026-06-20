
import logging
import time
import trade_intentions_service
import os

def trade_generation_worker():
    logging.info("Trade generation worker started.")
    while trade_intentions_service.generator_state["is_running"]:
        trade_intentions_service.generate_and_send_to_trade_action_service()
            
        time.sleep(os.getenv("TICK_INTERVAL_MS", 500))
        
    logging.info("Trade generation worker stopped.")