
import logging
import generator
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType

audit_logger = AuditLogger("trade-generation-service")

# One OPEN or CLOSE at a time, slow enough to watch against live provider quotes.
PROVIDER_LOOP_INTERVAL_MS = 15_000

def trade_generation_worker():
    logging.info("Trade generation worker started.")
    audit_logger.info(EventType.WORKER_STARTED, "Trade generation worker started")
    try:
        while not generator.generator_should_stop():
            generator.generate_and_send_to_trade_action_service()

            interval_ms = generator.generator_config["interval_ms"]
            if generator.generator_config.get("use_provider_data"):
                interval_ms = max(interval_ms, PROVIDER_LOOP_INTERVAL_MS)
            generator.wait_for_stop(max(interval_ms, 50) / 1000.0)
    finally:
        generator.mark_generator_stopped()
        logging.info("Trade generation worker stopped.")
        audit_logger.info(EventType.WORKER_STOPPED, "Trade generation worker stopped")
