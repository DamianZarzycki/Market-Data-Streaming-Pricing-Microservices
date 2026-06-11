import time
import logging
from datetime import datetime, timezone
from urllib import request
from models import ServiceStatus

health_cache = {}

previous_states = {
    "market-data-service": None,
    "pricing-service": None
}

def check_service_health(url, service_name):
    start_time = time.perf_counter()
    last_checked = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )    
    logging.info(f"Checking health for {service_name}...")
    
    try:
        with request.urlopen(url, timeout=2) as response:
            response.read()
            end_time = time.perf_counter()
            response_time_ms = int((end_time - start_time) * 1000)
            
            return {
                "status": ServiceStatus.UP.value,
                "last_checked": last_checked,
                "response_time_ms": response_time_ms,
            }
    except Exception as e:
        end_time = time.perf_counter()
        response_time_ms = int((end_time - start_time) * 1000)
        raw_error = str(e.reason) if hasattr(e, "reason") else str(e)

        if "]" in raw_error:
            clean_error = raw_error.split("]")[-1].strip()
        else:
            clean_error = raw_error
            
        return {
            "status": ServiceStatus.DOWN.value,
            "error": clean_error,
            "last_checked": last_checked,
        }

def monitoring_worker():
    global health_cache
    
    while True:
        market_data_status = check_service_health("http://market-data-service:8001/health", "market-data-service")
        pricing_status = check_service_health("http://pricing-service:8002/health", "pricing-service")

        current_states = {
            "market-data-service": market_data_status,
            "pricing-service": pricing_status,
        }

        for service_name, status_data in current_states.items():
            current_status = status_data["status"]
            prev_status = previous_states[service_name]

            if current_status == ServiceStatus.DOWN.value and prev_status != ServiceStatus.DOWN.value:
                logging.warning(f"Service Unavailability Detected: {service_name} is DOWN! Error: {status_data.get('error')}")
            
            elif current_status == ServiceStatus.UP.value and prev_status == ServiceStatus.DOWN.value:
                logging.info(f"Service Recovery Detected: {service_name} is back UP!")

            previous_states[service_name] = current_status

        health_cache = current_states

        time.sleep(1)