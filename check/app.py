# PROCESSING APP.PY
import connexion
from apscheduler.schedulers.background import BackgroundScheduler
import yaml
from connexion import FlaskApp
import logging.config
import requests
import json
from datetime import datetime
import os
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
from flask_cors import CORS
import httpx
with open('/config/check_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/check_logs.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    
logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')


RECEIVER_URL = app_config['services']['receiver']
STORAGE_URL = app_config['services']['storage']
PROCESSING_URL = app_config['services']['processing']
ANALYZER_URL = app_config['services']['analyzer']



def check_services(): 
    num_available = 0
    # Load existing health status or create default
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            check_status = json.load(f)
    else:
        check_status = {
            "receiver": "No Data",
            "storage": "No Data",
            "processing": "No Data",
            "analyzer": "No Data",
        }
    # points to config file
    services = app_config['services']
    timeout = app_config['timeout'] # Checks the timout in seconds, set the service as "down" if no response within this time


    receiver_status = "down"
    try: 
        response = httpx.get(RECEIVER_URL, timeout=timeout)
        if response.status_code == 200:
            response_data = response.json()
            receiver_status = f"Receiver is healthy at{response_data.get('status_datetime')}"
            logger.info("The RECEIVER  service is running healthy")
            num_available +=1 
        else: 
            logger.info("The RECEIVER service is down")

    except (httpx.TimeoutException): 
        logger.info("Receiver service is not available")

    check_status['receiver'] = receiver_status
#  ================= END OF RECEIVER CHECK ================


# ================= STORAGE CHECK ================
    storage_status = "down"
    try: 
        response = httpx.get(STORAGE_URL, timeout=timeout)
        if response.status_code == 200:
            response_data = response.json()
            storage_status = f"Storage has {response_data.get('num_temp')} temp and {response_data.get('num_airquality')} airquality events"
            logger.info("The STORAGE  service is running healthy")
            num_available +=1 
        else: 
            logger.info("The STORAGE service is down")
    except (httpx.TimeoutException): 
        logger.info("Storage service is not available")

    check_status['storage'] = storage_status

#  ================= END OF STORAGE CHECK ================


# ================= PROCESSING CHECK ================

    processing_status = "down"
    try: 
        response = httpx.get(PROCESSING_URL, timeout=timeout)
        if response.status_code == 200:
            response_data = response.json()
            processing_status = f"Processing has {response_data.get('num_temp_readings')} temperature and {response_data.get('num_airquality_readings')} air quality events"
            logger.info("The PROCESSING  service is running healthy")
            num_available +=1 
        else: 
            logger.info("The PROCESSING service is down")
    except (httpx.TimeoutException): 
        logger.info("PROCESSING service is not available")

    check_status["processing"] = processing_status
# ================= END OF PROCESSING CHECK ================

# ================= START OF ANALYZER ================

    analyzer_status = "down"
    try: 
        response = httpx.get(ANALYZER_URL, timeout=timeout)
        if response.status_code == 200:
            response_data = response.json()
            analyzer_status = f"Analyzer has processed {response_data.get('num_events', 0)} events"
            logger.info("The ANALYZER  service is running healthy")
            num_available +=1 
        else: 
            logger.info("The ANALYZER service is down")
    except (httpx.TimeoutException): 
        logger.info("ANALYZER service is not available")
        
    check_status["analyzer"] = analyzer_status


    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(check_status, f, indent=4)
    
    logger.info(f"Completed Check - {num_available} services available")
    
    return {"service_count": num_available}, 201


def get_checks():
    """Returns the current status of all services from the datastore"""
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            check_status = json.load(f)
        logger.info("Retrieved service status from datastore")
        return check_status, 200
    else:
        logger.error("Check datastore file not found")
        return {"message": "No check data available"}, 404


app = FlaskApp(__name__)

# LAB 12: Add base_path
app.add_api("openapi.yml", 
            base_path="/check",  
            strict_validation=True, 
            validate_responses=True)

# LAB 12: Conditional CORS
if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    from flask_cors import CORS
    CORS(app.app, resources={r"/*": {"origins": "*"}})
    logger.info("CORS enabled for all origins")

if __name__ == "__main__":
    app.run(port=8130, host="0.0.0.0")


# app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)
