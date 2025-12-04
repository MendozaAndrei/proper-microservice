import connexion
from apscheduler.schedulers.background import BackgroundScheduler
import yaml
import logging.config
import requests
import json
from datetime import datetime
import os

# Load configuration files
with open('/config/health_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/health_log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')


def get_health_status():
    """Get the current health status of all services"""
    logger.info("Health status request received")
    
    # Check if the health status file exists
    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Health status data does not exist")
        return {"message": "Health status data does not exist"}, 404
    
    # Read the health status from the file
    with open(app_config['datastore']['filename'], 'r') as f:
        health_status = json.load(f)
    
    logger.info("Health status returned successfully")
    return health_status, 200


def check_service_health():
    """Poll all services and update their health status"""
    logger.info("Started periodic health check")
    
    # Load existing health status or create default
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            health_status = json.load(f)
    else:
        health_status = {
            "receiver": "Unknown",
            "storage": "Unknown",
            "processing": "Unknown",
            "analyzer": "Unknown",
            "last_update": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
    
    # Check each service
    services = app_config['services']
    timeout = app_config['timeout']
    
    for service_name, service_url in services.items():
        try:
            logger.debug(f"Checking health of {service_name} at {service_url}")
            response = requests.get(service_url, timeout=timeout)
            
            if response.status_code == 200:
                health_status[service_name] = "Running"
                logger.info(f"{service_name} is Running")
            else:
                health_status[service_name] = "Down"
                logger.warning(f"{service_name} is Down (status code: {response.status_code})")
                
        except requests.exceptions.Timeout:
            health_status[service_name] = "Down"
            logger.warning(f"{service_name} is Down (timeout)")
        except requests.exceptions.ConnectionError:
            health_status[service_name] = "Down"
            logger.warning(f"{service_name} is Down (connection error)")
        except Exception as e:
            health_status[service_name] = "Down"
            logger.error(f"Error checking {service_name}: {e}")
    
    # Update timestamp
    health_status["last_update"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Write updated health status to file
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(health_status, f, indent=4)
    
    logger.debug(f"Updated health status: {health_status}")
    logger.info("Periodic health check completed")


def init_scheduler():
    """Initialize the background scheduler"""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        check_service_health, 
        'interval', 
        seconds=app_config['scheduler']['interval']
    )
    sched.start()
    logger.info(f"Scheduler started with interval of {app_config['scheduler']['interval']} seconds")


# Create Connexion app
app = connexion.App(__name__, specification_dir=".")

# Add API with base_path
app.add_api("openapi.yml", 
            base_path="/health",
            strict_validation=True, 
            validate_responses=True)

# Conditional CORS
if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    from flask_cors import CORS
    CORS(app.app, resources={r"/*": {"origins": "*"}})
    logger.info("CORS enabled for all origins")

if __name__ == "__main__":
    # Initialize scheduler before starting the app
    init_scheduler()
    app.run(port=8120, host="0.0.0.0")