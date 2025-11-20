import connexion
from apscheduler.schedulers.background import BackgroundScheduler
import yaml
import logging.config
import requests
import json
from datetime import datetime
import os

# Loads the configuration files
with open('/config/processing_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/processing_log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    
logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')


def get_stats():
    logger.info("Started Request for Statistics")
    
    # Check if the statistics file exists
    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404
    
    # Read the statistics from the file
    with open(app_config['datastore']['filename'], 'r') as f:
        stats = json.load(f)
    
    logger.debug(f"Statistics: {stats}")
    logger.info("Request for statistics has completed")
    
    return stats, 200


def populate_stats():
    logger.info("Started Periodic Processing")
        
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            stats = json.load(f)
    else:
        # Default statistics if file doesn't exist
        stats = {
            "num_temp_readings": 0,
            "max_temperature_celsius": 0,
            "num_airquality_readings": 0,
            "max_air_quality": 0,
            "last_updated": "2000-01-01T00:00:00Z"
        }
    
    # Get current datetime and last update datetime
    current_datetime = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    last_updated = stats["last_updated"]
    
    # Query temperature readings
    temp_response = requests.get(
        app_config['eventstores']['temperature']['url'],
        params={'start_timestamp': last_updated, 'end_timestamp': current_datetime}
    )
    
    if temp_response.status_code == 200:
        temp_readings = temp_response.json()
        logger.info(f"Received {len(temp_readings)} temperature readings")
        
        # Update statistics
        stats["num_temp_readings"] += len(temp_readings)
        
        # Calculate max temperature
        if len(temp_readings) > 0:
            max_temp = max([reading['temperature_celsius'] for reading in temp_readings])
            if max_temp > stats["max_temperature_celsius"]:
                stats["max_temperature_celsius"] = max_temp
    else:
        logger.error(f"Failed to get temperature readings. Status code: {temp_response.status_code}")
    
    # Query air quality readings
    airquality_response = requests.get(
        app_config['eventstores']['airquality']['url'],
        params={'start_timestamp': last_updated, 'end_timestamp': current_datetime}
    )
    
    if airquality_response.status_code == 200:
        airquality_readings = airquality_response.json()
        logger.info(f"Received {len(airquality_readings)} air quality readings")
        
        # Update statistics
        stats["num_airquality_readings"] += len(airquality_readings)
        
        # Calculate max air quality
        if len(airquality_readings) > 0:
            max_aq = max([reading['air_quality'] for reading in airquality_readings])
            if max_aq > stats["max_air_quality"]:
                stats["max_air_quality"] = max_aq
    else:
        logger.error(f"Failed to get air quality readings. Status code: {airquality_response.status_code}")
    
    # Update last_updated timestamp
    stats["last_updated"] = current_datetime
    
    # Write updated statistics to JSON file
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.debug(f"Updated statistics: {stats}")
    logger.info("Periodic processing has ended")


def init_scheduler():
    """Initialize the background scheduler"""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['interval'])
    sched.start()


# Create Connexion app
app = connexion.App(__name__, specification_dir=".")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
