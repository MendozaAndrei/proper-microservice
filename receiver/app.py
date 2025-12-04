import datetime
import json
import time
import random
import connexion
from connexion import NoContent
import time
import yaml
import logging.config
from pykafka import KafkaClient

# Loads External Configuration File. This is used specifically for LOGGING agent. 
with open("/config/receiver_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')
# Loads External Configuration File. This is used specifically for KAFKA agent. 
with open('/config/receiver_conf.yml', 'r') as f:

    app_config = yaml.safe_load(f.read())
logger.info("File read successful")


# Kafka configuration
KAFKA_HOSTNAME = app_config['events']['hostname']
KAFKA_PORT = app_config['events']['port']
KAFKA_TOPIC = app_config['events']['topic']

# Create Kafka client and producer once at startup (REUSE IT!)
# This prevents the threading errors and improves performance
try:
    client = KafkaClient(hosts=f'{KAFKA_HOSTNAME}:{KAFKA_PORT}')
    topic = client.topics[str.encode(KAFKA_TOPIC)]
    producer = topic.get_producer(sync=True)
    logger.info(f"Successfully connected to Kafka at {KAFKA_HOSTNAME}:{KAFKA_PORT}")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    producer = None


def connect_to_kafka_with_retry(max_retries=5):
    """Connect to Kafka with exponential backoff"""
    for attempt in range(max_retries):
        try:
            client = KafkaClient(hosts=f'{KAFKA_HOSTNAME}:{KAFKA_PORT}')
            topic = client.topics[str.encode(KAFKA_TOPIC)]
            producer = topic.get_producer(sync=True)
            logger.info(f"Successfully connected to Kafka at {KAFKA_HOSTNAME}:{KAFKA_PORT}")
            return producer
        except Exception as e:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries reached. Kafka producer unavailable.")
                return None

def report_temperature_readings(body):
    """
    Receives temperature reading batches and sends them to Kafka
    Works even when storage service is down - messages are queued in Kafka
    """
    readings = body.get("readings", [])
    logger.info(f"Received body: {json.dumps(body, indent=2)}")
    
    if not producer:
        logger.error("Kafka producer is not available")
        return NoContent, 503  # Service Unavailable
    
    try:
        # Loop through the readings in the "readings" array
        for r in readings:
            # Autogenerate the trace_id using time in nanoseconds
            trace_id = time.time_ns()
            
            # Log when event is received
            logger.info(f"Received event temperature_reading with a trace id of {trace_id}")
            
            data = {
                "trace_id": trace_id,
                "fire_id": body["fire_id"],
                "latitude": body["latitude"],
                "longitude": body["longitude"],
                "temperature_celsius": r["temperature_celsius"],
                "humidity_level": r.get("humidity_level"),
                "batch_timestamp": body["reporting_timestamp"],
                "reading_timestamp": r["recorded_timestamp"],
            }
            
            # Create message for Kafka
            msg = {
                "type": "temperature_reading",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": data
            }
            msg_str = json.dumps(msg)
            
            # Send to Kafka (this works even if storage is down!)
            producer.produce(msg_str.encode('utf-8'))
            
            # Log successful send
            logger.debug(f"Sent temperature_reading (trace_id: {trace_id}) to Kafka")
            
    except Exception as e:
        logger.error(f"Error processing temperature readings: {e}")
        return NoContent, 500
    
    return NoContent, 201


def report_airquality_reading(body):
    """
    Receives air quality reading batches and sends them to Kafka
    Works even when storage service is down - messages are queued in Kafka
    """
    readings = body.get("readings", [])
    logger.info(f"Received body: {json.dumps(body, indent=2)}")

    if not producer:
        logger.error("Kafka producer is not available")
        return NoContent, 503  # Service Unavailable

    try:
        for r in readings:
            # Generate trace_id
            trace_id = time.time_ns()
            
            # Log the event
            logger.info(f"Received event airquality_reading with a trace id of {trace_id}")
            
            data = {
                "trace_id": trace_id,
                "fire_id": body["fire_id"],
                "location_name": body["location_name"],
                "particulate_level": body["particulate_level"],
                "air_quality": r["air_quality"],
                "smoke_opacity": r["smoke_opacity"],
                "batch_timestamp": body["reporting_timestamp"],
                "reading_timestamp": r["recorded_timestamp"],
            }

            # Create message for Kafka
            msg = {
                "type": "airquality_reading",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": data
            }
            msg_str = json.dumps(msg)
            
            # Send to Kafka (this works even if storage is down!)
            producer.produce(msg_str.encode('utf-8'))
            
            # Log successful send
            logger.debug(f"Sent airquality_reading (trace_id: {trace_id}) to Kafka")
            
    except Exception as e:
        logger.error(f"Error processing airquality readings: {e}")
        return NoContent, 500

    return NoContent, 201



#========================== ASSIGNMENT 1
def health():
    return {"status": "healthy"}, 200



# This connects the app.py to the openapi.yaml
# app = connexion.App(__name__, specification_dir=".")
# app.add_api("lab1.yaml", strict_validation=True, validate_responses=True)

# if __name__ == "__main__":
#     try:
#         app.run(port=8080, host="0.0.0.0")
#     finally:
#         # Clean up producer on shutdown
#         if producer:
#             try:
#                 producer.stop()
#                 logger.info("Kafka producer stopped cleanly")
#             except Exception as e:
#                 logger.error(f"Error stopping producer: {e}")


app = connexion.App(__name__, specification_dir=".")

# ADD base_path HERE
app.add_api("lab1.yaml", 
            base_path="/receiver",  # <--- ADD THIS LINE
            strict_validation=True, 
            validate_responses=True)
import os
# Conditional CORS
if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    from flask_cors import CORS
    CORS(app.app, resources={r"/*": {"origins": "*"}})
    logger.info("CORS enabled for all origins")

if __name__ == "__main__":
    try:
        app.run(port=8080, host="0.0.0.0")
    finally:
        # Clean up producer on shutdown
        if producer:
            try:
                producer.stop()
                logger.info("Kafka producer stopped cleanly")
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")