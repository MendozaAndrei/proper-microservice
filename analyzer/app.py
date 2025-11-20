import yaml
import logging.config
from pykafka import KafkaClient
import json
from connexion import NoContent
import connexion


with open('/config/analyzer_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open("/config/analyzer_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')


KAFKA_HOSTNAME = app_config['events']['hostname']
KAFKA_PORT = app_config['events']['port']
KAFKA_TOPIC = app_config['events']['topic']


def get_temperature_reading(index):
    logger.info("Get Temperature Reading initiated")
    try:

        client = KafkaClient(hosts=f"{KAFKA_HOSTNAME}:{KAFKA_PORT}")
        topic= client.topics[KAFKA_TOPIC.encode()]

        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
        counter = 0
        logger.info("Consumer Received")
        for msg in consumer:
            message = msg.value.decode("utf-8")
            data = json.loads(message)

            if data.get('type') == 'temperature_reading':
                if counter == index:
                    logger.info(f"Payload found {data['payload']}")
                    return {"message": data['payload']}, 201
                
                counter+=1

        # If we finish the loop without finding the index
        logger.info(f"Temperature reading at index {index} not found")
        return {"message": "Error: 404, not found"}, 404

    except Exception as e:
        logger.error(f"Error received: {e}")
        return {"message": "Internal Server Error:500"}, 500


def get_airquality_reading(index):
    logger.info("Get Airquality Reading")
    try:

        client = KafkaClient(hosts=f"{KAFKA_HOSTNAME}:{KAFKA_PORT}")
        topic= client.topics[KAFKA_TOPIC.encode()]

        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
        counter = 0



        logger.info("Consumer Received")
        for msg in consumer:
            message = msg.value.decode("utf-8")
            data = json.loads(message)

            if data.get('type') == 'airquality_reading':
                if counter == index:
                    logger.info(f"Payload found {data['payload']}")
                    return {"message": data['payload']}, 201
                
                counter+=1
        logger.info(f"Temperature reading at index {index} not found")
        return {"message": "Not Found"}, 404

    except Exception as e:
        logger.error(f"Error received: {e}")
        return {"message": "Internal Server Error"}, 500


def get_reading_stats():
    logger.info("Getting Stats")


    try:
        client = KafkaClient(hosts=f"{KAFKA_HOSTNAME}:{KAFKA_PORT}")
        topic = client.topics[KAFKA_TOPIC.encode()]

        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
        air_count = 0
        temp_count = 0
        #Created a dictionary and shit
        data_to_send = {
            "num_temperature_readings": 0,
            "num_airquality_readings": 0 
        }
        for msg in consumer: 
            message = msg.value.decode("utf-8")
            data = json.loads(message)

            if data.get("type") == "temperature_reading":
                temp_count+=1

            elif data.get("type") == "airquality_reading":
                air_count+=1


        data_to_send["num_temperature_readings"] = temp_count
        data_to_send["num_airquality_readings"] = air_count

        return data_to_send, 200
    
    except Exception as e: 
        logger.error(f"Error received: {e}")
        return {"message": "Nothing Found"}, 401
    return {"message" : "Nothing found"}, 404



app = connexion.App(__name__, specification_dir=".")
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # Added "host" to keep the "localhost" link stil lworking and not have to change anything 
    # 
    app.run(port=8110, host="0.0.0.0")
