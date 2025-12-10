# STORAGE APP.PY
import os
import connexion
from sqlalchemy import create_engine, Integer, String, Float, DateTime, func, BigInteger, text, select
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker
from datetime import datetime
import pymysql
import yaml
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import json


#================= Lab 4 Code Added ==============================
#Opens the app_conf.yml configuration to load. 
with open('/config/storage_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

#Opens the log_conf.yml for configuration
with open("/config/storage_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())

#Sets up logging from the configuration file. 
#Creates a logging instance to write logs basically. 
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

#This is the Database Configuration setup
#This is where we set up the database connection details.
# This is where the database, like the user and such is created and set.
db_config = app_config['datastore']



# This verifies the connection to the database using the configuration details provided. 
try:
    #Uses "keys" to grab the value and to create the things needed to connect to the network. 
    connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['hostname']}:{db_config['port']}/{db_config['db']}"
    # mysql = create_engine(connection_string, future=True)
    # Lab12
    mysql = create_engine(
        connection_string,
        pool_size=10,
        pool_recycle=3600,
        pool_pre_ping=True
    )
    # Logs that the connection is successful and is connected
    logger.info("Connected to the database")
except Exception as e:
    logger.error(f"Error: {e}")
    mysql = create_engine("sqlite:///storage.db", future=True)

SessionLocal = sessionmaker(bind=mysql)

#Required for the MySQL Mapping. (Received a little help for this one.)
# Without the base declarative I receive the error of failure. 
class Base(DeclarativeBase):
    pass

# I don't know where you'd want this to be in so I just added in app.py storage
class Temperature(Base):
    __tablename__ = "temperature"
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(BigInteger, nullable=False)
    fire_id = mapped_column(String(250), nullable=False)
    latitude = mapped_column(Float, nullable=False)
    longitude = mapped_column(Float, nullable=False)
    temperature_celsius = mapped_column(Float, nullable=False)
    humidity_level = mapped_column(Float, nullable=True)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    reading_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        """Convert Temperature object to dictionary matching the OpenAPI schema"""
        return {
            "trace_id": self.trace_id,
            "fire_id": self.fire_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "temperature_celsius": self.temperature_celsius,
            "humidity_level": self.humidity_level,
            "batch_timestamp": self.batch_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "reading_timestamp": self.reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }


class AirQuality(Base):
    __tablename__ = "airquality"
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(BigInteger, nullable=False)
    fire_id = mapped_column(String(250), nullable=False)
    location_name = mapped_column(String(250), nullable=False)
    particulate_level = mapped_column(Float, nullable=False)
    air_quality = mapped_column(Float, nullable=False)
    smoke_opacity = mapped_column(Float, nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    reading_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

    def to_dict(self):
        """Convert AirQuality object to dictionary matching the OpenAPI schema"""
        return {
            "trace_id": self.trace_id,
            "fire_id": self.fire_id,
            "location_name": self.location_name,
            "particulate_level": self.particulate_level,
            "air_quality": self.air_quality,
            "smoke_opacity": self.smoke_opacity,
            "batch_timestamp": self.batch_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "reading_timestamp": self.reading_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }

Base.metadata.create_all(mysql)
logger.info("Database tables created/verified")
def create_temperature_reading(body):
    session = SessionLocal()
    logger.debug(f"Storing {body['trace_id']} to the database")

    humidity = None
    if "humidity_level" in body and body["humidity_level"] is not None:
        humidity = float(body["humidity_level"])
    # Time stamps are formatted differently, so I added this to convert them into the datetime format to remove any conflicts
    batch_timestamp = datetime.fromisoformat(body["batch_timestamp"].replace('Z', '+00:00'))
    reading_timestamp = datetime.fromisoformat(body["reading_timestamp"].replace('Z', '+00:00'))
        
    event = Temperature(
        trace_id=int(body["trace_id"]),
        fire_id=body["fire_id"],
        latitude=float(body["latitude"]),
        longitude=float(body["longitude"]),
        temperature_celsius=float(body["temperature_celsius"]),
        humidity_level=humidity,
        batch_timestamp=batch_timestamp,
        reading_timestamp=reading_timestamp,
    )
    session.add(event)
    session.commit()
    session.close()
    # Log message when event is successfully stored
    logger.debug(f"Stored event temperature_reading with a trace id of {body['trace_id']}")
    return {"message": "stored"}, 201


def create_airquality_reading(body):
    session = SessionLocal()
    logger.debug(f"Storing {body['trace_id']} to the database")

    batch_timestamp = datetime.fromisoformat(body["batch_timestamp"].replace('Z', '+00:00'))
    reading_timestamp = datetime.fromisoformat(body["reading_timestamp"].replace('Z', '+00:00'))
    
    event = AirQuality(
        trace_id=int(body["trace_id"]),
        fire_id=body["fire_id"],
        location_name=body["location_name"],
        particulate_level=float(body["particulate_level"]),
        air_quality=float(body["air_quality"]),
        smoke_opacity=float(body["smoke_opacity"]),
        batch_timestamp=batch_timestamp,
        reading_timestamp=reading_timestamp,
    )
    session.add(event)
    session.commit()
    session.close()
    # Log message when event is successfully stored (after DB session is closed)
    logger.debug(f"Stored event airquality_reading with a trace id of {body['trace_id']}")
    return {"message": "stored"}, 201

# =============================== Lab 6 
# def process_messages():
#     """ Process event messages from Kafka """
#     hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
#     topic_name = app_config['events']['topic']
    
#     logger.info(f"Connecting to Kafka at {hostname}")
    
#     client = KafkaClient(hosts=hostname)
#     topic = client.topics[str.encode(topic_name)]
    
#     # Create a consumer on a consumer group
#     consumer = topic.get_simple_consumer(
#         consumer_group=b'event_group',
#         reset_offset_on_start=False,
#         auto_offset_reset=OffsetType.LATEST
#     )
    
#     logger.info("Kafka consumer started, waiting for messages...")
    
#     # This is blocking - it will wait for new messages
#     for msg in consumer:
#         msg_str = msg.value.decode('utf-8')
#         msg = json.loads(msg_str)
#         logger.info(f"Message: {msg}")
        
#         payload = msg["payload"]
        
#         if msg["type"] == "temperature_reading":
#             # Store the temperature reading to the DB
#             create_temperature_reading(payload)
#             logger.info(f"Stored temperature_reading event with trace_id: {payload['trace_id']}")
            
#         elif msg["type"] == "airquality_reading":
#             # Store the air quality reading to the DB
#             create_airquality_reading(payload)
#             logger.info(f"Stored airquality_reading event with trace_id: {payload['trace_id']}")
        
#         # Commit the new message as being read
#         consumer.commit_offsets()

# ======================= LAB 12 
import time
import random
from pykafka.exceptions import KafkaException

class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.connect()
    
    def connect(self):
        """Infinite loop: will keep trying"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client():
                if self.make_consumer():
                    break
            # Sleep for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)
    
    def make_client(self):
        """
        Runs once, makes a client and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False
    
    def make_consumer(self):
        """
        Runs once, makes a consumer and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.consumer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic = self.client.topics[str.encode(self.topic)]
            self.consumer = topic.get_simple_consumer(
                consumer_group=b'event_group',
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            logger.info("Kafka consumer created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making consumer: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False
    
    def messages(self):
        """Generator method that catches exceptions in the consumer loop"""
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaException as e:
                msg = f"Kafka issue in consumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.connect()


def process_messages():
    """ Process event messages from Kafka """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    topic_name = app_config['events']['topic']
    
    logger.info(f"Connecting to Kafka at {hostname}")
    
    kafka_wrapper = KafkaWrapper(hostname, topic_name)
    
    logger.info("Kafka consumer started, waiting for messages...")
    
    # Use the wrapper's messages generator
    for msg in kafka_wrapper.messages():
        msg_str = msg.value.decode('utf-8')
        msg_data = json.loads(msg_str)
        logger.info(f"Message: {msg_data}")
        
        payload = msg_data["payload"]
        
        if msg_data["type"] == "temperature_reading":
            create_temperature_reading(payload)
            logger.info(f"Stored temperature_reading event with trace_id: {payload['trace_id']}")
            
        elif msg_data["type"] == "airquality_reading":
            create_airquality_reading(payload)
            logger.info(f"Stored airquality_reading event with trace_id: {payload['trace_id']}")
        
        # Commit the new message as being read
        kafka_wrapper.consumer.commit_offsets()


def setup_kafka_thread():
    """Setup Kafka consumer thread"""
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    logger.info("Kafka consumer thread started")


# ==============================FINALS================================


def get_events_stats():
    session = SessionLocal()
    
    logger.info("STATS event initiated: COUNTING EVENTS ON DATABASE")
    
    temp_count = select(func.count()).select_from(Temperature)
    airq_count = select(func.count()).select_from(AirQuality)
    
    
    session.close()
    
    stats = {
        "temperature_readings": temp_count,
        "airquality_readings": airq_count
    }
    
    logger.info(f"Event statistics: {stats}")
    
    return stats, 200


# ==============================FINALS================================
def get_temperature_readings(start_timestamp, end_timestamp):
    """Gets temperature readings between the start and end timestamps"""
    session = SessionLocal()
    
    logger.info(f"Query for Temperature readings between {start_timestamp} and {end_timestamp}")
    
    # Convert ISO format timestamps to datetime objects
    start_datetime = datetime.fromisoformat(start_timestamp.replace('Z', '+00:00'))
    end_datetime = datetime.fromisoformat(end_timestamp.replace('Z', '+00:00'))
    
    # Query the database for readings within the timestamp range
    statement = select(Temperature).where(
        Temperature.date_created >= start_datetime
    ).where(
        Temperature.date_created < end_datetime
    )
    
    results = [
        result.to_dict()
        for result in session.execute(statement).scalars().all()
    ]
    
    session.close()
    
    logger.info(f"Query for Temperature readings returns {len(results)} results")
    
    return results, 200


def get_airquality_readings(start_timestamp, end_timestamp):
    """Gets air quality readings between the start and end timestamps"""
    session = SessionLocal()
    
    logger.info(f"Query for Air Quality readings between {start_timestamp} and {end_timestamp}")
    
    # Convert ISO format timestamps to datetime objects
    start_datetime = datetime.fromisoformat(start_timestamp.replace('Z', '+00:00'))
    end_datetime = datetime.fromisoformat(end_timestamp.replace('Z', '+00:00'))
    
    # Query the database for readings within the timestamp range
    statement = select(AirQuality).where(
        AirQuality.date_created >= start_datetime
    ).where(
        AirQuality.date_created < end_datetime
    )
    
    results = [
        result.to_dict()
        for result in session.execute(statement).scalars().all()
    ]
    
    session.close()
    
    logger.info(f"Query for Air Quality readings returns {len(results)} results")
    
    return results, 200


#============Assignment 1
def health():
    return {"status": "healthy"}, 200


app = connexion.App(__name__, specification_dir=".")
# LAB 12: Add base_path
app.add_api("openapi.yaml", 
            base_path="/storage",  # <--- ADD THIS
            strict_validation=True, 
            validate_responses=True)
# LAB 12: Conditional CORS (replace the existing CORS lines)
if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    from flask_cors import CORS
    CORS(app.app, resources={r"/*": {"origins": "*"}})
    logger.info("CORS enabled for all origins")

if __name__ == "__main__":
    logger.info("Database tables created/verified")
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")