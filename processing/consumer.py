import json
import time
import re
from kafka import KafkaConsumer, TopicPartition
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
import pandera as pa
from pandera.typing import DataFrame, Series
from utils.logger import get_logger
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from datetime import datetime, timezone

logger = get_logger("Consumer")

KAFKA_BOOTSTRAP_SERVER = 'localhost:19092'
INPUT_TOPIC = 'raw_scrapes'
DLQ_TOPIC = 'dead_letter_queue' #? Where bad data goes to die
DB_CONNECTION = "postgresql://retail_user:retail_password@localhost:5432/retail_db"

Base = declarative_base()

# Metrics Definition

# Counts how many messages were processed, split by outcome
# Lets you see throughput and error rate (is the pipeline healthy or failing?)
SCRAPES_PROCESSED = Counter(
    "retail_scrapes_total",
    "Total scrapes processed",
    ["status"]  # success | error
)

# Measures end-to-end time taken to process a single Kafka message
# Used to track latency (P50/P95/P99) and detect slowdowns
PROCESSING_TIME = Histogram(
    "retail_processing_seconds",
    "Time spent processing a message"
)

# Measures how long database writes take
# Helps identify whether Postgres is the bottleneck
DB_WRITE_TIME = Histogram(
    "retail_db_write_seconds",
    "Time spent writing to Postgres"
)

# Tracks how far the consumer is behind the latest Kafka offset
# If this keeps growing, the consumer cannot keep up with incoming data (backpressure detection)
KAFKA_LAG = Gauge(
    "kafka_consumer_lag",
    "Difference between latest offset and consumer offset",
    ["topic", "partition"]
)

#! Upon changing the schema, make sure you delete the old schema table or create a new one
class ProductModel(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    currency = Column(String(3))
    in_stock = Column(Boolean)
    url = Column(String, unique=True) #? Prevent duplicates, upsert logic
    scraped_at = Column(DateTime(timezone=True), nullable=False)

engine = create_engine(DB_CONNECTION)
Base.metadata.create_all(engine) #? Creates table if not exists
Session = sessionmaker(bind=engine)

# TODO: Remove this after pandera validation is implemented
class ProductSchema(pa.DataFrameModel):
    product_name: str
    price_raw: str = pa.Field(str_matches=r"^[£$€]?\d+(\.\d+)?$") 
    url: str = pa.Field(str_startswith="http")

def clean_price(price_str):
    """Extracts numeric value from string like $51.77"""
    clean = re.sub(r'[^\d.]', '', price_str)
    return float(clean)

def process_message(msg_value):
    """
    1. Validate Raw Data
    2. Clean/Transform
    3. Write to DB
    """
    session = Session()
    start_time = time.time()  
    status = "success"  

    try:
        data = json.loads(msg_value)

        # 1. Validation
        # TODO: Convert to DataFrame to use Pandera fully
        if not data.get('url') or 'price_raw' not in data:
            raise ValueError("Missing critical fields")

        # 2. Transformation
        price = clean_price(data['price_raw'])
        is_in_stock = "In stock" in data.get('availability_raw', '')

        scraped_ts = datetime.fromtimestamp(
            data['scraped_at'],
            tz=timezone.utc
        )

        # 3. Upsert Logic (Update if exists, Insert if new) 
        existing = session.query(ProductModel).filter_by(url=data['url']).first()
        
        if existing:
            existing.price = price
            existing.in_stock = is_in_stock
            logger.info(f" [OK] Updated: {data['product_name'][:20]}")
        else:
            new_product = ProductModel(
                name=data['product_name'],
                price=price,
                currency="USD", # TODO: Extract currency from raw data
                in_stock=is_in_stock,
                url=data['url'],
                scraped_at=scraped_ts
            )
            session.add(new_product)
            logger.info(f" [OK] Inserted: {data['product_name'][:20]}")

        with DB_WRITE_TIME.time():
            session.commit()

    except Exception as e:
        session.rollback()
        status = "error"
        logger.error(f" [x] Error processing message: {e}")
        # TODO: Send to DLQ (Dead Letter Queue) Topic
    finally:
        session.close()
        duration = time.time() - start_time
        PROCESSING_TIME.observe(duration)
        SCRAPES_PROCESSED.labels(status=status).inc()

def run_consumer():
    logger.info(" [~] Metrics server running on port 8000")
    start_http_server(8000)

    logger.info(" [~] Listening for messages on 'raw_scrapes'...")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        auto_offset_reset='earliest',
        group_id='retail_consumer_group_v1', # Crucial for scaling
        enable_auto_commit=False #? In production systems, this is often set to False and commits are done after successful processing
    )

    # # Note: Updating lag for every message, if we have 1000 incoming message, it will update 1000 times, which increases the overhead of 1000 gRPC calls per second as well

    # for message in consumer:
    #     tp = TopicPartition(message.topic, message.partition)
    #     end_offset = consumer.end_offsets([tp])[tp]
    #     lag = end_offset - message.offset

    #     KAFKA_LAG.labels(
    #         topic=message.topic,
    #         partition=str(message.partition)
    #     ).set(lag)

    #     process_message(message.value)
    #     consumer.commit()

    message_count = 0
    for message in consumer:
        process_message(message.value)
        
        # Check lag every 50 messages to save network bandwidth
        message_count += 1
        if message_count % 50 == 0:
            tp = TopicPartition(message.topic, message.partition)
            end_offset = consumer.end_offsets([tp])[tp]
            lag = end_offset - message.offset
            KAFKA_LAG.labels(topic=message.topic, partition=str(message.partition)).set(lag)
            
        consumer.commit()

if __name__ == "__main__":
    run_consumer()