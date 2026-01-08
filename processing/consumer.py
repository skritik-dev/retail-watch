import json
import time
import re
import requests 
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
import pandera as pa
from pandera.typing import DataFrame, Series
from utils.logger import get_logger
from prometheus_client import start_http_server, Counter, Histogram, Gauge
from datetime import datetime, timezone

logger = get_logger("Consumer")

# Config
KAFKA_BOOTSTRAP_SERVER = 'localhost:19092'
INPUT_TOPIC = 'raw_scrapes'
DLQ_TOPIC = 'dead_letter_queue'
ALERTS_TOPIC = "alerts"  
DB_CONNECTION = "postgresql://retail_user:retail_password@localhost:5432/retail_db"
MODEL_API_URL = "http://localhost:8001/predict"

Base = declarative_base()

# Metrics
SCRAPES_PROCESSED = Counter("retail_scrapes_total", "Total scrapes processed", ["status"])
PROCESSING_TIME = Histogram("retail_processing_seconds", "Time spent processing a message")
DB_WRITE_TIME = Histogram("retail_db_write_seconds", "Time spent writing to Postgres")
KAFKA_LAG = Gauge("kafka_consumer_lag", "Difference between latest offset and consumer offset", ["topic", "partition"])
ALERTS_GENERATED = Counter("retail_alerts_total", "Total stock-out alerts generated")

class ProductModel(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    currency = Column(String(3))
    in_stock = Column(Boolean)
    url = Column(String, unique=True)
    scraped_at = Column(DateTime(timezone=True), nullable=False)

engine = create_engine(DB_CONNECTION)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Helper Functions
def clean_price(price_str):
    clean = re.sub(r'[^\d.]', '', price_str)
    return float(clean) if clean else 0.0

def get_prediction(price):
    """
    Calls the XGBoost Model API.
    Returns: (probability, is_alert)
    """
    # Mocking features for MVP (In prod, fetch history from Feature Store)
    payload = {
        "price": price,
        "price_diff": 0.0,
        "price_diff_percent": 0.0,
        "price_rolling_3": price,
        "price_std_3": 0.0
    }
    
    try:
        response = requests.post(MODEL_API_URL, json=payload, timeout=0.5)
        response.raise_for_status()
        result = response.json()
        return result['stock_out_probability'], result['alert_triggered']
    except Exception as e:
        logger.error(f" [!] Model API Failed: {e}")
        return 0.0, False

def process_message(msg_value, alert_producer):
    """
        Validate -> Predict -> Upsert DB -> Alert
    """
    session = Session()
    start_time = time.time()  
    status = "success"  

    try:
        data = json.loads(msg_value)

        # 1. Validation
        if not data.get('url') or 'price_raw' not in data:
            raise ValueError("Missing critical fields")

        # 2. Transformation
        price = clean_price(data['price_raw'])
        is_in_stock = "In stock" in data.get('availability_raw', '')
        
        # Handle Timestamp
        ts_raw = data.get('scraped_at')
        if ts_raw:
             scraped_ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        else:
             scraped_ts = datetime.now(timezone.utc)

        # 3. Model Inference 
        stock_out_prob, should_alert = 0.0, False
        if is_in_stock:
            stock_out_prob, should_alert = get_prediction(price)

        # 4. Upsert Logic
        existing = session.query(ProductModel).filter_by(url=data['url']).first()
        
        if existing:
            existing.price = price
            existing.in_stock = is_in_stock
            existing.scraped_at = scraped_ts  
            logger.info(f" [OK] Updated: {data['product_name'][:20]} (Prob: {stock_out_prob:.2f})")
        else:
            new_product = ProductModel(
                name=data['product_name'],
                price=price,
                currency="GBP", 
                in_stock=is_in_stock,
                url=data['url'],
                scraped_at=scraped_ts
            )
            session.add(new_product)
            logger.info(f" [OK] Inserted: {data['product_name'][:20]} (Prob: {stock_out_prob:.2f})")

        with DB_WRITE_TIME.time():
            session.commit()

        # 5. Alerting Logic
        if should_alert:
            alert_msg = {
                "product": data['product_name'],
                "price": price,
                "stock_out_prob": stock_out_prob,
                "url": data['url'],
                "alert_ts": time.time()
            }
            alert_producer.send(ALERTS_TOPIC, alert_msg)
            ALERTS_GENERATED.inc()
            logger.warning(f" [!] ALERT SENT: {data['product_name'][:15]} is at risk!")

    except Exception as e:
        session.rollback()
        status = "error"
        logger.error(f" [x] Error processing message: {e}")
    finally:
        session.close()
        duration = time.time() - start_time
        PROCESSING_TIME.observe(duration)
        SCRAPES_PROCESSED.labels(status=status).inc()

def run_consumer():
    logger.info(" [~] Metrics server running on port 8000")
    start_http_server(8000)

    # Initialize Producer for Alerts
    logger.info(" [~] Initializing Alert Producer.")
    alert_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    logger.info(" [~] Listening for messages on 'raw_scrapes'.")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        auto_offset_reset='latest', 
        group_id='retail_consumer_group_v2', 
        enable_auto_commit=False 
    )

    message_count = 0
    for message in consumer:
        process_message(message.value, alert_producer)
        
        # Check lag every 50 messages to save network bandwidth
        message_count += 1
        if message_count % 50 == 0:
            try:
                tp = TopicPartition(message.topic, message.partition)
                end_offset = consumer.end_offsets([tp])[tp]
                lag = end_offset - message.offset
                KAFKA_LAG.labels(topic=message.topic, partition=str(message.partition)).set(lag)
            except Exception as e:
                logger.warning(f" [x] Failed to update lag metrics: {e}")
            
        consumer.commit()

if __name__ == "__main__":
    run_consumer()