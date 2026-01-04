import json
import re
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
import pandera as pa
from pandera.typing import DataFrame, Series
from utils.logger import get_logger

logger = get_logger("Consumer")

KAFKA_BOOTSTRAP_SERVER = 'localhost:19092'
INPUT_TOPIC = 'raw_scrapes'
DLQ_TOPIC = 'dead_letter_queue' #? Where bad data goes to die
DB_CONNECTION = "postgresql://retail_user:retail_password@localhost:5432/retail_db"

Base = declarative_base()

class ProductModel(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)
    currency = Column(String(3))
    in_stock = Column(Boolean)
    url = Column(String, unique=True) #? Prevent duplicates, upsert logic
    scraped_at = Column(DateTime(timezone=True), server_default=func.now())

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
    try:
        data = json.loads(msg_value)
        logger.info(f" [OK] Received: {data.get('product_name', 'Unknown')[:20]}...")

        # 1. Validation
        # TODO: Convert to DataFrame to use Pandera fully
        if not data.get('url') or 'price_raw' not in data:
            raise ValueError("Missing critical fields")

        # 2. Transformation
        price = clean_price(data['price_raw'])
        is_in_stock = "In stock" in data.get('availability_raw', '')

        # 3. Upsert Logic (Update if exists, Insert if new) 
        existing = session.query(ProductModel).filter_by(url=data['url']).first()
        
        if existing:
            existing.price = price
            existing.in_stock = is_in_stock
            existing.scraped_at = func.now()
            logger.info(f" [OK] Updated: {data['product_name'][:20]}")
        else:
            new_product = ProductModel(
                name=data['product_name'],
                price=price,
                currency="USD", # TODO: Extract currency from raw data
                in_stock=is_in_stock,
                url=data['url']
            )
            session.add(new_product)
            logger.info(f" [OK] Inserted: {data['product_name'][:20]}")

        session.commit()

    except Exception as e:
        session.rollback()
        logger.error(f" [x] Error processing message: {e}")
        # TODO: Send to DLQ (Dead Letter Queue) Topic
    finally:
        session.close()

def run_consumer():
    logger.info(" [~] Listening for messages on 'raw_scrapes'...")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        auto_offset_reset='earliest', # Start from beginning if no history
        group_id='retail_consumer_group_v1', # Crucial for scaling
        enable_auto_commit=False #? In production systems, this is often set to False and commits are done after successful processing
    )

    for message in consumer:
        process_message(message.value)
        consumer.commit()

if __name__ == "__main__":
    run_consumer()