import psycopg2
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import time
from utils.logger import get_logger

logger = get_logger("Check Setup")

def check_postgres():
    logger.info(" [~] Testing Postgres connection...")
    try:
        conn = psycopg2.connect(
            dbname="retail_db", user="retail_user", 
            password="retail_password", host="localhost", port="5432"
        )
        logger.info(" [OK] Postgres is UP and accepting connections.")
        conn.close()
    except Exception as e:
        logger.error(f" [x] Postgres Failed: {e}")

def check_redpanda():
    logger.info(" [~] Testing Redpanda connection...")
    retries = 5
    for i in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers="localhost:19092")
            logger.info(" [OK] Redpanda is UP and reachable.")
            admin.close()
            return
        except NoBrokersAvailable:
            logger.warning(f" [!] Redpanda not ready... Retrying ({i+1}/{retries})")
            time.sleep(2)
    logger.error(" [x] Redpanda Failed.")

if __name__ == "__main__":
    check_postgres()
    check_redpanda()