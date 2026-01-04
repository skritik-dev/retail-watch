import asyncio
import json
import time
from playwright.async_api import async_playwright
from kafka import KafkaProducer
from kafka.errors import KafkaError
from utils.logger import get_logger

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092' 
TOPIC_NAME = 'raw_scrapes'
TARGET_URL = 'http://books.toscrape.com/'
logger = get_logger("Producer")

class ScrapeProducer:
    def __init__(self):
        # Initialize Kafka Producer
        # value_serializer ensures we send JSON bytes, not strings
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    async def scrape_catalog(self):
        """
        Launches browser, scrapes data, and sends to Redpanda.
        """
        async with async_playwright() as p:
            # Launch browser (headless=True, good for production)
            browser = await p.chromium.launch(headless=True)
            # Creating a real User Agent for our scraper
            #? When a browser hits a website, it sends metadata along with requests. One of those is the User Agent.
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...'
            )
            page = await context.new_page()
            
            logger.info(f" [~] Navigating to {TARGET_URL}...")
            await page.goto(TARGET_URL)
            
            # Select all product articles
            # In a real site, selectors like '.product_pod' break often.
            products = await page.query_selector_all('.product_pod')
            
            logger.info(f" [OK] Found {len(products)} products. Streaming to Redpanda...")

            for product in products:
                # Extract Data (Raw Extraction)
                # We try/except inside the loop so one bad item doesn't kill the batch (fault tolerance)
                try:
                    title_el = await product.query_selector('h3 a')
                    price_el = await product.query_selector('.price_color')
                    stock_el = await product.query_selector('.instock.availability')
                    
                    data = {
                        "product_name": await title_el.get_attribute('title'),
                        "price_raw": await price_el.inner_text(), # e.g. £51.77
                        "availability_raw": await stock_el.inner_text(),
                        "url": TARGET_URL + await title_el.get_attribute('href'),
                        "scraped_at": time.time(),
                        "source": "books_toscrape"
                    }

                    # Send data to Redpanda
                    self.send_to_stream(data)
                    
                except Exception as e:
                    logger.error(f" [x] Error parsing item: {e}")

            await browser.close()
            logger.info(" [OK] Batch complete.")

    # # Asynchronous version of send_to_stream for high throughput
    # def send_to_stream(self, data):
    #     """
    #     Publishes message to Redpanda asynchronously for high throughput.
    #     """
    #     self.producer.send(TOPIC_NAME, data).add_callback(
    #         lambda _md: logger.info(f" [OK] Sent: {data['product_name'][:20]}...")
    #     ).add_errback(
    #         lambda e: logger.error(f" [!] Failed to send to Redpanda: {e}")
    #     )

    def send_to_stream(self, data):
        """
        Publishes message to Redpanda.
        Future ensures we catch network errors immediately.
        """
        try:
            future = self.producer.send(TOPIC_NAME, data)
            future.get(timeout=10) # Wait for confirmation  
            # In high-throughput, you wouldn't wait (.get) on every message
            logger.info(f" [OK] Sent: {data['product_name'][:20]}...")
        except KafkaError as e:
            logger.error(f" [!] Failed to send to Redpanda: {e}")

if __name__ == "__main__":
    scraper = ScrapeProducer()
    asyncio.run(scraper.scrape_catalog())