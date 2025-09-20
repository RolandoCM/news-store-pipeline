
import logging
import yaml
from connector.NewsProcessor import NewsProcessor
from connector.spark_processor import SparkNewsProcessor
from threading import Thread

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler('news_store_pipeline.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    """Load configuration from a YAML file."""
    with open('config/config.yml', 'r') as file:
        return yaml.safe_load(file)

def mantenance_worker(processor: NewsProcessor, interval_hours: int = 24):
    """Perform periodic maintenance tasks."""
    while True:
        try:
            ## compact tables in iceberg
            processor.iceberg_connector
        except Exception as e:
            logging.error(f"Mantenance worker error: {e}")
def main():
    """Backgound service to consume news articles from Kafka and store them in Iceberg."""
    setup_logging()

    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        logger.info(f"Configuration loaded successfully: {config}")
        
        processor = SparkNewsProcessor(config)
        logger.info("SparkNewsProcessor initialized successfully")

        mantenance_thread = Thread( 
            target=mantenance_worker, 
            args=(processor, config['maintenance_interval_hours']), 
            daemon=True 
        )
    
        #processor.consume()
    except Exception as e:
        logger.error(f"Error in main execution: {e}")  
    finally:
        if 'processor' in locals():
            processor.close()
            logger.info("Processor closed")

if __name__ == "__main__" and __package__ is None:
    __package__ = "com.itquetzali.news"
    main()