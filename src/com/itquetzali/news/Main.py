
import logging
import yaml
from connector.NewsProcessor import NewsProcessor

def setup_logging():

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler('news_store_pipeline.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    with open('config/config.yml', 'r') as file:
        return yaml.safe_load(file)
    
def main():
    setup_logging()

    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        logger.info(f"Configuration loaded successfully: {config}")
        logger.info("Configuration loaded successfully")
        
        processor = NewsProcessor(config)
    
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