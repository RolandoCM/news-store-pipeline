
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime, timedelta
from typing import List

logger = logging.getLogger(__name__)   

class NewsProcessor:
    
    def __init__(self, config: dict):
        self.config = config

    def consume(self):
        topics = tuple(self.config['kafka']['topics'])   
        logger.info(f"Kafka topics: {type(topics[0])}")
        consumer = KafkaConsumer(
            topics[0],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            group_id=self.config['kafka']['group_id'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=1000

        )
        logger.info("Starting to consume messages")
        batch = []
        batch_start_time = datetime.now()
        
        logger.info(f"condumer info {consumer}")
        for message in consumer:
            try:
                article_data = message.value
                article = self._create_article(article_data)
                batch.append(article)

                current_time = datetime.now()
                if (len(batch) >= self.config['processing']['batch_size'] or
                    (current_time - batch_start_time).total_seconds() >= 30):
                    self._process_batch(batch)
                    batch = []
                    batch_start_time = current_time
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    def _create_article(self, data: dict):
        from com.itquetzali.news.models.NewsArticle import NewsArticle
        return NewsArticle(**data)
    
    def _process_batch(self, articles):
        if not articles:
            return
        
        logger.info(f"Processing batch of {len(articles)} articles")

        hot_articles = []

        # determine hot articles
        cutoff_date = datetime.now() - timedelta(days=self.config['storage']['retention']['hot_days_threshold'])

        for article in articles:
            if article.published_at >= cutoff_date:
                hot_articles.append(article)
    def close(self):
        logger.info("Closing NewsProcessor")
        # Add any necessary cleanup code here