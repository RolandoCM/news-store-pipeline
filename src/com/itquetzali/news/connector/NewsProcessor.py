
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime, timedelta
from typing import List
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from connector.iceberg_connector import IcebergConnector

logger = logging.getLogger(__name__)   

class NewsProcessor:
    
    def __init__(self, config: dict):
        self.config = config
        self.spark = self._create_spark_session()

        self.iceberg_connector = IcebergConnector(self.spark, config)

        self.schema = StructType([
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("sumary", StringType(), True),
            StructField("source", StringType(), True),
            StructField("author", StringType(), True),
            StructField("url", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("language", StringType(), True),
            StructField("tags", ArrayType(StringType()), True)
        ])
    def _create_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master']) \
            .config("spark.executor.memory", self.config['spark']['executor_memory']) \
            .config("spark.driver.memory", self.config['spark']['driver_memory']) \
            .config("spark.sql.shuffle.partitions", self.config['spark']['sql_shuffle_partitions']) \
            .config("spark.cassandra.connection.host", self.config['cassandra']['host']) \
            .config("spark.cassandra.connection.port", self.config['cassandra']['port']) \
            .config("spark.sql.adapative.enabled", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
            .config("spark.sql.catalog.news_catalog", self.config["iceberg"]["catalog_impl"]) \
            .config("spark.sql.catalog.news_catalog.werehouse", self.config["iceberg"]["warehouse_path"]) \
            .getOrCreate()     
            #.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
             




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