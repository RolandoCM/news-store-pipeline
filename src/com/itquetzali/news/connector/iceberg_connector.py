from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from src.com.itquetzali.news.models.models import StorageMetrics
from datetime import datetime  
import logging

logger = logging.getLogger(__name__)

class IcebergConnector:
    def __init__(self, config: dict, schema:StructType):
        self.config = config
        self.schema = schema
        self.spark = self.__setup_iceberg_config()
        self._create_table_if_not_exists()

    def __setup_iceberg_config(self) -> SparkSession:
        """spark config for iceberg"""
        
        return SparkSession.builder \
            .appName("iceberg") \
            .master(self.config["spark"]["master"]) \
            .config("spark.executor.memory", self.config["spark"]["executor_memory"]) \
            .config("spark.driver.memory", self.config["spark"]["driver_memory"]) \
            .config("spark.sql.shuffle.partitions", self.config["spark"]["shuffle_partitions"]) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "rest") \
            .config("spark.sql.catalog.spark_catalog.uri", "http://localhost:8181") \
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://my-bucket/warehouse") \
            .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1," \
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.0") \
            .getOrCreate()
        

        

        # S3 configuration if using S3
       # if self.config['iceberg']['warehouse_path'].startswith('s3'):
       #     self.spark.conf.set("spark.sql.catalog.news_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
       #     self.spark.conf.set("spark.hadoop.fs.s3a.endpoint", self.config['s3']['endpoint'])
       #     self.spark.conf.set("spark.hadoop.fs.s3a.access.key", self.config['s3']['access_key'])
       #     self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", self.config['s3']['secret_key'])
       #     self.spark.conf.set("spark.hadoop.fs.s3a.path.style.access", str(self.config['s3']['path_style_access']).lower())

    def _create_table_if_not_exists(self):
        logger.info("Checking if Iceberg table exists or needs to be created.")
        try:
            df = self.spark.createDataFrame([], self.schema)
            df.show()
            df.writeTo("spark_catalog.news_catalog.news_aticles").create()
            #self.spark.sql("USE spark_catalog.news_catalog")
            #list = self.spark.catalog.listTables("news_catalog")
            #logger.info(f"Tables in news_catalog: {list}")
            # check if table exists
            #tables = self.spark.sql("SHOW TABLES IN news_catalog")
            #table_exists = tables.filter(col("tableName") == self.config["iceberg"]["table_name"]).count() > 0
            
            """if not table_exists:
                create_table_query = f
                CREATE TABLE news_catalog.{self.config['iceberg']['table_name']} (
                    title STRING,
                    content STRING,
                    sumary STRING,
                    source STRING,
                    author STRING,
                    url STRING,
                    published_at TIMESTAMP,
                    category STRING,
                    language STRING,
                    tags ARRAY<STRING>
                ) USING iceberg
                PARTITIONED BY (days(published_at), category)
                TBLPROPERTIES (
                    'format-version'='2',
                    'write.compression-codec'='${self.config['iceberg']['compression']}',
                    'write,distribution-mode'='${self.config['iceberg']['write_distribution_mode']}'
                )
                
                self.spark.sql(create_table_query)
                logger.info(f"Table {self.config['iceberg']['table_name']} created successfully.")
            else:
                logger.info(f"Table {self.config['iceberg']['table_name']} already exists.") """
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    def store_dataframe(self, df):
        metrics = StorageMetrics()
        start_date = datetime.now()

        try:
            df.write \
                .format("iceberg") \
                .mode("append") \
                .save(f"news_catalog.{self.config['iceberg']['table_name']}")
            
            metrics.iceberg_count = df.count()
            logger.info(f"Data appended to table {self.config['iceberg']['table_name']} successfully.")
        except Exception as e:
            logger.error(f"Error writing to table: {e}")
        
        metrics.total_articles = metrics.iceberg_count
        metrics.processing_time_ms = (datetime.now() - start_date).total_seconds() * 1000
        metrics.last_processed = datetime.now()
        return metrics
    
