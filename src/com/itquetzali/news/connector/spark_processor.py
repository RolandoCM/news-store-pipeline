
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime, timedelta
from pyspark.sql.streaming import DataStreamReader, StreamingQuery

logger = logging.getLogger(__name__)
class SparkNewsProcessor:
    def __init__(self, config:dict):
        self.config = config
        self.spark = self._create_spark_session()
        #self.casandra_client = self._create_casandra_client(config)
        #self.iceberg_client = self._create_iceberg_client(self.spark, config)
        
        ## create enricher

        self.schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("author", StringType(), True),
            StructField("url", StringType(), True),
            StructField("published", StringType(), True),
            StructField("category", StringType(), True),
            StructField("language", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("processing_timestamp", StringType(), True)
        ])
    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session."""
        return SparkSession.builder \
            .appName(self.config["spark"]["app_name"]) \
            .master(self.config["spark"]["master"]) \
            .config("spark.executor.memory", self.config["spark"]["executor_memory"]) \
            .config("spark.driver.memory", self.config["spark"]["driver_memory"]) \
            .config("spark.sql.shuffle.partitions", self.config["spark"]["shuffle_partitions"]) \
            .config("spark.sql.adapive.enabled", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .getOrCreate()
    def process_kafka_stream(self):
        """Proess data from Kafka stream."""
        #scc = StreamingContext(self.spark.sparkContext, self.config["spark"]["batch_interval"])

        try:
            rdd = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config["kafka"]["bootstrap_servers"]) \
                .option("subscribe", self.config["kafka"]["topics"]) \
                .load()
            
            logger.info(f"Kafka stream created successfully.")
            # Cast 'key' and 'value' columns to STRING
            df_s = rdd.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .withColumn("data", from_json(col("value"), self.schema)) \
                .select("key","data.*")
            # Now, df_string has 'key' and 'value' as StringType columns
            df = df_s \
                .writeStream \
                .foreachBatch(self.__process_rdd) \
                .queryName("news_articles") \
                .option("truncate", "false") \
                .start()
            df_s.printSchema()
            df.awaitTermination()
        except Exception as e:
            logger.error(f"Error creating Kafka stream: {e}")
            raise
        finally:
          df.stop()
            #scc.stop(stopSparkContext=True, stopGraceFully=True)
    def __process_rdd(self, df, batch_id):
        """Process each RDD from the Kafka stream."""
        logger.info(f"Processing new RDD with batch ID: {batch_id}")
        try:

            if df.isEmpty():
                logger.info("Received empty RDD, skipping processing.")
                return
            df.printSchema()
            df.withColumn("processing_timestamp", current_timestamp())

            if self.config["enrichment"]["enabled"]:
                # Enrich data
                pass
            
            # Determine storage based on time
            cutoff_time = datetime.now() - timedelta(hours=self.config["retention"]["hot_data_days"])
            cutoff_timestamp = lit(cutoff_time.isoformat())
            
            hot_data = df.filter(col("published") >= cutoff_timestamp)
            analythics_data = df
            ## call storage methods 
            logger.info(f"processing batch with {analythics_data.count()} total records and {hot_data.count()} hot records")
            
        except Exception as e:
            logger.error(f"Error processing RDD: {e}")
            raise
            

    def close(self):
        pass

    def compat_tables(self):
        pass