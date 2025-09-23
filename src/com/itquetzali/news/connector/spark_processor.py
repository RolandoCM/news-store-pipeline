
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import logging

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
            StructField("content", StringType(), True),
            StructField("author", StringType(), True),
            StructField("published_at", StringType(), True),
            StructField("source", StringType(), True),
            StructField("url", StringType(), True),
            StructField("category", StringType(), True),
            StructField("language", StringType(), True),
            StructField("country", StringType(), True)
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
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config["kafka"]["bootstrap_servers"]) \
                .option("subscribe", self.config["kafka"]["topics"]) \
                .load()
            # Cast 'key' and 'value' columns to STRING
            df_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            # Now, df_string has 'key' and 'value' as StringType columns
            df_string.printSchema()
            query = df_string \
                .writeStream \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 10) \
                .start()
            query.awaitTermination()
            logger.info(f"Kafka stream created successfully.")
            #self.sleep(100)
            """KafkaUtils.createDirectStream(
                scc,
                [self.config["kafka"]["topic"]],
                { "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
                  "group.id": self.config["kafka"]["group_id"],
                  "auto.offset.reset": self.config["kafka"]["starting_offset"]})
            kafka_stream.foreachRDD(self.__process_rdd)
            logger.info("Starting Kafka stream processing...")
            scc.start()
            scc.awaitTermination()"""
        except Exception as e:
            logger.error(f"Error creating Kafka stream: {e}")
            raise
        #finally:
            #scc.stop(stopSparkContext=True, stopGraceFully=True)
    def close(self):
        pass

    def compat_tables(self):
        pass