import sys
import logging
from config.kafka_config import KafkaConfig

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, IntegerType, BooleanType
)
from pyspark.sql.functions import col, from_json, struct, to_json, when

from process.transformers import clean, validate, feature_engineer

logger = logging.getLogger(__name__)



def _create_spark_session(config: KafkaConfig) -> SparkSession:
    builder = SparkSession.builder.appName("MarketStreamApp")
    builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    builder = builder.config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/marketstream")
    #builder = builder.config("spark.sql.adaptive.enabled", "true")           
    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("="*75)
    logger.info("Spark version: %s",spark.version)
    logger.info("Spark Session created successfully!")
    logger.info("="*75)
    return spark


def _read_stream(spark: SparkSession, config: KafkaConfig):
    try:
        raw_stream = (spark.readStream
                            .format("kafka")
                            .option("kafka.bootstrap.servers",",".join(config.bootstrap_servers))
                            .option("subscribe",f"{config.RAW_STOCKS_TOPIC},{config.RAW_CRYPTO_TOPIC}")
                            .option("startingOffsets", "earliest")
                            .option("failOnDataLoss", "false")
                            .load()
                            )
        logger.info("Kafka stream created for topic: %s and %s", config.RAW_STOCKS_TOPIC, config.RAW_CRYPTO_TOPIC)
        return raw_stream
    
    except Exception as e:
        logger.error(f"Failed to read from kafka, {e}", exc_info=True)
        raise
    
    
    
    
def _parse_json(raw_stream: DataFrame)-> DataFrame: 
    try:
        OHLCV_SCHEMA = StructType([
            StructField("symbol", StringType(), True),
            StructField("market", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("trades", IntegerType(), True),
            StructField("vwap", DoubleType(), True),
            StructField("closed", BooleanType(), True),
            StructField("interval", StringType(), True)
                          
    ])
        parsed_data = raw_stream.withColumn("data", from_json(col("value").cast("string"), OHLCV_SCHEMA)) \
                                .select(
                                    col("topic"),
                                    col("timestamp").alias("kafka_timestamp"),
                                    col("data.*")
                                     
                                    ) \
                                .filter(col("symbol").isNotNull())
        logger.info("JSON parsing applied. Schema: %s", parsed_data.schema.simpleString())
        return parsed_data
           
    except Exception as e: 
        logger.error("Failed to parse Kafka JSON: %s", e, exc_info=True)
        raise     



def _transform(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Starting transformation pipeline...")
    df = clean(df)
    df = validate(df)
    df = feature_engineer(df)
    logger.info("Transformation pipeline complete.")
    return df

def _write_kafka(clean_stream: DataFrame, config: KafkaConfig):
    logger.info("="*75)
    logger.info("Writing clean stream dataframe rows to kafka....")
    
    kafka_df = (clean_stream.withColumn("topic",
                                        when(col("market")=="stock", config.CLEAN_STOCKS_TOPIC)
                                        .when(col("market")=="crypto", config.CLEAN_CRYPTO_TOPIC)
                                        .withColumn("value",
                                                    to_json(struct(*[col(c) for c in clean_stream.columns
                                                                     if c != "topic"])))
                                        .withColumn("key", col("symbol"))
                                        .select("topic", "key", "value")                             
                                        ))
    return (kafka_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers",
                    ",".join(config.boostrap_servers))
                    .option("checkpointLocation", "/tmp/checkpoints/write_kafka")
                    .start())

#def _write_s3() :

def main() -> None: 
   try:
        kafka_config = KafkaConfig()
        spark = _create_spark_session(kafka_config)

        logger.info("="*75)
        logger.info("Stream reading process initiated...")
        logger.info("="*75)

        raw_stream = _read_stream(spark, kafka_config) 
        parsed_stream = _parse_json(raw_stream)
        clean_stream = _transform(parsed_stream)


        query = (clean_stream
                    .writeStream
                    .format("console")
                    .option("truncate", False)
                    .start())
        
        query.awaitTermination()
   except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
   except Exception as e:
        logger.critical("Application failed", exc_info=True)  
        sys.exit(1)  
        








if __name__ == "__main__": 
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()