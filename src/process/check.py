#check these code and see if there might be a bug causing no stocks to be written in timescalde db. if not then we can cancel out code bugs as the issue and drill down our debugging

#spark_stream.py
import sys
import logging
from config.kafka_config import KafkaConfig

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, IntegerType, BooleanType)
from pyspark.sql.functions import col, from_json, struct, to_json, to_timestamp, when , year, month, dayofmonth
from pyspark.sql.streaming import StreamingQuery

from process.transformers import clean, validate, feature_engineer

logger = logging.getLogger(__name__)



def _create_spark_session(config: KafkaConfig) -> SparkSession:
    builder = SparkSession.builder.appName("MarketStreamApp")
    builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    builder = builder.config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/marketstream")
    builder = builder.config("spark.jars","infra/jars/postgresql-42.7.3.jar")        
    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
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
        logger.info("[READ]Kafka stream created for topic: %s and %s", config.RAW_STOCKS_TOPIC, config.RAW_CRYPTO_TOPIC)
        return raw_stream
    
    except Exception as e:
        logger.error(f"[READ]Failed to read from kafka, {e}", exc_info=True)
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
        logger.info("[PARSE]JSON parsing applied. Schema: %s", parsed_data.schema.simpleString())
        return parsed_data
           
    except Exception as e: 
        logger.error("[PARSE]Failed to parse Kafka JSON: %s", e, exc_info=True)
        raise     



def _transform(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Starting transformation pipeline...")
    cleaned_df = clean(df)
    (cleaned_df.groupBy("market").count()
               .writeStream.format("console").outputMode("complete")
               .queryName("after_clean").start())
    
    validated_df = validate(cleaned_df)
    (validated_df.groupBy("market").count()
                 .writeStream.format("console").outputMode("complete")
                 .queryName("after_validate").start())

    clean_df = feature_engineer(validated_df)
    logger.info("Transformation pipeline complete.")
    return clean_df

def _write_kafka(clean_stream: DataFrame, config: KafkaConfig) -> StreamingQuery:
    try:
        logger.info("="*75)
        logger.info("[WRITE]Writing clean stream to kafka....")
        
        kafka_df = (clean_stream
                        .withColumn("topic",
                           when(col("market")=="stock", config.CLEAN_STOCKS_TOPIC)
                           .when(col("market")=="crypto", config.CLEAN_CRYPTO_TOPIC)
                        )
                           .withColumn("value",
                                    to_json(struct(*[col(c) for c in clean_stream.columns
                                                    if c != "topic"]))
                        )
                        .withColumn("key", col("symbol"))
                        .select("topic", "key", "value")                             
                    )
        return (kafka_df.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", ",".join(config.bootstrap_servers))
                        .option("checkpointLocation", "/tmp/checkpoints/write_kafka")
                        .start())
    
    except Exception as e:
        logger.error("[WRITE]Failed to write clean stream to Kafka: %s", e, exc_info=True)
        raise


def _write_timescale(clean_stream: DataFrame) -> StreamingQuery:
    logger.info("="*75)
    logger.info("[WRITE]Writing clean stream to timescale-db....")
    return (clean_stream.writeStream
            .foreachBatch(_write_batch_to_timescale)
            .option("checkpointLocation", 
                    "/tmp/checkpoints/write_timescale")
            .start())

def _write_batch_to_timescale(batch_df, batch_id):
    if batch_df.isEmpty():
         return

    ts_df = (batch_df.withColumn("timestamp", 
                                to_timestamp(col("timestamp")))
                    .withColumnRenamed("timestamp", "time") \
                    .select(
                          "time", "symbol", "market",
                        "open", "high", "low", "close",
                        "price", "volume", "trades", "vwap",
                        "price_range", "body_size",
                        "upper_shadow", "lower_shadow",
                        "is_bullish", "interval"
                     ))


    (ts_df.write
        .format("jdbc")
        .option("url",
                "jdbc:postgresql://localhost:5432/marketstream")
        .option("dbtable", "ohlcv_ticks")
        .option("user" , "postgres") 
        .option("password" ,"postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())
    
    logger.info("[WRITE]Batch %d written to TimescaleDB - %d rows", batch_id, ts_df.count())



# def _write_s3(clean_stream: DataFrame) -> StreamingQuery:
#     try:
#         logger.info("="*75)
#         logger.info("Writing clean stream to AWS S3 ....")

#         s3_df = (clean_stream
#                             .withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
#                             .withColumn("year", year(col("timestamp_parsed")))
#                             .withColumn("month", month(col("timestamp_parsed")))
#                             .withColumn("day", dayofmonth(col("timestamp_parsed")))
#                             .drop("timestamp_parsed")             
                                        
#                 )
        
#         return (s3_df.writeStream
#                 .format("parquet")
#                 .option("path", "s3a://bucket/market-data/")
#                 .option("checkpointLocation", "/tmp/checkpoints/write_s3")
#                 .partitionBy("market", "year", "month", "day")
#                 .start())
    
#     except Exception as e:
#         logger.error("Failed to write clean stream to AWS S3: %s", e, exc_info=True)
#         raise




def main() -> None: 
   try:
        kafka_config = KafkaConfig()
        spark = _create_spark_session(kafka_config)

        logger.info("="*75)
        logger.info("Stream reading process initiated...")
        logger.info("="*75)

        raw_stream = _read_stream(spark, kafka_config) 
        parsed_stream = _parse_json(raw_stream)
        (parsed_stream.groupBy("market").count()
                      .writeStream.format("console").outputMode("complete")
                      .queryName("after_parse").start())
        
        clean_stream = _transform(parsed_stream)

        # debug_query = (parsed_stream.groupBy("market")
        #                               .count()
        #                               .writeStream
        #                               .format("console")
        #                               .outputMode("complete")
        #                               .start())

        kafka_query = _write_kafka(clean_stream, kafka_config)
        timescale_query = _write_timescale(clean_stream)
        logger.info("All streams started. Awaiting termination....")

        #debug_query.awaitTermination()
        kafka_query.awaitTermination()
        timescale_query.awaitTermination()


        spark.streams.awaitAnyTermination

   except KeyboardInterrupt:
        logger.info("Streaming stopped by user.")
   except Exception as e:
        logger.critical("Application failed", exc_info=True)  
        sys.exit(1)  
        








if __name__ == "__main__": 
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()

    #cleaner.py
    import logging

from pyspark.sql.functions import col
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def clean(df: DataFrame) -> DataFrame:
    
    logger.info("="*75)
    logger.info("Dropping rows with null open, high, low and close rows....")
    clean_df = df.dropna(subset=[ 'symbol', 'open', 'high', 'low', 'close'], how='any')
    logger.info("[CLEAN]Null rows for symbol,open, high, low, close dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with 0 or negative price rows....")
    clean_df = clean_df.filter(col("price") > 0)
    logger.info("[CLEAN]0 and negative price rows dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with negative volumes rows....")
    clean_df = clean_df.filter(col("volume") >= 0)
    logger.info("[CLEAN]Negative volume rows dropped....")

    logger.info("="*75)
    logger.info("Dropping rows where high < low")
    clean_df = clean_df.filter(col("high") >= col("low"))
    logger.info("[CLEAN]High rows lower than low dropped....")

    logger.info("Cleaning complete.")

    return clean_df



  # validator.py
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import abs as spark_abs

logger = logging.getLogger(__name__)

TOLERANCE = 0.0001

def validate(df: DataFrame) -> DataFrame:

    logger.info("Validating: high must be highest value...")
    df = df.filter(
        (col("high") >= col("open")  - TOLERANCE) &
        (col("high") >= col("close") - TOLERANCE) &
        (col("high") >= col("low")   - TOLERANCE)
    )

    logger.info("Validating: low must be lowest value...")
    df = df.filter(
        (col("low") <= col("open")  + TOLERANCE) &
        (col("low") <= col("close") + TOLERANCE) &
        (col("low") <= col("high")  + TOLERANCE)
    )

    logger.info("Validating: price must match close...")
    df = df.filter(
        spark_abs(col("price") - col("close")) < TOLERANCE
    )

    logger.info("Validating: volume must be positive...")
    df = df.filter(col("volume") > 0)

    logger.info("Validation complete.")
    return df

# feature_engineer.py
import logging

from pyspark.sql.functions import col, greatest, least
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)




def feature_engineer(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Engineering features....")
    df = df.withColumn("price_range", col("high") - col("low"))
    logger.info("[TRANSFORM]Price range added")
    
    logger.info("="*75)
    logger.info("Calculating and adding body size....")
    from pyspark.sql.functions import abs as spark_abs
    df = df.withColumn("body_size", spark_abs(col("close") - col("open")))
    logger.info("[TRANSFORM]Body size column added....")

    logger.info("="*75)
    logger.info("Calculating and adding upper shadow....")
    df = df.withColumn("upper_shadow", col("high") - greatest(col("open"), col("close")))
    logger.info("[TRANSFORM]Upper shadow column added....")

    logger.info("="*75)
    logger.info("Calculating and adding lower shadow....")
    df = df.withColumn("lower_shadow", least(col("open"), col("close")) - col("low"))
    logger.info("[TRANSFORM]Lower shadow column added....")

    logger.info("="*75)
    logger.info("Calculating and adding if bullish or not....")
    df = df.withColumn("is_bullish", col("close") > col("open"))
    logger.info("[TRANSFORM]Is bullish column added ....")

    logger.info("Feature engineering complete.")

    return df