import sys
import json
import logging
from config.kafka_config import KafkaConfig





logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession


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
        rawstocks_stream = (spark.readStream
                            .format("kafka")
                            .option("kafka.bootstrap.servers",",".join(config.bootstrap_servers))
                            .option("subscribe",f"{config.RAW_STOCKS_TOPIC},{config.RAW_CRYPTO_TOPIC}")
                            .option("startingOffsets", "latest")
                            .option("failOnDataLoss", "false")
                            .load()
                            )
        logger.info("Kafka stream created for topic: %s and %s", config.RAW_STOCKS_TOPIC, config.RAW_CRYPTO_TOPIC)
        return rawstocks_stream
    
    except Exception as e:
        logger.error(f"Failed to read from kafka, {e}", exc_info=True)
        raise
    
    
    

    
#def _parse_json(): 

#def _transform() :

#def _write_kafka():


#def _write_s3() :

def main() -> None: 
   try:
        kafka_config = KafkaConfig()
        spark = _create_spark_session(kafka_config)

        logger.info("="*75)
        logger.info("Stream reading process initiated...")
        logger.info("="*75)

        rawstocks_stream = _read_stream(spark, kafka_config) 


        query = (rawstocks_stream
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
        
   #raw_stream.printSchema()






#clean_data = raw_stream.select(...).filter(...).withColumn(...) 





if __name__ == "__main__": 
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()