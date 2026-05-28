import json
import logging
from config.kafka_config import KafkaConfig





logger = logging.getLogger(__name__)
from pyspark.sql import SparkSession

def _create_spark_session(config: KafkaConfig) -> SparkSession:
    builder = SparkSession.builder.appName("MarketStreamApp")
    builder = builder.config("spark.jars.packages, org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")


               
    spark = builder.getOrCreate()


    print("="*60)
    print(f"Spark version: {spark.version}")
    print("="*60)

    return spark


def _read_stream(spark: SparkSession, config: KafkaConfig) -> None:
    rawstocks_stream = (spark.readStream
                        .format("kafka")
                        .option("kafka_bootstrap_servers",config.bootstrap_servers)
                        .option("subscribe", config.RAW_STOCKS_TOPIC)
                        .option("startingOffsets", "latest")
                        .option("failOnDataLoss", "false")
                        .load()
                        )
    return rawstocks_stream
    

    
#def _parse_json(): 

#def _transform() :

#def _write_kafka():


#def _write_s3() :

def main() -> None: 
   spark =  _create_spark_session(KafkaConfig)
   raw_stream = _read_stream(spark, KafkaConfig) 
   print("Streaming schema:")
   raw_stream.printSchema()






#clean_data = raw_stream.select(...).filter(...).withColumn(...) 





if __name__ == "__main__": 
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    main()