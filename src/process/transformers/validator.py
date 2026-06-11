import logging

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs

logger = logging.getLogger(__name__)


def validate(df:DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Checking high rows to ensure they follow the schema standards....")
    df = df.filter((col("high") >= col("open") - 0.0001) &
                        (col("high") >= col("close") - 0.0001) &
                        (col("high") >= col("low")))                    
    logger.info("[VALIDATE]High rows schema validated....")
    
    logger.info("="*75)
    logger.info("Checking low rows to ensure they follow the schema standards....")
    df = df.filter((col("low") <= col("open") - 0.0001) & 
                              (col("low") <= col("close") - 0.0001) &
                              (col("low") <= col("high")) )                  
    logger.info("[VALIDATE]Low rows schema found....")

    logger.info("="*75)
    logger.info("Checking price rows to ensure they follow the schema standards....")
    df = df.filter(spark_abs(col("price") - col("close")) < 0.0001)
    logger.info("[VALIDATE]Price rows schema validated....")

    logger.info("="*75)
    logger.info("Checking volume rows to ensure they follow the schema standards....")
    df = df.filter(col("volume") > 0 )
    logger.info("[VALIDATE]Volume rows schema validated...." )

    logger.info("Validation complete.")

    return df