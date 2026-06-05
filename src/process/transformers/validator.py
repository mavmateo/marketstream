import logging

from pyspark.sql.functions import col
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def validate(df:DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Checking high rows to ensure they follow the schema standards....")
    df = df.filter((col("high") >= col("open")) &
                        (col("high") >= col("close")) &
                        (col("high") >= col("low")))                    
    logger.info("High rows schema validated....")
    
    logger.info("="*75)
    logger.info("Checking low rows to ensure they follow the schema standards....")
    df = df.filter((col("low") <= col("open")) & 
                              (col("low") <= col("close")) &
                              (col("low") <= col("high")) )                  
    logger.info("Low rows schema found....")

    logger.info("="*75)
    logger.info("Checking price rows to ensure they follow the schema standards....")
    df = df.filter(col("price") == col("close"))
    logger.info("Price rows schema validated....")

    logger.info("="*75)
    logger.info("Checking volume rows to ensure they follow the schema standards....")
    df = df.filter(col("volume") > 0 )
    logger.info("Volume rows schema validated...." )

    logger.info("Validation complete.")

    return df