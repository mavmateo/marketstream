import logging

from pyspark.sql import col, greatest, abs, least
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def validate(df:DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Checking high rows to ensure they follow the schema standards....")
    valid_df = df.filter((col("high") < col("open")))
    valid_df = valid_df.filter(col("high") < col("close")) 
    valid_df = valid_df.filter(col("high") < col("low"))                     
    logger.info("High rows schema validated....")
    
    logger.info("="*75)
    logger.info("Checking low rows to ensure they follow the schema standards....")
    valid_df = valid_df.filter((col("low") > col("open"))) 
    valid_df = valid_df.filter(col("low") > col("close")) 
    valid_df = valid_df.filter(col("low") > col("high"))                   
    logger.info("Low rows schema found....")

    logger.info("="*75)
    logger.info("Checking price rows to ensure they follow the schema standards....")
    valid_df = valid_df.filter(col("price") != col("close"))
    logger.info("Price rows schema validated....")

    logger.info("="*75)
    logger.info("Checking volume rows to ensure they follow the schema standards....")
    valid_df = valid_df.filter(col("volume") <= 0 )
    logger.info("volume rows schema validated...." )

    logger.info("Validation complete.")

    return valid_df