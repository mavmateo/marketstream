import logging

from pyspark.sql import col
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def clean(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Dropping rows with null symbol values.....")
    clean_df = df.dropna(subset=['symbol'], how='any')
    logger.info("Null symbol values dropped..")

    logger.info("="*75)
    logger.info("Dropping rows with null open, high, low and close values....")
    clean_df = df.dropna(subset=['open', 'high', 'low', 'close'], how='any')
    logger.info("Null values for open, high, low, close dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with 0 or negative price values....")
    clean_df = df.filter(col("price") > 0)
    logger.info("0 and negative price values dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with negative volumes values....")
    clean_df = df.filter(col("volume") >= 0)
    logger.info("Negative volume values dropped....")

    logger.info("="*75)
    logger.info("Dropping rows where high < low")
    clean_df = df.filter(col("high") >= col("low"))
    logger.info("High values lower than low dropped....")

    return clean_df

