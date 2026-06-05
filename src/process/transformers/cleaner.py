import logging

from pyspark.sql.functions import col
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def clean(df: DataFrame) -> DataFrame:
    
    logger.info("="*75)
    logger.info("Dropping rows with null open, high, low and close rows....")
    clean_df = df.dropna(subset=[ 'symbol', 'open', 'high', 'low', 'close'], how='any')
    logger.info("Null rows for symbol,open, high, low, close dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with 0 or negative price rows....")
    clean_df = clean_df.filter(col("price") > 0)
    logger.info("0 and negative price rows dropped....")

    logger.info("="*75)
    logger.info("Dropping rows with negative volumes rows....")
    clean_df = clean_df.filter(col("volume") >= 0)
    logger.info("Negative volume rows dropped....")

    logger.info("="*75)
    logger.info("Dropping rows where high < low")
    clean_df = clean_df.filter(col("high") >= col("low"))
    logger.info("High rows lower than low dropped....")

    logger.info("Cleaning complete.")

    return clean_df









     