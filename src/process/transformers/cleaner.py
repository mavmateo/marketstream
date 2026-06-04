import logging

from pyspark.sql import col, greatest
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

def clean(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Dropping rows with null symbol values.....")
    symbol_nullcount = df.filter(col("symbol").isNull()).count()
    clean_df = df.dropna(subset=['symbol'], how='any')
    logger.info({symbol_nullcount},"rows with null symbol values were dropped...")

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


def validate(df:DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Checking high values to ensure they follow the schema standards....")
    high_count = df.filter((col("high") < col("open")) &
                           (col("high") < col("close")) &
                           (col("high") < col("low"))
                          )
    logger.warning({high_count}, "ireggular high values found")
    
    logger.info("="*75)
    logger.info("Checking low values to ensure they follow the schema standards....")
    low_count = df.filter((col("low") > col("open")) &
                           (col("low") > col("close")) &
                           (col("low") > col("high"))
                        )
    logger.warning({low_count}, "ireggular low values found")

    logger.info("="*75)
    logger.info("Checking price values to ensure they follow the schema standards....")
    price_count = df.filter(col("price") != col("close"))
    logger.warning({price_count}, "price values do not match close values")

    logger.info("="*75)
    logger.info("Checking volume values to ensure they follow the schema standards....")
    volume_count = df.filter(col("volume") <= 0 )
    logger.warning({volume_count},"volume values are 0 or negative...." )