import logging

from pyspark.sql.functions import col, greatest, least
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)




def feature_engineer(df: DataFrame) -> DataFrame:
    logger.info("="*75)
    logger.info("Engineering features....")
    df = df.withColumn("price_range", col("high") - col("low"))
    logger.info("Price range added")
    
    logger.info("="*75)
    logger.info("Calculating and adding body size....")
    from pyspark.sql.functions import abs as spark_abs
    df = df.withColumn("body_size", spark_abs(col("close") - col("open")))
    logger.info("Body size column added....")

    logger.info("="*75)
    logger.info("Calculating and adding upper shadow....")
    df = df.withColumn("upper_shadow", col("high") - greatest(col("open"), col("close")))
    logger.info("Upper shadow column added....")

    logger.info("="*75)
    logger.info("Calculating and adding lower shadow....")
    df = df.withColumn("lower_shadow", least(col("open"), col("close")) - col("low"))
    logger.info("Lower shadow column added....")

    logger.info("="*75)
    logger.info("Calculating and adding if bullish or not....")
    df = df.withColumn("is_bullish", col("close") > col("open"))
    logger.info("Is bullish column added ....")

    logger.info("Feature engineering complete.")

    return df