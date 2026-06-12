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