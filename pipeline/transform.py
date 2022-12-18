import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config

class Transform:

    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logger = logging.getLogger('Transform')
        logger.info('Transforming....')

        df1 = df.na.fill("Unknown", ["author_name"])
        df2 = df1.na.fill("0", ["no_of_reviews"])
        return df2