import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

class Ingest:
    logging.config.fileConfig('pipeline/resources/configs/logging.conf')

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')
        #customer_df = self.spark.read.csv("retailstore.csv", header=True)
        course_df = self.spark.sql("SELECT * FROM fxcoursedb.fx_course_table")
        logger.info('Dataframe created')
        return course_df

    def read_from_pg(self):
        connection = psycopg2.connect(user='postgres', password='admin', host='localhost', database='postgres')
        cursor = connection.cursor()
        sql_query = 'SELECT * FROM futurexschema.futurex_course_catalog'
        pd_df = sqlio.read_sql_query(sql_query, connection)
        spark_df = self.spark.createDataFrame(pd_df)
        spark_df.show()

    def read_from_pg_using_jdbc_driver(self):
        jdbc_df = self.spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "futurexschema.futurex_course_catalog") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .load()
        jdbc_df.show()
