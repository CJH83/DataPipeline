import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import sys
import logging
import logging.config
import psycopg2
import configparser


class Persist:

    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df):
        try:
            config = configparser.ConfigParser()
            config.read('pipeline/resources/pipeline.ini')
            target_table = config.get('DB_CONFIGS', 'TARGET_PG_TABLE')
            logger = logging.getLogger('Persist')
            logger.info('Persisting....')
            #df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")
            df.show()
            df.write \
            .mode("overwrite") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", target_table) \
            .option("user", "postgres") \
            .option("password", "admin") \
            .save()

        except Exception as exp:
            logger.error(f"An error occured while persisting data >{exp}")
            raise Exception("HDFS directory already exists")

    def insert_into_pg(self):
        connection = psycopg2.connect(user='postgres', password='admin', host='localhost', database='postgres')
        cursor = connection.cursor()
        insert_query = """INSERT INTO futurexschema.futurex_course_catalog 
                        (course_id, course_name, author_name, course_section, creation_date) 
                        VALUES  (%s, %s, %s, %s, %s);"""
        insert_tuple = (3, 'Machine Learning', 'FutureX', '{}', '2022-10-20')
        cursor.execute(insert_query, insert_tuple)
        cursor.close()
        connection.commit()