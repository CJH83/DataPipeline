import sys
from pyspark.sql import SparkSession
from pipeline import ingest, persist, transform
import logging
import logging.config

class Pipeline:
    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self):
        self.spark = SparkSession.builder.appName("first spark app") \
            .config("spark.driver.extraClassPath", "pipeline/postgresql-42.5.1.jar") \
            .enableHiveSupport() \
            .getOrCreate()

        logging.info('Spark Session created')


    def create_hive_table(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS fxcoursedb")
        self.spark.sql("""CREATE TABLE IF NOT EXISTS fxcoursedb.fx_course_table
        (course_id STRING, course_name STRING, author_name STRING, no_of_reviews STRING)""")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (1, 'Java', 'FutureX', 45)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (2, 'Java', 'FutureXSkill', 56)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (3, 'Big Data', 'Future', 100)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (4, 'Java', 'FutureX', 45)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (5, 'Microservices', 'Future', 100)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (6, 'CMS', '', 100)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (7, 'Python', 'FutureX', '')")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (8, 'CMS', 'Future', 56)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (9, 'Dot Net', 'FutureXSkill', 34)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (10, 'Ansible', 'FutureX', 123)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (11, 'Jenkins', 'Future', 32)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (12, 'Chef', 'FutureX', 121)")
        self.spark.sql("INSERT INTO fxcoursedb.fx_course_table VALUES (13, 'Go Lang', '', 105)")
        #Treat empty strings as null
        self.spark.sql("ALTER TABLE fxcoursedb.fx_course_table SET tblproperties('serialization.null.format'='')")

    def run_pipeline(self):

        try:
            logging.info('run_pipeline method started')
            ingest_process = ingest.Ingest(self.spark)
            #ingest_process.read_from_pg()

            ingest_process.read_from_pg_using_jdbc_driver()

            df = ingest_process.ingest_data()
            df.show()

            transform_process = transform.Transform(self.spark)
            transformed_df = transform_process.transform_data(df)
            transformed_df.show()

            persist_process = persist.Persist(self.spark)
            #persist_process.insert_into_pg()
            persist_process.persist_data(transformed_df)
            logging.info('run_pipeline method ended')
        except Exception as exp:
            logging.error(f"An error occured while running the pipeline > {exp}")
            sys.exit(1)
        return


if __name__ == '__main__':

        logging.info('Application started')

        pipeline = Pipeline()

        #pipeline.create_hive_table()
        pipeline.run_pipeline()
        logging.info('Pipeline executed')


