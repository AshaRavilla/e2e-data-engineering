import logging
import datetime from datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.fucntions import from_json, col

def create_keyspace(session):
    #create keyspace here

def create_table(session):
    #create table here

def insert_date(session, **kwargs):
    #insertion here

def create_spark_connection():
    try:
        s_conn = SparkSession.builder\
            .appName('SparkDataStreaming')\
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                       "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
    #creating spark connection
    except Exception as e:
        logging.error(f"couldn't create the spark session due to exception {e}")

    return s_conn

def create_cassandra_connection():
    session = None

    #Connecting to the cassandra cluster
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")

    return session



if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn 