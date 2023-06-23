import os
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col

# Postgres and Redshift JDBCs
driver_path = "/home/coder/working_dir/driver_jdbc/postgresql-42.2.27.jre7.jar"

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-class-path {driver_path} --jars {driver_path} pyspark-shell'
os.environ['SPARK_CLASSPATH'] = driver_path

# Create SparkSession 
spark = SparkSession.builder \
        .master("local") \
        .appName("Clean and Load data to Redshift") \
        .config("spark.jars", driver_path) \
        .config("spark.executor.extraClassPath", driver_path) \
        .getOrCreate()

# Connect to Redshift using psycopg2
conn = psycopg2.connect(
    host=env['AWS_REDSHIFT_HOST'],
    port=env['AWS_REDSHIFT_PORT'],
    dbname=env['AWS_REDSHIFT_DBNAME'],
    user=env['AWS_REDSHIFT_USER'],
    password=env['AWS_REDSHIFT_PASSWORD']
)


