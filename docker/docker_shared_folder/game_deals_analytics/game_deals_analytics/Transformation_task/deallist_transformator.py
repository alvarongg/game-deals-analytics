import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col

def setup():
    try:
        env = os.environ

        # Postgres and Redshift JDBCs
        driver_path = env['DRIVER_PATH']

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

        redshift_url = "jdbc:postgresql://{host}:{port}/{database}".format(
            host=env['AWS_REDSHIFT_HOST'],
            port=env['AWS_REDSHIFT_PORT'],
            database=env['AWS_REDSHIFT_DBNAME']
            )

        redshift_properties = {
            "user": env['AWS_REDSHIFT_USER'],
            "password": env['AWS_REDSHIFT_PASSWORD'],
            "driver": "org.postgresql.Driver"
        }


        return env,spark,conn,redshift_url,redshift_properties

    except Exception as err:
            print(err)

def create_table(env,conn):
    try:
          
        cursor = conn.cursor()
        cursor.execute(f"""
        create table if not exists {env['AWS_REDSHIFT_SCHEMA']}.game_deals (
            internalName VARCHAR,
                title VARCHAR,
                metacriticLink VARCHAR,
                dealID VARCHAR,
                storeID INT,
                gameID INT,
                salePrice DOUBLE PRECISION,
                normalPrice DOUBLE PRECISION,
                isOnSale INT,
                savings DOUBLE PRECISION,
                metacriticScore INT,
                steamRatingText VARCHAR,
                steamRatingPercent INT,
                steamRatingCount INT,
                steamAppID INT,
                releaseDate INT,
                lastChange INT,
                dealRating DOUBLE PRECISION,
                thumb VARCHAR,
                execution_datetime VARCHAR
        );
        """)
        conn.commit()
        cursor.close()
        print("Table created!")

    except Exception as err:
            print(err)


def load_data(spark):
    try:

        # Import data from game_deals.csv
        df_s = spark.read.option("header",True) \
                .option("inferSchema",True) \
                .csv("../../../data/landing/game_deals/", )
        df_s.printSchema()
        df_s.show()
        df_s.count()

        return df_s
    except Exception as err:
        print(err)

def clean_data(df_s):
    try:
        
        df_s = df_s.na.drop(subset=['execution_datetime'])
        df_to_write = df_s.na.fill('/game/pc', subset=['metacriticLink'])

        return df_to_write
    except Exception as err:
         print(err)


def write_data_to_redshift(df_to_write,redshift_url,redshift_properties,table_name):
    try:
        
        df_to_write.write.jdbc(url=redshift_url, table='game_deals' , mode="overwrite", properties=redshift_properties)
    except Exception as err:
         print(err)


def process_data():

    print("Setup")

    env,spark,conn,redshift_url,redshift_properties = setup()

    print("Create table")

    create_table(env,conn)

    print("Extract")
    df = load_data(spark)

    print("Clean Data")

    df = clean_data(df)

    
    print("Load")

    write_data_to_redshift(df,redshift_url,redshift_properties,'game_deals')

    print('success')

if __name__ == "__main__":
     process_data()