# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
from datetime import datetime, timedelta
from os import environ as env
import json

from pyspark.sql.functions import concat, col, lit, when, expr, to_date
from utils import format_time_now_utc

from commons import ETL_Spark


class ETL_Game_Deals(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def add_execution_datetime_to_list(self, data):
        execution_datetime = datetime.datetime.strptime(
            format_time_now_utc(), "%Y_%m_%d_%H_%M_%S"
        )
        for diccionario in data:
            diccionario.update(
                {"execution_datetime": execution_datetime.strftime("%Y/%m/%d %H:%M:%S")}
            )
        return data

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        response = requests.get(
            "https://www.cheapshark.com/api/1.0/deals?storeID=1&sortBy=Price&upperPrice=5&lowerPrice=0"
        )
        if response.status_code == 200:
            data = response.text
            print(data)
            data = json.loads(data)
            data = self.add_execution_datetime_to_list(data)

        else:
            print("Error al extraer datos de la API")
            data = []
            raise Exception("Error al extraer datos de la API")

        df = self.spark.read.json(
            self.spark.sparkContext.parallelize(data), multiLine=True
        )
        df.printSchema()
        df.show()

        return df

    def clean_data(self, df_s):
        try:
            df_s = df_s.na.drop(subset=["execution_datetime"])
            df_to_write = df_s.na.fill("/game/pc", subset=["metacriticLink"])

            return df_to_write
        except Exception as err:
            print(err)
            raise Exception("Error al limpiar los datos")

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        df = self.clean_data(df_original)

        return df

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write.format("jdbc").option("url", env["REDSHIFT_URL"]).option(
            "dbtable", f"{env['REDSHIFT_SCHEMA']}.game_deals"
        ).option("user", env["REDSHIFT_USER"]).option(
            "password", env["REDSHIFT_PASSWORD"]
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Game_Deals()
    etl.run()
