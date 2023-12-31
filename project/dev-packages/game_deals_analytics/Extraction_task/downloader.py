import requests
import json
import csv
import click
from utils import format_time_now_utc
from datetime import timezone
import datetime


class APIDataDownloader:
    def __init__(self, url, output_file):
        self.url = url
        self.output_file = output_file
        self.execution_datetime = format_time_now_utc()
        self.endpoint_assembler()
        self.filename_aseembler()

    def endpoint_aseembler(self):
        self.enpoint = self.url

    def filename_aseembler(self):
        self.file_name = self.output_file + "_" + format_time_now_utc()
        print(f"Filename:{self.file_name}")

    def json_digger(self, json):
        return json

    def custom_transformation(self, data):
        return data

    def add_execution_datetime_to_list(self, data):
        execution_datetime = datetime.datetime.strptime(
            self.execution_datetime, "%Y_%m_%d_%H_%M_%S"
        )
        for diccionario in data:
            diccionario.update(
                {"execution_datetime": execution_datetime.strftime("%Y/%m/%d %H:%M:%S")}
            )
        return data

    def download_data(self):
        response = requests.get(self.enpoint)
        try:
            if response.status_code == 200:
                data = response.text
                print(data)
                json_data = json.loads(data)
                json_data = self.json_digger(json_data)
                data = self.add_execution_datetime_to_list(json_data)
                data = self.custom_transformation(data)
                self.save_to_csv(json_data)
                print("Data downloaded and saved successfully.")
            else:
                print(
                    f"Failed to download data. Response.Error: {response.status_code}"
                )
        except Exception as err:
            print(f"Failed to download data. Error: {err}")

    def save_to_csv(self, data):
        try:
            if data:
                with open(self.file_name + ".csv", "w", newline="") as csv_file:
                    writer = csv.writer(csv_file, delimiter=",")
                    writer.writerow(data[0].keys())
                    for item in data:
                        writer.writerow(item.values())
        except Exception as err:
            print(err)


@click.command()
@click.option("--url", prompt="API URL", help="URL of the API")
@click.option("--output", prompt="Output CSV", help="Output CSV file name")
def main(url, output):
    downloader = APIDataDownloader(url, output)
    downloader.download_data()


if __name__ == "__main__":
    main()
