
# Generated by CodiumAI
from game_deals_analytics.game_deals_analytics.Extraction_task.storelist_downloader import StoreListDownloader
import os
import csv
from urllib import response
from game_deals_analytics.game_deals_analytics.Extraction_task.downloader import APIDataDownloader


import pytest

"""
Code Analysis

Main functionalities:
The StoreListDownloader class is a subclass of APIDataDownloader and is designed to download and transform data from a specific API endpoint. It inherits the endpoint and filename assembly methods from its parent class and overrides the custom_transformation method to remove the "images" key from each dictionary in the data list. The class also has a download_data method that retrieves the data from the API endpoint, applies the custom transformation, and saves the resulting data to a CSV file.

Methods:
- __init__: initializes the object with the provided URL and output file name, as well as the current execution datetime. It also calls the endpoint and filename assembly methods.
- custom_transformation: removes the "images" key from each dictionary in the data list.
- download_data: retrieves the data from the API endpoint, applies the custom transformation, and saves the resulting data to a CSV file.

Fields:
- url: the URL of the API endpoint to download data from.
- output_file: the name of the output file to save the downloaded data to.
- execution_datetime: the datetime of when the download was executed.
"""

class TestStoreListDownloader:
    # Tests that data is successfully downloaded and transformed
    def test_successful_download_and_transformation(self):
        url = "https://example.com/api/storelist"
        output_file = "storelist"
        downloader = StoreListDownloader(url, output_file)
        downloader.download_data()
        assert downloader.file_name + ".csv" in os.listdir()
        with open(downloader.file_name + ".csv", "r") as csv_file:
            reader = csv.reader(csv_file)
            assert len(list(reader)) > 1

    # Tests that custom transformation successfully removes 'images' key from data
    def test_custom_transformation_removes_images_key(self):
        url = "https://example.com/api/storelist"
        output_file = "storelist"
        downloader = StoreListDownloader(url, output_file)
        data = [{"name": "Store 1", "images": ["image1.jpg", "image2.jpg"]}]
        transformed_data = downloader.custom_transformation(data)
        assert "images" not in transformed_data[0].keys()

    # Tests that appropriate error message is displayed when empty data is returned from API
    def test_empty_data_from_API(self):
        url = "https://example.com/api/empty"
        output_file = "empty"
        downloader = StoreListDownloader(url, output_file)
        downloader.download_data()
        assert f"Failed to download data. Response.Error: {response.status_code}" in capsys.readouterr().out

    # Tests that appropriate error message is displayed when API endpoint returns non-200 status code
    def test_non_200_status_code_from_API(self):
        url = "https://example.com/api/non200"
        output_file = "non200"
        downloader = StoreListDownloader(url, output_file)
        downloader.download_data()
        assert f"Failed to download data. Response.Error: {response.status_code}" in capsys.readouterr().out

    # Tests that appropriate error message is displayed when invalid JSON data is returned from API
    def test_invalid_JSON_data_from_API(self):
        url = "https://example.com/api/invalidjson"
        output_file = "invalidjson"
        downloader = StoreListDownloader(url, output_file)
        downloader.download_data()
        assert "Failed to download data. Error:" in capsys.readouterr().out

    # Tests that StoreListDownloader class inherits from APIDataDownloader class
    def test_inherits_from_parent_class(self):
        assert issubclass(StoreListDownloader, APIDataDownloader) is True