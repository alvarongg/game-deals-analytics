
# Generated by CodiumAI
from game_deals_analytics.game_deals_analytics.Extraction_task.deallist_downloader import DealListDownloader
import os


import pytest

"""
Code Analysis

Main functionalities:
The DealListDownloader class is a subclass of APIDataDownloader and is designed to download deal lists from the CheapShark API. It takes in various parameters such as store ID, upper and lower price limits, and page number to construct the API endpoint URL. It then downloads the data from the API, processes it, and saves it to a CSV file.

Methods:
- __init__: Initializes the class and sets the various parameters such as store ID, upper and lower price limits, and page number. It also calls the endpoint_assembler and filename_assembler methods to construct the API endpoint URL and the output file name.
- endpoint_assembler: Constructs the API endpoint URL using the various parameters passed to the class.
- filename_assembler: Constructs the output file name using the current date and time.
- download_data: Downloads the data from the API, processes it, and saves it to a CSV file.
- save_to_csv: Saves the processed data to a CSV file.
- add_execution_datetime_to_list: Adds the execution date and time to the processed data.
- custom_transformation: Allows for custom transformation of the processed data.
- json_digger: Allows for digging into the JSON data to extract specific information.

Fields:
- output_file: The name of the output file.
- store_id: The ID of the store to retrieve deals from.
- upperPrice: The upper price limit for the deals.
- lowerPrice: The lower price limit for the deals.
- pageNumber: The page number of the results to retrieve.
- url: The base URL of the CheapShark API.
- execution_datetime: The date and time the class was executed.
- endpoint: The constructed API endpoint URL.
"""

class TestDealListDownloader:
    # Tests that data is successfully downloaded and saved to a CSV file
    def test_successful_download_and_save(self):
        url = "https://www.cheapshark.com/api/1.0/deals"
        output_file = "deals"
        store_id = "1"
        upper_price = "5"
        lower_price = "0"
        page_number = "0"
        downloader = DealListDownloader(
            url, output_file, store_id, upper_price, lower_price, page_number
        )
        downloader.download_data()
        assert os.path.exists(downloader.file_name + ".csv")

    # Tests that the endpoint is correctly assembled with valid input parameters
    def test_endpoint_assembly_with_valid_input(self):
        url = "https://www.cheapshark.com/api/1.0/deals"
        output_file = "deals"
        store_id = "1"
        upper_price = "5"
        lower_price = "0"
        page_number = "0"
        downloader = DealListDownloader(
            url, output_file, store_id, upper_price, lower_price, page_number
        )
        downloader.endpoint_aseembler()
        expected_endpoint = (
            url
            + "?"
            + "storeID="
            + store_id
            + "&pageNumber="
            + page_number
            + "&sortBy=Price&"
            + "upperPrice="
            + upper_price
            + "&lowerPrice="
            + lower_price
        )
        assert downloader.enpoint == expected_endpoint

    # Tests that an exception is raised when invalid input parameters are used for endpoint assembly
    def test_invalid_input_parameters_for_endpoint_assembly(self):
        url = "https://www.cheapshark.com/api/1.0/deals"
        output_file = "deals"
        store_id = "1"
        upper_price = "5"
        lower_price = "0"
        page_number = "invalid_input"
        with pytest.raises(TypeError):
            downloader = DealListDownloader(
                url, output_file, store_id, upper_price, lower_price, page_number
            )
            downloader.endpoint_aseembler()