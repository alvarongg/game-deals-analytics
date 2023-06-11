import click
from downloader import APIDataDownloader
from utils import format_time_now_utc


class StoreListDownloader(APIDataDownloader):
    def __init__(self, url, output_file):
        self.output_file = output_file
        self.url = url
        self.endpoint_aseembler()
        self.filename_aseembler()

    def filename_aseembler(self):
        self.file_name = self.output_file+'_'+format_time_now_utc()
        print(f'Filename:{self.file_name}')

    def json_digger(self,json):
            return json
    
    def custom_transformation(self,data):
            return data


#poetry run python .\storelist_downloader.py --url https://www.cheapshark.com/api/1.0/stores --output_file ../../csv_files/store_list/store_list
@click.command()
@click.option("--url", prompt="API URL", help="URL of the API default=https://www.cheapshark.com/api/1.0/stores",default='https://www.cheapshark.com/api/1.0/stores')
@click.option("--output_file", prompt="Output CSV", help="Output CSV file name default=game_deals",default='../../csv_files/store_list/store_list')
def main(url, output_file):
    downloader = StoreListDownloader(url, output_file)
    downloader.download_data()


if __name__ == "__main__":
    main()
    
    
    
