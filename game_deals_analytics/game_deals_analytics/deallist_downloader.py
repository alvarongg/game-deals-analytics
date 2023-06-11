import click
from downloader import APIDataDownloader


class DealListDownloader(APIDataDownloader):
    def __init__(self, url, output_file,store_id):
        self.url = url.replace('storeID=1',f'storeID={store_id}')
        self.output_file = output_file
        self.store_id = store_id

    def json_digger(self,json):
            return json
    
    def custom_transformation(self,data):
            return data

@click.command()
@click.option("--url", prompt="API URL", help="URL of the API")
@click.option("--output", prompt="Output CSV", help="Output CSV file name")
def main(url, output):
    downloader = APIDataDownloader(url, output)
    downloader.download_data()


if __name__ == "__main__":
    main()