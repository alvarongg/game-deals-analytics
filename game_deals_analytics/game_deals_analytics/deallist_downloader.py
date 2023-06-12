import click
from downloader import APIDataDownloader
from utils import format_time_now_utc


class DealListDownloader(APIDataDownloader):
    def __init__(
        self, url, output_file, store_id, upper_price, lower_price, page_number
    ):
        self.output_file = output_file
        self.store_id = store_id
        self.upperPrice = upper_price
        self.lowerPrice = lower_price
        self.pageNumber = page_number
        self.url = url
        self.execution_datetime = format_time_now_utc()
        self.endpoint_aseembler()
        self.filename_aseembler()

    def endpoint_aseembler(self):
        # https://www.cheapshark.com/api/1.0/deals?storeID=1&sortBy=Price&upperPrice=5&lowerPrice=0
        self.enpoint = (
            self.url
            + "?"
            + "storeID="
            + self.store_id
            + "&pageNumber="
            + self.pageNumber
            + "&sortBy=Price&"
            + "upperPrice="
            + self.upperPrice
            + "&lowerPrice="
            + self.lowerPrice
        )
        print(f"Endpoint :{self.enpoint}")


# poetry run python .\deallist_downloader.py --url https://www.cheapshark.com/api/1.0/deals --output_file ../../csv_files/game_deals/game_deals --store_id 1 --upperprice 5 --lowerprice 0 --pagenumber 1
@click.command()
@click.option(
    "--url",
    prompt="API URL",
    help="URL of the API default=https://www.cheapshark.com/api/1.0/deals",
    default="https://www.cheapshark.com/api/1.0/deals",
)
@click.option(
    "--output_file",
    prompt="Output CSV",
    help="Output CSV file name default=game_deals",
    default="../../csv_files/game_deals/game_deals",
)
@click.option(
    "--store_id", prompt="id Store", help="Id of the store default=1", default="1"
)
@click.option(
    "--upperprice", prompt="Upper Price", help="Max Price to lookup default=5"
)
@click.option(
    "--lowerprice",
    prompt="Lower Price",
    help="Lower Price to lookup defaul=0",
    default="0",
)
@click.option(
    "--pagenumber",
    prompt="Page Number",
    help="Number of the page of deals default=1",
    default="1",
)
def main(url, output_file, store_id, upperprice, lowerprice, pagenumber):
    downloader = DealListDownloader(
        url, output_file, store_id, upperprice, lowerprice, pagenumber
    )
    downloader.download_data()


if __name__ == "__main__":
    main()
