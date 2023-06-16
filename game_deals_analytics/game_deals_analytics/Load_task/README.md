## Extractions

### Store List
> In order to download the store list, in terminal move to project folder and run de next command line statement. This command line downloads the list of all stores and saves it in a csv file, in the /csv_files/store_list/ folder of the project
```bash
poetry run python .\game_deals_analytics\extraction_task\storelist_downloader.py --url https://www.cheapshark.com/api/1.0/stores --output_file ../csv_files/store_list/store_list
```
#### Store List Downloader Arguments
* **--url** > URL of the endpoint to download data Defaul=https://www.cheapshark.com/api/1.0/stores
* **--output_file** > folder and name where you want to download the data
### Game Deals
> In order to download the game deals, in terminal move to project folder and run de next command line statement.This command line downloads the list of game deals for the store and range specified in the argument and saves it in a csv file, in the /csv_files/game_deals/ folder of the project
> By varying the execution arguments we can select different ranges of deals and also select different stores.
```bash
poetry run python .\game_deals_analytics\extraction_task\deallist_downloader.py --url https://www.cheapshark.com/api/1.0/deals --output_file ../csv_files/game_deals/game_deals --store_id 1 --upperprice 5 --lowerprice 0 --pagenumber 1
```
#### Game Deals Downloader Arguments
* **--url** > URL of the endpoint to download data default= https://www.cheapshark.com/api/1.0/deals
* **--output_file** > folder and name where you want to download the data = ../csv_files/game_deals/
* **--store_id** > id of online game store Default = 1 (SteamStore)
* **--upperprice** > max price to search Defaul = 5 (usd)
* **--lowerprice** > min price to search Default = 0 (usd) min = 0 
* **--pagenumber** > number of the page we want to download for the search default = 1 max= 50