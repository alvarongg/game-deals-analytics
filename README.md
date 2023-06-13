# Game-deals-analytics --  Final Project  -- Data Engineer CoderHouse 2023

Game deals analytics is a tool that allows you to download information about video game deals from different online stores.

After downloading the information, it will be transformed according to the business rules and loaded into a Redshift database to be subsequently analyzed.
## Builded With 🛠️
* [Docker :whale: ](https://www.docker.com/)
* [Python](https://expressjs.com/es/4x/api.html)
* [Poetry](https://momentjs.com)
## Python Libraries
* [Requests](https://requests.readthedocs.io/en/latest/)
* [Click](https://click.palletsprojects.com/en/8.1.x/)
* [Json](https://docs.python.org/3/library/json.html)
* [Datetime](https://docs.python.org/3/library/datetime.html)
* [Pandas](https://pandas.pydata.org/docs/)
## Deliverable Package 1

    > The script should extract data in JSON and be able to read the format in a Python dictionary.
    > The delivery involves the creation of an initial version of the table where the data will be loaded later.

# Data extraction

The API selected for the extraction of information is "cheapshark.com", all the documentation is available here: [https://apidocs.cheapshark.com/#b9b738bf-2916-2a13-e40d-d05bccdce2ba](https://apidocs.cheapshark.com/#b9b738bf-2916-2a13-e40d-d05bccdce2ba) 

## Package Deploy📦
### Requirements
* Python ^3.11
* Poetry ^1.5

* Installation of dependencies
> In command line move to the project folder and execute
```bash
poetry init 
poetry install
```

[To know how to execute the extraction and transformation you must enter here](game_deals_analytics\README.md)

# Database 

In the **database_scritps** folder there are the scripts for creating the tables in redshift in which the information will later be dumped using the transformation scripts.

The database diagram will be as follows:

![Game Deals Analytics](https://github.com/alvarongg/game-deals-analytics/assets/8601103/3f59a14c-baab-4d34-a7af-f30d8d31a98b)
