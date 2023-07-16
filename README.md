# Game-deals-analytics --  Final Project  -- Data Engineer CoderHouse 2023

Game deals analytics is a tool that allows you to download information about video game deals from different online stores.

After downloading the information, it will be transformed according to the business rules and loaded into a Redshift database to be subsequently analyzed.
## Builded With 🛠️
* [Docker :whale: ](https://www.docker.com/)
* [Python](https://expressjs.com/es/4x/api.html)
* [Poetry](https://momentjs.com)
## Python Libraries
* [Requests](https://requests.readthedocs.io/en/latest/)
* [Json](https://docs.python.org/3/library/json.html)
* [Datetime](https://docs.python.org/3/library/datetime.html)
* [Spark](https://spark.apache.org/docs/latest/api/python/index.html)
## Deliverable Package 1

> The script should extract data in JSON and be able to read the format in a Python dictionary.
> The delivery involves the creation of an initial version of the table where the data will be loaded later.
## Deliverable Package 2
> Create a pyspark job that allows you to transform the data and load it into a table in Redshift.
## Deliverable Package 2
> Automate the extraction and transformation of data using Airflow.

# Data extraction

The API selected for the extraction of information is "cheapshark.com", all the documentation is available here: [https://apidocs.cheapshark.com/#b9b738bf-2916-2a13-e40d-d05bccdce2ba](https://apidocs.cheapshark.com/#b9b738bf-2916-2a13-e40d-d05bccdce2ba) 

## Package Deploy📦 and Execution🚀
### Folders and files structure
The files to be deployed are in the **project** folder, the structure of the folders and files is as follows:
* `docker_images/`: Contains the Dockerfiles for the Airflow and Spark images.  
* `docker-compose.yml`: Docker compose file for the Airflow and Spark containers.
* `.env`: Environment variables file for the Airflow and Spark containers.
* `dags/`: Cointains the DAGs for Airflow.
    * `etl_game_deals.py`: Pricipal DAG for the ETL process how is executed in Airflow for download, transform and load the data from the API to the Redshift database.
* `logs/`: Folder with the logs of Airflow.
* `postgres_data/`: Folder with the data of Postgres.
* `scripts/`: Folder with the scripts for the ETL process.
    * `postgresql-42.5.2.jar`: Jar file for the JDBC connection to Redshift.
    * `common.py`:  Common class for the ETL processes.
    * `utils.py`: Utility functions for the ETL process.
    * `ETL_Game_Deals.py`: Script for the ETL process.


# Steps for deploy the project
1. Clone the repository.
2. Move to the project folder.
4. Create a .env file with the next environment variables:
```bash
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_SCHEMA=...
REDSHIFT_PASSWORD=...
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```
3. Run the next command line statement for build the images and run the containers.
```bash 
docker-compose up -d
```
4. Once the containers are running, enter to the Airflow web interface in `http://localhost:8080/`.
5. On tab `Admin -> Connections` create a new connection with the following data for Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
6. On tab `Admin -> Connections` create a new connection with the following data for Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
7. On tab `Admin -> Variables` create a new variable with the following data:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
8. On tab`Admin -> Variables` create a new variable with the following data:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`
9. Execute DAG `etl_game_deals`.


# Build the images and run the containers
1. Clone the repository.
2. Move to the project folder.
3. Move to the `docker_images` folder.
4. Run the next command line statement for build the images and run the containers.
```bash
docker build -t airflow:2.1.2 .
docker build -t spark:3.1.2 .
```
5. Change the `docker-compose.yml` file with the name of the new images.
6. Run the next command line statement for run the containers.
```bash
docker-compose up -d
```

# Database 

In the **database_scritps** folder there are the scripts for creating the tables in redshift in which the information will later be dumped using the transformation scripts.

The database diagram will be as follows:

![Game Deals Analytics](https://github.com/alvarongg/game-deals-analytics/assets/8601103/3f59a14c-baab-4d34-a7af-f30d8d31a98b)


# Develop by
* **Alvaro Garcia** - [alvarongg](https://github.com/alvarongg/)