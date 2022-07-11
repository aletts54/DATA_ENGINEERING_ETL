# DATA_ENGINEERING_ETL
An REST API ETL PySpark Docker containerized program that automatically ingest trips taken by different vehicles, points of origin and a destination CSV files into a PostgreSQL Database.

**Technologies Involved:** 
- Python 3
- Fast API
- Apache Spark(Big Data framework chosen for scalability, performance and robustness)
- Docker
- PostgreSQL(Database that can integrate spatial data with PostGIS, do multiple inserts queries at once and provides scalability features like partitioned databases)
- Apache Sedona (Big Data library for Spark to handle Spatial Data)

**Main Functionality:**

REST API with the Fast API Python library that displays a basic custom HTML webpage with 3 Post Methods to upload data into PostgreSQL and calculates weekly average trips by area or region.

**Instalation and running**

Run the ARM docker image attached on my email and execute

```shell
docker exec -it -u 0 <containerID> /bin/sh 
```
```shell
cd /opt/spark/work-dir/Data_Engineering/API/
```
```shell
uvicorn main:app --reload
```


