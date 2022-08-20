# DATA ENGINEERING ETL
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

**Project Folders**
- **API:** Contains the main  program, it's a REST API python code that triggers an Apache Spark ETL Job. 
- **SQL:** Contains the SQL CREATE Statement, partitions definitions and bonus queries.
- **temporary folder** Needed as a temporary folder to store the data to be uploaded to PostgreSQL.

**Instalation and running**

Load and run the ARM docker image attached on my email and execute


Loads the Docker Image.
```shell
docker load < rest_api.pyspark.tar
```

Runs a container with the docker image.
```shell
docker run -it -d -t rest_api/pyspark /bin/sh 
```

Opens the command prompt of the container as SU.
```shell
docker exec -it -u 0 <containerID> /bin/sh 
```

Goes to the project path.
```shell
cd /opt/spark/work-dir/Data_Engineering/API/
```
Starts the Uvicorn Web Server.
```shell
uvicorn main:app --reload
```

Then you can go to your localhost on port 8000 http://127.0.0.1:8000/ to try the application. Firewalls, networks configurations or even docker could make the application not run or fail.

If you have problems running the docker image as it was made with an ARM Docker Image and not a x86 one I suggest to build one like this or I can help you to build one: 

**Building own docker image**

Pulls the Apache Spark Official Docker Image.
```shell
docker pull apache/spark-py
```
Runs a container with the docker image.
```shell
docker run -it -d -t apache/spark-py /bin/sh 
```
Copies the Data Engineer project folder attached on the email as there are some configuration files with authentication and passwords to make the project work.
```shell
docker cp /..../..../Data_Engineering/ <containerID>:/opt/spark/work-dir/
```
Opens the command prompt of the container as SU.
```shell
docker exec -it -u 0 <containerID> /bin/sh 
```
Install the required python libraries.
```python
pip install findspark
pip install pyspark
pip install numpy
pip install fastapi
pip install "uvicorn[standard]"
pip install apache-sedona
pip install requests
pip install python-multipart
```
Goes to the project path.
```shell
cd /opt/spark/work-dir/Data_Engineering/API/
```
Starts the Uvicorn Web Server.
```shell
uvicorn main:app --reload
```

As I said then you can go to your localhost on port 8000 http://127.0.0.1:8000/ to try the application. Firewalls, networks configurations or even docker could make the application not run or fail.

