#REST API with the Fast API Python library that displays a basic custom HTML webpage with 3 Post Methods 
#to upload data into PostgreSQL and calculates weekly average trips by area or region 
#using Apache Spark and Apache Sedona Geospatial Library.

#Libraries
from typing import List
from config_variables import *  #Config file with passwords and authentication
import shutil
from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse, Response
import sys
import os
import findspark
findspark.init() #It needs to be here so pyspark library can be imported
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sedona.register import SedonaRegistrator  
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
import requests
from datetime import datetime


#Function that sends notifications emails when the ETL Process starts and finishes, messages it's the message to be send
#It currently sends emails to one email adress as it is a test account.  More info: https://www.mailgun.com
def send_simple_message(message):
	return requests.post(
		email_endpoint,
		auth=auth,
		data={"from": from_email,
			"to": to,
			"subject": "ETL STATUS",
			"text": message})

#Spark initialization function that configures Apache Spark with the required settings as apache sedona, postgresql jdbc, geotools
def spark_init():
    spark = SparkSession. \
        builder. \
        appName('appName'). \
        config("spark.serializer", KryoSerializer.getName). \
        config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
        config("spark.driver.extraClassPath", "org.postgresql:postgresql:42.4.0"). \
        config('spark.jars.packages', 'org.postgresql:postgresql:42.4.0,org.apache.sedona:sedona-python-adapter-3.0_2.12:1.2.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-24.1'). \
        getOrCreate()
    return spark

#Function that reads a file from the REST API, 
#Groups the data by similar origin, destination, and time of day with K-Means ML Algorithm
#Finally it uploads the data to a PostgreSQL Database
def data_to_sql(file_to_export):

    #Datetime object containing current date and time to send an ETL initialization email
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    message = 'ETL Process Started, inserting {file_to_export} file into PostgreSQL at {dt_string}'.format(file_to_export = file_to_export, dt_string = dt_string) 
    send_simple_message(message)

    #Apache Spark initialization function call and Apache Sedona geospatial functions enabled
    spark = spark_init()
    SedonaRegistrator.registerAll(spark)

    #Reads the csv file to be uploaded to PostgreSQL and partitions it for improved perfomance
    file = "/opt/spark/work-dir/Data_Engineering/temporary_folder/" + file_to_export
    csv_file = spark.read.format("csv").option("header", "true").\
                load(file).\
                repartition(10)
    csv_file.createOrReplaceTempView("csv")  

    #Extracts coordinates from origin and destination columns and time of day to be used as features for the K-Means Algorithm
    csv_geometry = spark.sql(
                            """
                            SELECT region, 
                                    origin_coord,
                                    destination_coord,                                
                                    ST_X(ST_GeomFromWKT(origin_coord)) AS origin_coord_X,
                                    ST_Y(ST_GeomFromWKT(origin_coord)) AS origin_coord_Y,
                                    ST_X(ST_GeomFromWKT(destination_coord)) AS destination_coord_X,
                                    ST_Y(ST_GeomFromWKT(destination_coord)) AS destination_coord_Y,
                                    CAST(date_format(datetime, 'HH') AS INT) AS time_of_day,
                                    to_timestamp(datetime) AS datetime,
                                    datasource
                            FROM csv
                            """ 
                            ) 

    #Makes the features vector from origin, destination, and time of day columns
    vecAssembler = VectorAssembler(inputCols=["origin_coord_X", "origin_coord_Y", "destination_coord_X", "destination_coord_Y", "time_of_day"], outputCol="features")
    features_geometry = vecAssembler.transform(csv_geometry)

    # Trains a k-means model with 10 clusters
    kmeans = KMeans().setK(10).setSeed(1)
    model = kmeans.fit(features_geometry)

    #Make predictions, drops the features columns, 
    #Renames the kmeans predictions column that contains origin, destination, and time of day clustered/grouped data
    predictions = model.transform(features_geometry).\
    drop('origin_coord_X', 'origin_coord_Y', 'destination_coord_X', 'destination_coord_Y', 'time_of_day', 'features').\
    withColumnRenamed('prediction', 'similar_origin_destination_time_group')

    #Uploads data into PostgreSQL trips database
    predictions.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", url) \
        .option("dbtable", "public.trips") \
        .option("user", properties['user']) \
        .option("password", properties['password']) \
        .mode("append").save()

    # datetime object containing current date and time containing current date and time to send an ETL completed email
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    message = 'ETL Process Finished at {dt_string}, {file_to_export} file Inserted into PostgreSQL'.format(file_to_export = file_to_export, dt_string = dt_string) 
    send_simple_message(message)

    #Removes the file from the temporary_folder and stops Apache Spark Cluster
    os.remove(file)
    spark.stop()

#Function that calculates weekly average trips by area like
#Example Inputs: Turin, Hamburg, Prague
#Or a Polygon Shape Region( POLYGON((x1 y1, x2 y2, x3 y3, x4 y4, x1 y1))  ) like 
#Example Input: POLYGON((25.32427 60.00002, 24.32427345662177 30.00002074358429, 1.32427345662177 60.00002074358429, 25.32427 60.00002))    
def weekly_average(filter_string, flag_area_region):

    #Apache Spark initialization function call and Apache Sedona geospatial functions enabled
    spark = spark_init()
    SedonaRegistrator.registerAll(spark)

    #Reads the data from the PostgreSQL trips database and partitions it for improved perfomance
    postgresql_trips_df = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", url) \
        .option("dbtable", "public.trips") \
        .option("user", properties['user']) \
        .option("password", properties['password']) \
        .load().repartition(10) 

    postgresql_trips_df.createOrReplaceTempView("trips")

    #IF Statement conditions that distinguishes between an area or a Polygon Shape Region by using the flag_area_region variable 
    #Then it calculates the weekly average trips, by counting trips grouped by week and year then  calculating the average of it
    if flag_area_region == 'region':
        query = """
                    SELECT 
                        region,
                        CASE 
                            WHEN AVG(TripsCount) IS NULL THEN 0
                            ELSE AVG(TripsCount)
                        END AS weekly_average
                    FROM(
                            SELECT 
                                region,
                                CONCAT(extract(week FROM datetime), '-' ,extract(year FROM datetime)) as WeekYear,
                                COUNT(*) as TripsCount
                            FROM
                                trips
                            WHERE
                                region = "{}"
                            GROUP BY
                                region,
                                WeekYear
                        )AS REGION_WEEK_COUNT
                    GROUP BY
                        region
                """.format(filter_string) 
    elif flag_area_region == 'area':
        query = """
                    SELECT 
                        CASE 
                            WHEN AVG(TripsCount) IS NULL THEN 0
                            ELSE AVG(TripsCount)
                        END AS weekly_average
                    FROM(
                            SELECT 
                                CONCAT(extract(week FROM datetime), '-' ,extract(year FROM datetime)) as WeekYear,
                                COUNT(*) as TripsCount
                            FROM
                                trips
                            WHERE
                                WHERE ST_Contains(ST_GeomFromWKT('{}'),ST_GeomFromWKT(origin_coord))
                            GROUP BY
                                WeekYear
                        )AS REGION_WEEK_COUNT
                """.format(filter_string) 

    #If there is no results it returns 'Empty Area/Region' otherwise it returns the average weekly trips
    if spark.sql(query).isEmpty():
        weekly_average = 'Empty Area/Region' 
    else:
        weekly_average = 'Weekly Average Trips of ' + filter_string + ': ' + str(spark.sql(query).first()["weekly_average"]) 

    #Stops Apache Spark Cluster and returns the results
    spark.stop()
    return weekly_average


#Fast REST API with 3 Post Methods to upload data into postgree and to calculate weekly avergae trips by area or region
app = FastAPI()

#uploadfiles Gets the CSV files to be uploaded to PostgreSQL
#Then it calls the data_to_sql function that uploads the csv file into postgresql using apache spark
@app.post("/uploadfiles/")
async def create_upload_files(files: List[UploadFile]):
    #Saves file into a temporary folder
    for file in files:
        with open("/opt/spark/work-dir/Data_Engineering/temporary_folder/{}".format(file.filename), "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    #Calls the data_to_sql apache spark function
    for file in files:
        data_to_sql(file.filename) 

    return "Successful"

#searchregion calls the weekly_average function to calculate weekly average trips by region
@app.post("/searchregion/", response_class=Response)
async def get_body(request: Request):
    response_string = str(await request.body(), 'utf-8').partition("=")[2] 
    return weekly_average(response_string.rstrip(), "region")

#searchregion calls the weekly_average function to calculate weekly average trips by area
@app.post("/searcharea/", response_class=Response)
async def get_body(request: Request):
    response_string = str(await request.body(), 'utf-8').partition("=")[2]
    return weekly_average(response_string.rstrip(), "area")

#GET Rest Method that displays a custom HTML webpage GUI to test the Posts REST APIS on localhost URL usually http://127.0.0.1:8000
#To display the webpage it needs "uvicorn main:app --reload" library and command to display
#More Info https://fastapi.tiangolo.com/tutorial/
@app.get("/")
async def main():
    content = """
            <style>
                 body {text-align: center;}
            </style>
            <body>
                <br>
                <br>
                <br>
                <h1>Upload files to PostgreSQL</h1>
                    <br>
                    <br>
                    <br>
                <form action="/uploadfiles/" enctype="multipart/form-data" method="post" style="font-size: 24px;">
                    <input name="files" type="file" multiple style="font-size: 24px;">
                    <input type="submit" style="font-size: 24px;">
                </form>
                <br>
                <br>
                <h1>Weekly average number of trips by region</h1>
                <h2>Example Inputs: Turin, Hamburg, Prague</h2>
                    <br>
                    <br>
                    <br>
                <form action="/searchregion/" enctype="text/plain" method="post" style="font-size: 24px;">
                    <input name="region" type="text" multiple style="font-size: 24px;">
                    <input type="submit" style="font-size: 24px;">
                </form>
                <h1>Weekly average number of trips by area</h1>
                <h3>POLYGON((x1 y1, x2 y2, x3 y3, x4 y4, x1 y1)) As Argument</h3>
                <h2>Example Input: POLYGON((25.32427 60.00002, 24.32427345662177 30.00002074358429, 1.32427345662177 60.00002074358429, 25.32427 60.00002)) </h2>
                    <br>
                    <br>
                    <br>
                <form action="/searcharea/" enctype="text/plain" method="post" style="font-size: 24px;">
                    <input name="area" type="text" multiple style="font-size: 24px;">
                    <input type="submit" style="font-size: 24px;">
                </form>
            </body>
    """
    return HTMLResponse(content=content)
