import sys
from pyspark.sql import SparkSession

def spark_init():
    spark = SparkSession.builder \
             .appName("Data Engineering ETL") \
             .getOrCreate()
    return spark

def data_to_sql():


if __name__ == "__main__"

    spark = spark_init()
    data_to_sql(spark)