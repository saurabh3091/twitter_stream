from pyspark.sql import SparkSession


def get_spark_session(app_name):
    session = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    return session
