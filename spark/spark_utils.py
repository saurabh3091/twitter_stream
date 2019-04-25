from pyspark.sql import SparkSession


def get_spark_session(app_name):
    """
    Creates a spark session with local master
    :param app_name:
    :return:
    """
    session = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    return session
