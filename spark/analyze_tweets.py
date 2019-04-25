import argparse
import json
import logging
import os

from pyspark.sql.functions import lower, explode, split, col

from resources.tweet_schema import tweet_schema
from spark.spark_utils import get_spark_session


def parse_cmd_line_args():
    """
    This function parses the command line arguments
    :returns: argparse
    """
    parser = argparse.ArgumentParser(description="Twitter analytics")
    parser.add_argument('--json', help='Source file path',
                        required=True)
    return parser.parse_args()


def main():
    # set logger
    logging.basicConfig(level=logging.INFO)

    # parse input params to spark job
    args = parse_cmd_line_args()

    # create spark session with local master
    spark = get_spark_session("twitter_analysis")

    # read raw tweets json
    df = spark.read.json(args.json, schema=tweet_schema) \
        .selectExpr("created_at", "text", "place.name as city","place.country_code as country",
                    "user.location as user_location", "entities.hashtags as hashtags")
    logging.info(f" Dataframe has a total of {df.count()} tweets")

    # filter tweets for Amsterdam or NL
    place_filtered_df = df.filter((lower(df.city) == "amsterdam") | (lower(df.country)=="nl"))

    logging.info(f"Filtered dataframe has {place_filtered_df.count()} tweets")

    logging.info("Caching Dataframe...")
    place_filtered_df.cache()

    # get top trending words from tweets
    top_words_df = place_filtered_df.select("text").withColumn('word', explode(split(col('text'), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)

    top_words = [row.word for row in top_words_df.take(10)]

    # get top trending hashtags from tweets
    trending_hashtags_df = place_filtered_df.select("text").withColumn('hashtag', explode(split(col('text'), ' '))) \
        .filter(col("hashtag").startswith("#")) \
        .groupBy('hashtag') \
        .count() \
        .sort('count', ascending=False)

    trending_hashtags = [row.hashtag for row in trending_hashtags_df.take(10)]

    logging.info(f"Saving trend analysis to {os.path.dirname(os.path.realpath(__file__))}/../output.json")

    with open("../output.json", "w") as f:
        json.dump({"trending_words" : top_words, "trending_hashtags": trending_hashtags}, f)
    f.close()

    logging.info("Job completed successfully")


if __name__ == '__main__':
    main()
