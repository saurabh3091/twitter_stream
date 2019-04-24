from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# schema used to parse tweets json.
tweet_schema = StructType([
    StructField("created_at", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("place", StructType([
        StructField("name", StringType(), nullable=True),
        StructField("country_code", StringType(), nullable=True)
    ]), nullable=True),
    StructField("user", StructType([
        StructField("location", StringType(), nullable=True),
        StructField("created_at", StringType(), nullable=True),
        StructField("updateTime", StringType(), nullable=True)
    ]), nullable=True),
    StructField("entities", StructType([
        StructField("hashtags",
                    ArrayType(StructType([
                        StructField("text", StringType(), nullable=True)
                    ])), nullable=True)
    ]), nullable=True)
])
