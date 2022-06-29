import pyspark.sql.types as dtypes
import boto3

from pyspark.sql.functions import (
    current_timestamp,
    avg,
    min,
    max,
    count,
)
from spark_session import create_spark_session
from utils import write_dataframe_to_redshift, get_configs


config = get_configs()
s3_bucket_name = config.get("S3", "AWS_S3_BUCKET")
data_source_prefix = config.get("S3", "S3_MOVIES_DATA_SOURCE_PREFIX")
ratings_data_prefix = config.get("S3", "RATINGS_DATA_PREFIX")

s3 = boto3.resource("s3")
data_bucket = s3.Bucket(f"{s3_bucket_name}")


def process_ratings_data(spark):

    ratings_schema = dtypes.StructType(
        [
            dtypes.StructField("userId", dtypes.IntegerType()),
            dtypes.StructField("movieId", dtypes.IntegerType()),
            dtypes.StructField("rating", dtypes.FloatType()),
            dtypes.StructField("timestamp", dtypes.IntegerType()),
        ]
    )

    ratings_data_df = spark.createDataFrame(
        data=spark.sparkContext.emptyRDD(), schema=ratings_schema
    )
    rating_data_full_prefix = data_source_prefix + "/" + ratings_data_prefix

    for ratings_object in data_bucket.objects.filter(Prefix=rating_data_full_prefix):
        print(ratings_object.key)
        ratings_object_uri = f"s3a://{s3_bucket_name}/{ratings_object.key}"
        csv_df = (
            spark.read.option("header", "true")
            .schema(ratings_schema)
            .csv(ratings_object_uri)
        )

        if ratings_data_df.count() > 0:
            ratings_data_df = ratings_data_df.union(csv_df)
        else:
            ratings_data_df = csv_df

    
    movies_ratings_df = (
        ratings_data_df.groupBy("movieId").agg(
            count("userId").alias("ratings_count"),
            avg("rating").alias("ratings_average"),
            min("rating").alias("min_rate"),
            max("rating").alias("max_rate"),
        )
    ).withColumn("last_updated", current_timestamp()).withColumnRenamed("movieId", "movie_id")

    movies_ratings_df.printSchema()
    print(
        "Saving ratings_data_df to redshift, Rows Count==========="
        + str(movies_ratings_df.count())
    )

    write_dataframe_to_redshift(movies_ratings_df, "fact_rating")


def main():
    spark = create_spark_session()
    process_ratings_data(spark)


if __name__ == "__main__":
    main()
