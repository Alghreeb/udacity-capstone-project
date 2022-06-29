import pyspark.sql.types as dtypes
import boto3

from pyspark.sql.functions import col
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    when,
)
from spark_session import create_spark_session
from utils import (
    write_dataframe_to_redshift,
    prepare_dataframe_with_json_column,
    get_configs,
)


config = get_configs()
s3_bucket_name = config.get("S3", "AWS_S3_BUCKET")

data_source_prefix = config.get("S3", "S3_MOVIES_DATA_SOURCE_PREFIX")
credits_data_source_prefix = config.get("S3", "CREDITS_DATA_PREFIX")

s3 = boto3.resource("s3")
data_bucket = s3.Bucket(f"{s3_bucket_name}")

FEMALE_GENDER = "F"
MALE_GENDER = "M"
UNSPECIFIED_GENDER = "U"


def process_credits_data(spark):

    credits_schema = dtypes.StructType(
        [
            dtypes.StructField("cast", dtypes.StringType()),
            dtypes.StructField("crew", dtypes.StringType()),
            dtypes.StructField("id", dtypes.IntegerType()),
        ]
    )

    credits_data_df = spark.createDataFrame(
        data=spark.sparkContext.emptyRDD(), schema=credits_schema
    )
    credits_data_full_prefix = data_source_prefix + "/" + credits_data_source_prefix

    for credits_object in data_bucket.objects.filter(Prefix=credits_data_full_prefix):
        object_uri = f"s3a://{s3_bucket_name}/{credits_object.key}"

        # read credits data files
        csv_df = (
            spark.read.option("quote", '"')
            .option("escape", '"')
            .option("header", "true")
            .schema(credits_schema)
            .csv(object_uri)
        )

        if credits_data_df.count() > 0:
            credits_data_df = credits_data_df.union(csv_df)
        else:
            credits_data_df = csv_df

    credits_data_df = credits_data_df.withColumnRenamed("id", "movie_id")

    cast_keys = [
        {
            "dict_key": "id",
            "df_output_column": "id",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "name", "df_output_column": "name"},
        {"dict_key": "character", "df_output_column": "character"},
        {
            "dict_key": "gender",
            "df_output_column": "gender",
            "column_type": dtypes.IntegerType(),
        },
    ]

    movies_cast_df = prepare_dataframe_with_json_column(
        credits_data_df, "cast", cast_keys
    )

    dim_cast_df = (
        movies_cast_df.select("id", "name", "character", "gender")
        .dropDuplicates(["id"])
        .withColumn("last_updated", current_timestamp())
        .withColumn(
            "gender",
            when(col("gender") == 1, FEMALE_GENDER).otherwise(
                when(col("gender") == 2, MALE_GENDER).otherwise(
                    when(col("gender") == 0, UNSPECIFIED_GENDER)
                )
            ),
        )
    )

    write_dataframe_to_redshift(dim_cast_df, "dim_cast")

    movies_cast_df = movies_cast_df.select("movie_id", "id").withColumnRenamed(
        "id", "cast_id"
    )

    write_dataframe_to_redshift(movies_cast_df, "movie_cast")

    crew_keys = [
        {
            "dict_key": "id",
            "df_output_column": "id",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "name", "df_output_column": "name"},
        {"dict_key": "department", "df_output_column": "department"},
        {
            "dict_key": "gender",
            "df_output_column": "gender",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "job", "df_output_column": "job"},
    ]

    movies_crew_df = prepare_dataframe_with_json_column(
        credits_data_df, "crew", crew_keys
    )

    dim_crew_df = (
        movies_crew_df.select("id", "name", "department", "gender", "job")
        .dropDuplicates(["id"])
        .withColumn("last_updated", current_timestamp())
        .withColumn(
            "gender",
            when(col("gender") == 1, FEMALE_GENDER).otherwise(
                when(col("gender") == 2, MALE_GENDER).otherwise(
                    when(col("gender") == 0, UNSPECIFIED_GENDER)
                )
            ),
        )
    )

    write_dataframe_to_redshift(dim_crew_df, "dim_crew")

    movies_crew_df = movies_crew_df.select("movie_id", "id").withColumnRenamed(
        "id", "crew_id"
    )

    write_dataframe_to_redshift(movies_crew_df, "movie_crew")


def main():

    spark = create_spark_session()
    process_credits_data(spark)


if __name__ == "__main__":
    main()
