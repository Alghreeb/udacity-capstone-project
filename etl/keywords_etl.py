import pyspark.sql.types as dtypes
import boto3

from pyspark.sql.functions import (
    lit,
    current_timestamp,
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
Keywords_data_source_prefix = config.get("S3", "KEYWORDS_DATA_PREFIX")


s3 = boto3.resource("s3")
data_bucket = s3.Bucket(f"{s3_bucket_name}")


def process_keywords_data(spark):

    keywords_schema = dtypes.StructType(
        [
            dtypes.StructField("id", dtypes.IntegerType()),
            dtypes.StructField("keywords", dtypes.StringType()),
        ]
    )

    keywords_data_df = spark.createDataFrame(
        data=spark.sparkContext.emptyRDD(), schema=keywords_schema
    )
    keywords_data_full_prefix = data_source_prefix + "/" + Keywords_data_source_prefix

    for keywords_object in data_bucket.objects.filter(Prefix=keywords_data_full_prefix):
        object_uri = f"s3a://{s3_bucket_name}/{keywords_object.key}"

        # read credits data files
        csv_df = (
            spark.read.option("quote", '"')
            .option("escape", '"')
            .option("header", "true")
            .schema(keywords_schema)
            .csv(object_uri)
        )

        if keywords_data_df.count() > 0:
            keywords_data_df = keywords_data_df.union(csv_df)
        else:
            keywords_data_df = csv_df

    keywords_data_df = keywords_data_df.withColumnRenamed("id", "movie_id")

    keywords_keys = [
        {
            "dict_key": "id",
            "df_output_column": "id",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "name", "df_output_column": "name"},
    ]

    movies_keywords_df = prepare_dataframe_with_json_column(
        keywords_data_df, "keywords", keywords_keys
    )

    dim_keywords_df = (
        movies_keywords_df.select("id", "name")
        .dropDuplicates(["id"])
        .withColumn("last_updated", current_timestamp())
    )

    dim_keywords_df.printSchema()
    print(
        "Saving dim_keywords_df to redshift, Rows Count==========="
        + str(dim_keywords_df.count())
    )
    write_dataframe_to_redshift(dim_keywords_df, "dim_keywords")

    movies_keywords_df = movies_keywords_df.select("movie_id", "id").withColumnRenamed(
        "id", "keyword_id"
    )

    movies_keywords_df.printSchema()
    print(
        "Saving movies_keywords_df to redshift, Rows Count==========="
        + str(movies_keywords_df.count())
    )
    write_dataframe_to_redshift(movies_keywords_df, "movie_keywords")


def main():
    spark = create_spark_session()
    process_keywords_data(spark)


if __name__ == "__main__":
    main()
