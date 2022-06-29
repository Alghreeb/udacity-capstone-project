import pyspark.sql.types as dtypes
import boto3
from pyspark.sql.functions import (
    current_timestamp,
    col,
    udf,
)
from spark_session import create_spark_session
from utils import (
    write_dataframe_to_redshift,
    prepare_dataframe_with_json_column,
    get_configs,
)
from langcodes import Language


config = get_configs()
s3_bucket_name = config.get("S3", "AWS_S3_BUCKET")
movies_data_source_prefix = config.get("S3", "S3_MOVIES_DATA_SOURCE_PREFIX")
movies_meta_data_prefix = config.get("S3", "MOVIES_META_DATA_PREFIX")

s3 = boto3.resource("s3")
data_bucket = s3.Bucket(f"{s3_bucket_name}")


def process_movies_data(spark):

    movies_schema = dtypes.StructType(
        [
            dtypes.StructField("adult", dtypes.BooleanType()),
            dtypes.StructField("belongs_to_collection", dtypes.StringType()),  # JSON
            dtypes.StructField("budget", dtypes.DoubleType()),
            dtypes.StructField("genres", dtypes.StringType()),  # JSON
            dtypes.StructField("homepage", dtypes.StringType()),
            dtypes.StructField("id", dtypes.IntegerType()),
            dtypes.StructField("imdb_id", dtypes.StringType()),
            dtypes.StructField("original_language", dtypes.StringType()),
            dtypes.StructField("original_title", dtypes.StringType()),
            dtypes.StructField("overview", dtypes.StringType()),
            dtypes.StructField("popularity", dtypes.FloatType()),
            dtypes.StructField("poster_path", dtypes.StringType()),
            dtypes.StructField("production_companies", dtypes.StringType()),  # JSON
            dtypes.StructField("production_countries", dtypes.StringType()),  # JSON
            dtypes.StructField("release_date", dtypes.DateType()),
            dtypes.StructField("revenue", dtypes.DoubleType()),
            dtypes.StructField("runtime", dtypes.IntegerType()),
            dtypes.StructField("spoken_languages", dtypes.StringType()),  # JSON
            dtypes.StructField("status", dtypes.StringType()),
            dtypes.StructField("tagline", dtypes.StringType()),
            dtypes.StructField("title", dtypes.StringType()),
            dtypes.StructField("video", dtypes.BooleanType()),
            dtypes.StructField("vote_average", dtypes.FloatType()),
            dtypes.StructField("vote_count", dtypes.IntegerType()),
        ]
    )

    movies_meta_data_df = spark.createDataFrame(
        data=spark.sparkContext.emptyRDD(), schema=movies_schema
    )
    full_movies_meta_data_prefix = (
        movies_data_source_prefix + "/" + movies_meta_data_prefix
    )

    for movies_meta_data_object in data_bucket.objects.filter(
        Prefix=full_movies_meta_data_prefix
    ):
        movies_meta_object_uri = f"s3a://{s3_bucket_name}/{movies_meta_data_object.key}"

        print("Reading CVS files, file======" + movies_meta_object_uri)
        csv_df = (
            spark.read.option("quote", '"')
            .option("escape", '"')
            .option("header", "true")
            .schema(movies_schema)
            .csv(movies_meta_object_uri)
        )

        if movies_meta_data_df.count() > 0:
            movies_meta_data_df = movies_meta_data_df.union(csv_df)
        else:
            movies_meta_data_df = csv_df

    print(
        "movies_meta_data_df before filter Count====="
        + str(movies_meta_data_df.count())
    )
    movies_meta_data_df = movies_meta_data_df.filter(col("id").isNotNull()).dropDuplicates(["id"]).na.fill(value=0)


    print(
        "movies_meta_data_df After filter Count===="
        + str(movies_meta_data_df.count())
    )


    dim_movies_columns = [
        "id",
        "title",
        "original_title",
        "overview",
        "status",
        "release_date",
        "video",
    ]

    dim_movies_df = (
        movies_meta_data_df.select(dim_movies_columns).withColumn("last_updated", current_timestamp())
    )

    dim_movies_df.printSchema()
    print(
        "Saving dim_movies_df to redshift, Rows Count==========="
        + str(dim_movies_df.count())
    )
    write_dataframe_to_redshift(dim_movies_df, "dim_movies")


    movies_meta_data_df = movies_meta_data_df.withColumnRenamed("id", "movie_id")

    dim_movies_columns = ["movie_id", "revenue", "budget", "vote_average", "vote_count"]
    fact_movie_sales_df = (
        movies_meta_data_df.select(dim_movies_columns)
        .withColumn("last_updated", current_timestamp())
    )

    fact_movie_sales_df.printSchema()
    print(
        "Saving fact_movie_sales_df to redshift, Rows Count==========="
        + str(fact_movie_sales_df.count())
    )

    write_dataframe_to_redshift(fact_movie_sales_df, "fact_movie_sales")


    genres_keys = [
        {
            "dict_key": "id",
            "df_output_column": "genre_id",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "name", "df_output_column": "genre_name"},
    ]
    movies_genres_df = prepare_dataframe_with_json_column(
        movies_meta_data_df, "genres", genres_keys
    )

    dim_genres_df = (
        movies_genres_df.select("genre_id", "genre_name")
        .dropDuplicates(["genre_id"])
        .withColumnRenamed("genre_id", "id")
        .withColumnRenamed("genre_name", "name")
        .withColumn("last_updated", current_timestamp())
    )

    dim_genres_df.printSchema()
    print(
        "Saving dim_genres_df to redshift, Rows Count==========="
        + str(dim_genres_df.count())
    )

    write_dataframe_to_redshift(dim_genres_df, "dim_genres")

    movies_genres_df = movies_genres_df.select("movie_id", "genre_id")

    movies_genres_df.printSchema()
    print(
        "Saving movies_genres_df to redshift, Rows Count==========="
        + str(movies_genres_df.count())
    )

    write_dataframe_to_redshift(movies_genres_df, "movie_genres")

    production_company_keys = [
        {
            "dict_key": "id",
            "df_output_column": "company_id",
            "column_type": dtypes.IntegerType(),
        },
        {"dict_key": "name", "df_output_column": "company_name"},
    ]
    movies_companies_df = prepare_dataframe_with_json_column(
        movies_meta_data_df, "production_companies", production_company_keys
    )

    dim_companies_df = (
        movies_companies_df.select("company_id", "company_name")
        .dropDuplicates(["company_id"])
        .withColumnRenamed("company_id", "id")
        .withColumnRenamed("company_name", "name")
        .withColumn("last_updated", current_timestamp())
    )

    dim_companies_df.printSchema()
    print(
        "Saving dim_companies_df to redshift, Rows Count==========="
        + str(dim_companies_df.count())
    )

    write_dataframe_to_redshift(dim_companies_df, "dim_companies")

    movies_companies_df = movies_companies_df.select("movie_id", "company_id")
    movies_companies_df.printSchema()
    print(
        "Saving movies_companies_df to redshift, Rows Count==========="
        + str(movies_companies_df.count())
    )

    write_dataframe_to_redshift(movies_companies_df, "movie_production_companies")

    production_countries_keys = [
        {"dict_key": "iso_3166_1", "df_output_column": "country_code"},
        {"dict_key": "name", "df_output_column": "country_name"},
    ]
    movies_countries_df = prepare_dataframe_with_json_column(
        movies_meta_data_df, "production_countries", production_countries_keys
    )

    dim_countries_df = (
        movies_countries_df.select("country_code", "country_name")
        .dropDuplicates(["country_code"])
        .withColumnRenamed("country_code", "iso_code")
        .withColumnRenamed("country_name", "name")
        .withColumn("last_updated", current_timestamp())
    )

    dim_countries_df.printSchema()
    print(
        "Saving dim_countries_df to redshift, Rows Count==========="
        + str(dim_countries_df.count())
    )

    write_dataframe_to_redshift(dim_countries_df, "dim_countries")

    movies_countries_df = movies_countries_df.select("movie_id", "country_code")

    movies_countries_df.printSchema()
    print(
        "Saving movies_countries_df to redshift, Rows Count==========="
        + str(movies_countries_df.count())
    )

    write_dataframe_to_redshift(movies_countries_df, "movie_countries")

    spoken_languages_keys = [
        {"dict_key": "iso_639_1", "df_output_column": "language_code"},
        {"dict_key": "name", "df_output_column": "language_name"},
    ]
    movies_spoken_languages_df = prepare_dataframe_with_json_column(
        movies_meta_data_df, "spoken_languages", spoken_languages_keys
    )

    def get_language_name_from_code(str):
        return Language.make(language=str).display_name()

    get_language_english_name = udf(
        lambda x: get_language_name_from_code(x), dtypes.StringType()
    )

    dim_languages_df = (
        movies_spoken_languages_df.select("language_code", "language_name")
        .dropDuplicates(["language_code"])
        .withColumnRenamed("language_code", "iso_code")
        .withColumn("name", get_language_english_name(col("iso_code")))
        .withColumnRenamed("language_name", "original_name")
        .withColumn("last_updated", current_timestamp())
    ).orderBy("iso_code")

    dim_languages_df.printSchema()
    print(
        "Saving dim_languages_df to redshift, Rows Count==========="
        + str(dim_languages_df.count())
    )

    write_dataframe_to_redshift(dim_languages_df, "dim_languages")

    movies_spoken_languages_df = movies_spoken_languages_df.select(
        "movie_id", "language_code"
    )

    movies_spoken_languages_df.printSchema()
    print(
        "Saving movies_spoken_languages_df to redshift, Rows Count==========="
        + str(movies_spoken_languages_df.count())
    )

    write_dataframe_to_redshift(movies_spoken_languages_df, "movie_languages")


def main():

    spark = create_spark_session()
    process_movies_data(spark)


if __name__ == "__main__":
    main()
