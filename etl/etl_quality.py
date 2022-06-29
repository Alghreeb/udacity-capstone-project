from spark_session import create_spark_session
from utils import (
    read_table_from_redshift
)



def quality_check(spark):

    print("Quality Check Started")
    dim_movies_table = "dim_movies"
    dim_movies_df = read_table_from_redshift(spark, dim_movies_table)
    dim_movies_df.createOrReplaceTempView("movies_view")
    movies_check = spark.sql("SELECT * FROM movies_view WHERE id is null")
    if movies_check.count() > 0:
        raise ValueError('dim_movies should not have null values in column ID')
   

    dim_languages_table = "dim_languages"
    dim_languages_df = read_table_from_redshift(spark, dim_languages_table)
    dim_languages_df.createOrReplaceTempView("languages_view")
    languages_check = spark.sql("SELECT * FROM languages_view WHERE name is null")
    if languages_check.count() > 0:
        raise ValueError('dim_languages should not have null values in column ID')

    
    fact_movie_sales_table = "fact_movie_sales"
    fact_movie_sales_df = read_table_from_redshift(spark, fact_movie_sales_table)
    fact_movie_sales_df.createOrReplaceTempView("movie_sales_view")
    movie_sales_check = spark.sql("SELECT * FROM movie_sales_view where revenue > 0")
    if movie_sales_check.count() <= 0:
        raise ValueError('fact_movie_sales should have revenues > 0')

    print("Quality check Ended")

def main():
    spark = create_spark_session()
    quality_check(spark)


if __name__ == "__main__":
    main()
