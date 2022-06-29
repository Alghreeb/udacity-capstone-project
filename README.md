# Movies DWH Pipeline
### Data Engineering Capstone Project

#### Project Summary
The goal of this project to have ETL pipeline for movies data. Which extract movies dataset from S3 bucket and ingest it to DWH.  


### Scope the Project and Gather Data

#### Scope 

The goal of the project is to have DWH which includes movies data to be able to answer some analytical questions at the end like:
 * How many movies were released in 2020 in non English languages?
 * The number of females were involved in movies between 1990-2000?
 * How much money did the German production companies earn between 2010-2015 from action movies?


The data used in this project is coming from https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset

 * credits.csv : contains the cast and crew information.
 * keywords.csv : conatins the keywords which each movie has.
 * movies_metadata.csv : conatins information about the movies.
 * ratings.csv : conatins users rating on rach movie.


In the project I used:
 * AWS S3: The CSV file will be stored in S3 bucket and the bucket will be used as aout data source.
 * AWS EMR: is used as our server less compute power. It allows us to use the power of Spark with the ability to 
 increase clusters on demand.
 * Apache Airflow: is used or ETL jobs orchestration.
 * AWS Redshift: Serverless DWH with column oriented databases.

