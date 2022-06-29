-- DONE

CREATE TABLE IF NOT EXISTS  "dim_cast" (
  "id" int PRIMARY KEY,
  "name" varchar(255),
  "character" varchar,
  "gender" char(1),
  "last_updated" datetime
);


-- DONE
CREATE TABLE IF NOT EXISTS  "dim_crew" (
  "id" int PRIMARY KEY,
  "name" varchar(255),
  "department" varchar,
  "gender" char(1),
  "job" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_keywords" (
  "id" int PRIMARY KEY,
  "name" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_genres" (
  "id" int PRIMARY KEY,
  "name" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_companies" (
  "id" int PRIMARY KEY,
  "name" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_countries" (
  "iso_code" varchar(2) PRIMARY KEY,
  "name" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_languages" (
  "iso_code" varchar(2) PRIMARY KEY,
  "name" varchar,
  "original_name" varchar,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "dim_movies" (
  "id" int PRIMARY KEY,
  "title" varchar,
  "original_title" varchar,
  "overview" varchar(max),
  "status" varchar,
  "release_date" date,
  "video" boolean,
  "last_updated" datetime
);

CREATE TABLE IF NOT EXISTS  "fact_rating" (
  "movie_id" int PRIMARY KEY,
  "ratings_count" int,
  "ratings_average" float,
  "min_rate" float,
  "max_rate" float,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "fact_movie_sales" (
  "movie_id" int PRIMARY KEY,
  "revenue" float,
  "budget" float,
  "vote_average" float,
  "vote_count" int,
  "last_updated" datetime
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_cast" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "cast_id" int
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_crew" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "crew_id" int
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_genres" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "genre_id" int
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_keywords" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "keyword_id" int
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_production_companies" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "company_id" int
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_languages" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "language_code" varchar(2)
);

-- DONE
CREATE TABLE IF NOT EXISTS  "movie_countries" (
  "id" BIGINT IDENTITY(1,1) PRIMARY KEY,
  "movie_id" int,
  "country_code" varchar(2)
);

ALTER TABLE "movie_cast" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_cast" ADD FOREIGN KEY ("cast_id") REFERENCES "dim_cast" ("id");

ALTER TABLE "movie_crew" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_crew" ADD FOREIGN KEY ("crew_id") REFERENCES "dim_crew" ("id");

ALTER TABLE "movie_genres" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_genres" ADD FOREIGN KEY ("genre_id") REFERENCES "dim_genres" ("id");

ALTER TABLE "movie_keywords" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_keywords" ADD FOREIGN KEY ("keyword_id") REFERENCES "dim_keywords" ("id");

ALTER TABLE "movie_countries" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_countries" ADD FOREIGN KEY ("country_code") REFERENCES "dim_countries" ("iso_code");

ALTER TABLE "movie_languages" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_languages" ADD FOREIGN KEY ("language_code") REFERENCES "dim_languages" ("iso_code");

ALTER TABLE "movie_production_companies" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "movie_production_companies" ADD FOREIGN KEY ("company_id") REFERENCES "dim_companies" ("id");

ALTER TABLE "fact_movie_sales" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");

ALTER TABLE "fact_rating" ADD FOREIGN KEY ("movie_id") REFERENCES "dim_movies" ("id");
