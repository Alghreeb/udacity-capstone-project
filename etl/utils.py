import os
import configparser
from pathlib import Path

import pyspark.sql.types as dtypes


from pyspark.sql.functions import (
    from_json,
    explode,
    col,
)


def get_configs():

    path = Path(__file__)
    workspace_dir = os.path.dirname(os.path.dirname(path))
    config_path = os.path.join(workspace_dir, "configs.cfg")

    config = configparser.ConfigParser()
    config.read(config_path)

    return config


def write_dataframe_to_redshift(input_dataframe, target_table):

    config = get_configs()
    cluster_name = config.get("REDSHIFT", "CLUSTER_NAME")
    db_name = config.get("REDSHIFT", "DATABASE_NAME")
    port = config.get("REDSHIFT", "PORT")
    db_user = config.get("REDSHIFT", "DATABASE_USER")
    db_password = config.get("REDSHIFT", "DATABASE_PASSWORD")
    schema = config.get("REDSHIFT", "TARGET_SCHEMA")

    tmp_bucket = f"""s3n://{config.get("S3", "AWS_S3_BUCKET")}/tmp"""
    input_dataframe.write.format("com.databricks.spark.redshift") \
    .option("url", f"""jdbc:redshift://{cluster_name}:{port}/{db_name}?user={db_user}&password={db_password}""",) \
    .option("dbtable", f"""{schema}.{target_table}""") \
    .option("tempdir", tmp_bucket) \
    .mode("append") \
    .save()



def read_table_from_redshift(spark ,table):

    config = get_configs()
    cluster_name = config.get("REDSHIFT", "CLUSTER_NAME")
    db_name = config.get("REDSHIFT", "DATABASE_NAME")
    port = config.get("REDSHIFT", "PORT")
    db_user = config.get("REDSHIFT", "DATABASE_USER")
    db_password = config.get("REDSHIFT", "DATABASE_PASSWORD")
    schema = config.get("REDSHIFT", "TARGET_SCHEMA")

    df = spark.read.format("jdbc").options(
        url=f"""jdbc:postgresql://{cluster_name}:{port}/{db_name}""",
        driver="org.postgresql.Driver",
        dbtable=f"""{schema}.{table}""",
        user=db_user,
        password=db_password,
    ).load()

    return df



def prepare_dataframe_with_json_column(input_df, selected_column, dict_keys):
    prepared_df = input_df.withColumn(
        "converted_array",
        from_json(selected_column, dtypes.ArrayType(dtypes.StringType())),
    )

    prepared_df = prepared_df.withColumn(
        "converted_dictionary",
        explode(prepared_df.converted_array),
    )

    prepared_df = prepared_df.withColumn(
        "converted_map",
        from_json(
            "converted_dictionary",
            dtypes.MapType(dtypes.StringType(), dtypes.StringType()),
        ),
    )

    for dict_key in dict_keys:
        prepared_df = prepared_df.withColumn(
            dict_key["df_output_column"],
            prepared_df.converted_map.getItem(dict_key["dict_key"]),
        )
        if "column_type" in dict_key:
            prepared_df = prepared_df.withColumn(
                dict_key["df_output_column"],
                col(dict_key["df_output_column"]).cast(dict_key["column_type"]),
            )

    prepared_df = prepared_df.drop(
        *["converted_array", "converted_dictionary", "converted_map"]
    )
    return prepared_df
