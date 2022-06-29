import os
import configparser
from pathlib import Path
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime



path = Path(__file__)
workspace_dir = os.path.dirname(path)
config_path = os.path.join(workspace_dir, "configs.cfg")

config = configparser.ConfigParser()
config.read(config_path)


cluster_id = config.get("AWS", "EMR_CLUSTER_ID")



default_args = {
    "owner": "ETL_USER",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

load_movies_task = [
    {
        "Name": "movies_etl",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars",
                "/home/hadoop/redshift-jdbc42-2.1.0.8.jar",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0",
                "/home/hadoop/project/etl/movies_etl.py",
                "--deploy-mode",
                "cluster",
                "--config",
                "/home/hadoop/project/configs.cfg",
            ],
        },
    }
]

load_keywords_task = [
    {
        "Name": "keywords_etl",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars",
                "/home/hadoop/redshift-jdbc42-2.1.0.8.jar",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0",
                "/home/hadoop/project/etl/keywords_etl.py",
                "--deploy-mode",
                "cluster",
                "--config",
                "/home/hadoop/project/configs.cfg",
            ],
        },
    }
]

load_credits_task = [
    {
        "Name": "credits_etl",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars",
                "/home/hadoop/redshift-jdbc42-2.1.0.8.jar",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0",
                "/home/hadoop/project/etl/credits_etl.py",
                "--deploy-mode",
                "cluster",
                "--config",
                "/home/hadoop/project/configs.cfg",
            ],
        },
    }
]

load_ratings_task = [
    {
        "Name": "ratings_etl",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars",
                "/home/hadoop/redshift-jdbc42-2.1.0.8.jar",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4,com.databricks:spark-redshift_2.11:2.0.1,com.databricks:spark-csv_2.11:1.5.0",
                "/home/hadoop/project/etl/ratings_etl.py",
                "--deploy-mode",
                "cluster",
                "--config",
                "/home/hadoop/project/configs.cfg",
            ],
        },
    }
]


quality_check = [
    {
        "Name": "quality_check",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jars",
                "/home/hadoop/postgresql-42.2.6.jar",
                "/home/hadoop/project/etl/etl_quality.py",
                "--deploy-mode",
                "cluster",
                "--config",
                "/home/hadoop/project/configs.cfg",
            ],
        },
    }
]


with DAG(
    dag_id="movies_etl_dag",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=5),
    start_date=datetime(2022, 1, 1),
    schedule_interval='@once',
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")
    end_operator = DummyOperator(task_id="Stop_execution")
    trigger_movies_etl = EmrAddStepsOperator(
        task_id="trigger_movies_etl",
        job_flow_id=cluster_id,
        aws_conn_id="aws_default",
        steps=load_movies_task,
        )

    watch_movies_etl = EmrStepSensor(
        task_id="watch_movies_etl",
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='trigger_movies_etl', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        )

    trigger_keywords_task = EmrAddStepsOperator(
            task_id="trigger_keywords_etl",
            job_flow_id=cluster_id,
            aws_conn_id="aws_default",
            steps=load_keywords_task,
        )

    watch_keywords_etl = EmrStepSensor(
            task_id="watch_keywords_etl",
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='trigger_keywords_etl', key='return_value')[0] }}",
            aws_conn_id="aws_default",
        )

    trigger_credits_task = EmrAddStepsOperator(
            task_id="trigger_credits_etl",
            job_flow_id=cluster_id,
            aws_conn_id="aws_default",
            steps=load_credits_task,
        )

    watch_credits_etl = EmrStepSensor(
            task_id="watch_credits_etl",
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='trigger_credits_etl', key='return_value')[0] }}",
            aws_conn_id="aws_default",
        )

    trigger_ratings_task = EmrAddStepsOperator(
            task_id="trigger_ratings_etl",
            job_flow_id=cluster_id,
            aws_conn_id="aws_default",
            steps=load_ratings_task,
        )

    watch_ratings_etl = EmrStepSensor(
            task_id="watch_ratings_etl",
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='trigger_ratings_etl', key='return_value')[0] }}",
            aws_conn_id="aws_default",
        )


    quality_task = EmrAddStepsOperator(
            task_id="quality_check",
            job_flow_id=cluster_id,
            aws_conn_id="aws_default",
            steps=quality_check,
        )

    watch_quality_task = EmrStepSensor(
            task_id="watch_quality_task",
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='quality_check', key='return_value')[0] }}",
            aws_conn_id="aws_default",
        )


    start_operator >> trigger_movies_etl >> watch_movies_etl
    watch_movies_etl >> trigger_keywords_task >> watch_keywords_etl
    watch_movies_etl >> trigger_credits_task >> watch_credits_etl
    watch_movies_etl >> trigger_ratings_task >> watch_ratings_etl
    [watch_keywords_etl, watch_credits_etl, watch_ratings_etl] >> quality_task >> watch_quality_task
    watch_quality_task >> end_operator
