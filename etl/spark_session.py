from pyspark.sql import SparkSession
from pyspark import SparkConf
from utils import get_configs


def create_spark_session():

    conf = SparkConf().setAll(
        [
            ("spark.pyspark.virtualenv.enabled", "true"),
            ("spark.pyspark.virtualenv.bin.path", "/usr/bin/virtualenv"),
            ("spark.pyspark.python", "python3"),
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"),
        ]
    )

    spark = (
        SparkSession.builder.config(conf=conf).appName("CapStoneProject").getOrCreate()
    )
    spark.sparkContext.install_pypi_package("boto3", "https://pypi.org/simple")
    spark.sparkContext.install_pypi_package("langcodes", "https://pypi.org/simple")
    spark.sparkContext.install_pypi_package("language-data", "https://pypi.org/simple")


    config = get_configs()
    aws_key_id = config.get("AWS", "AWS_KEY_ID")
    aws_key_secret = config.get("AWS", "AWS_KEY_SECRET")

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_key_id)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_key_secret)

    return spark
