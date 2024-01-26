from delta import configure_spark_with_delta_pip
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


def main():
    builder = SparkSession.builder\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    load_raw(spark)


def load_raw(spark: SparkSession):
    # process user data first
    df = spark.read.json("./user_test_file.json", multiLine=True)
    df.filter(col("username").isNotNull()).withColumn("dob", when(col("dob") == lit(""), lit(None).cast("string")).otherwise(col("dob"))).write.format("delta").mode("overwrite").save("./out/user")

    df = spark.read.csv("./ads_test_file.csv",header=True,schema="event_type string, timestamp string, user string, content string, ad string")
    df = df.filter(col("event_type")=="ad_completed").groupBy("user").count()
    df.write.format("delta").mode("overwrite").save("./out/user_ad_count")



if __name__ == "__main__":
    main()
