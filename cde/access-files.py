from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    print("list of databases:\n{}".format(spark.catalog.listDatabases()))
    print("the user is {}".format(spark.sparkContext.sparkUser()))
    db = "spark_sql_location_{}".format(spark.sparkContext.sparkUser())
    spark.sql("DROP DATABASE if exists {}".format(db)).show(truncate=False)
    spark.sql("CREATE DATABASE if not exists {}".format(db)).show(truncate=False)
    spark.sql("DESCRIBE DATABASE EXTENDED {}".format(db)).show(truncate=False)
    print("list of databases:\n{}".format(spark.catalog.listDatabases()))
    df = spark.read.text("hdfs:///tmp/example-data/access-logs.txt")
    df.show()
    spark.stop()

if __name__ == "__main__":
    main()