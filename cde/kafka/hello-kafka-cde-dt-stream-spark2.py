# using delegation token
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read/write kafka topic in streaming mode") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "test") \
    .option("startingOffsets", """{"test":{"0":-20}}""") \
    .option("kafka.bootstrap.servers", 
            "xhu-cm7101-1.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-10.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-2.xhu-cm7101.root.hwx.site:9093") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .load()
df.printSchema()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data = query.select("value")
checkpoint = "/tmp/hive/cdpuser1/checkpoint"

data.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("topic", "test") \
    .option("checkpointLocation", checkpoint) \
    .option("kafka.bootstrap.servers",
            "xhu-cm7101-1.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-10.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-2.xhu-cm7101.root.hwx.site:9093") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .start() \
    .awaitTermination()