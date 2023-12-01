# using delegation token
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read kafka topic in streaming mode") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "test") \
    .option("kafka.bootstrap.servers", 
            "xhu-cm7101-1.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-10.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-2.xhu-cm7101.root.hwx.site:9093") \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.scram.ScramLoginModule required debug=true;') \
    .load()
df.printSchema()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data = query.select("value")
data.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()