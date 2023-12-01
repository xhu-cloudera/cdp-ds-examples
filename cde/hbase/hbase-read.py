from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test HBase Connector from Python").getOrCreate()

df = spark.read.format("org.apache.hadoop.hbase.spark").option("hbase.columns.mapping", "name STRING :key, email STRING c:email, birthDate DATE p:birthDate, height FLOAT p:height").option("hbase.table", "person").option("hbase.spark.use.hbasecontext", False).load()
df.createOrReplaceTempView("personView")
results = spark.sql("SELECT * FROM personView WHERE name = 'alice'")
results.show()
spark.stop()