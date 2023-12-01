from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

spark = SparkSession.builder.appName("Test HBase Connector from Python").getOrCreate()
data = [("alice","alice@alice.com", datetime.strptime("2000-01-01",'%Y-%m-%d'), 4.5),
    ("bob","bob@bob.com", datetime.strptime("2001-10-17",'%Y-%m-%d'), 5.1)
  ]

schema = StructType([ \
    StructField("name",StringType(),True), \
    StructField("email",StringType(),True), \
    StructField("birthDate", DateType(),True), \
    StructField("height", FloatType(), True)
  ])
 
personDS = spark.createDataFrame(data=data,schema=schema)

personDS.write.format("org.apache.hadoop.hbase.spark").option("hbase.columns.mapping", "name STRING :key, email STRING c:email, birthDate DATE p:birthDate, height FLOAT p:height").option("hbase.table", "person").option("hbase.spark.use.hbasecontext", False).save()
spark.stop()