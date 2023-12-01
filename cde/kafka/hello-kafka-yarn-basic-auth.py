from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read kafka topic in batch mode").getOrCreate()

df = spark.read.format("kafka").option("kafka.bootstrap.servers", "xhu-cm7101-1.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-10.xhu-cm7101.root.hwx.site:9093,xhu-cm7101-2.xhu-cm7101.root.hwx.site:9093").option("subscribe", "test").option("kafka.security.protocol", "SASL_SSL").option("kafka.sasl.mechanism", "PLAIN").option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="kafka" username="cdpuser1" password="Test123";').option("kafka.ssl.truststore.location", "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks").load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.show()