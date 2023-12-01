package com.cloudera.cde.spark

import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

  private def writeToKafka[T](): Unit = {
    val now = LocalDateTime.now()
    val timestamp = DateTimeFormatter.ofPattern("MMddHHmmss").format(now)

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Streaming")
      .config("spark.sql.warehouse.dir", "/tmp/hive/pankaj/warehouse_" + timestamp)
      .getOrCreate()

    val df = spark.readStream.text("/tmp/hive/pankaj/input")
    df.printSchema()

    var outputTopic = "spark-kafka"
    var checkpoint = "/tmp/hive/pankaj/checkpoint_" + outputTopic + "_" + timestamp
    var brokers = "tparimi781-2.tparimi781.root.hwx.site:9093,tparimi781-1.tparimi781.root.hwx.site:9093,tparimi781-3.tparimi781.root.hwx.site:9093"

    val dataStreamWriter = df.writeStream
      .format("kafka")
      .option("checkpointLocation", checkpoint)
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", outputTopic)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.ssl.truststore.location", "/usr/lib/jvm/jre/lib/security/cacerts")
      .option("kafka.ssl.truststore.password", "changeit")

      .option("includeHeaders", "true")
      .start().awaitTermination()
  }
}