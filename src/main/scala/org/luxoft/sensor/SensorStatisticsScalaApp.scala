package org.luxoft.sensor

import java.io.File

import org.apache.spark.sql.{Dataset, Row, SparkSession}

// java -cp scala-library.jar -jar test2.jar

object SensorStatisticsScalaApp {
  def main(args: Array[String]): Unit = {

    //9490077621
    if(args.length == 0) {
      System.err.println("SensorStatisticsApp - Invalid Arguments")
      System.err.println("SensorStatisticsApp - <Path_Of_Sensor_Data>")
      System.exit(0)
    }

    // configure spark
    val spark = SparkSession.builder.appName("Sensor Statistics Scala App").master("local[2]").getOrCreate
    println("Spark Session created successfully")

    val directory = new File(args(0))
    val sensorDataset = spark.read.format("csv").option("mode", "DROPMALFORMED").option("header", "true").load(directory.getAbsolutePath + "/*.csv")
    val withColumnRenamed = sensorDataset.withColumnRenamed("sensor-id", "sensorId")
    withColumnRenamed.createOrReplaceTempView("sensorData")
    val nanDataset = spark.sql("SELECT humidity FROM sensorData WHERE humidity = 'NaN'")

    println("Num of processed files: " + directory.listFiles.length)
    println("Num of processed measurements: " + sensorDataset.count)
    println("Num of failed measurements: " + nanDataset.count)
    println("Sensors with highest avg humidity:")

    val mindedDataset = spark.sql("SELECT sensorId, (CASE WHEN humidity != 'NaN' THEN humidity ELSE NULL END) AS humidity FROM sensorData")
    mindedDataset.createOrReplaceTempView("minedSensorData")
    val sqlResult = spark.sql("SELECT sensorId `sensor-id`, (CASE WHEN min_humidity IS NOT NULL THEN min_humidity ELSE 'NaN' END) AS `min`, (CASE WHEN avg_humidity !=0 THEN avg_humidity ELSE 'NaN' END) AS `avg`, (CASE WHEN max_humidity IS NOT NULL THEN max_humidity ELSE 'NaN' END) AS `max` FROM (SELECT sensorId, min(humidity) as min_humidity, cast(avg(humidity) as Long) as avg_humidity, max(humidity) as max_humidity FROM minedSensorData GROUP BY sensorId ORDER BY avg_humidity DESC)")

    sqlResult.show()
    spark.close()
  }
}
