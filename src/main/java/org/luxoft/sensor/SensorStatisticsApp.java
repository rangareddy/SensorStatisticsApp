package org.luxoft.sensor;

/**
 * User: Ranga Reddy
 * Date: 9/24/2019
 * Time: 6:14 PM
 * Description:
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FilenameFilter;

public class SensorStatisticsApp {

    public static void main(String[] args) {

        if (args.length < 0) {
            System.err.println("SensorStatisticsApp - Invalid Arguments");
            System.out.println("SensorStatisticsApp - <Path_Of_Sensor_Data>");
            System.exit(0);
        }

        // configure spark
        SparkSession spark = SparkSession.builder()
                .appName("Java Sensor Statistics App")
                .master("local[2]")
                .getOrCreate();

        System.out.println("Spark Session created successfully");

        File directory = new File(args[0]);

        Dataset<Row> sensorDataset = spark.read().format("csv").option("mode", "DROPMALFORMED").
                option("header", "true").load(directory.getAbsolutePath() + "/*.csv");

        Dataset<Row> withColumnRenamed = sensorDataset.withColumnRenamed("sensor-id", "sensorId");
        withColumnRenamed.createOrReplaceTempView("sensorData");

        int processedFilesCount = getProcessedFilesCount(directory);
        long processedMeasurements = getProcessedMeasurements(sensorDataset);
        long failedMeasurements = getFailedMeasurements(sensorDataset);

        System.out.println("Num of processed files: " + processedFilesCount);
        System.out.println("Num of processed measurements: " + processedMeasurements);
        System.out.println("Num of failed measurements: " + failedMeasurements);

        System.out.println("\nSensors with highest avg humidity:");

        Dataset<Row> mindedDataset = spark.sql("SELECT sensorId, (CASE WHEN humidity != 'NaN' THEN humidity ELSE NULL END) AS humidity FROM sensorData");
        mindedDataset.createOrReplaceTempView("minedSensorData");
        Dataset<Row> sqlResult = spark.sql(
                "SELECT sensorId `sensor-id`, (CASE WHEN min_humidity IS NOT NULL THEN min_humidity ELSE 'NaN' END) AS `min`, (CASE WHEN avg_humidity !=0 THEN avg_humidity ELSE 'NaN' END) AS `avg`, (CASE WHEN max_humidity IS NOT NULL THEN max_humidity ELSE 'NaN' END) AS `max` FROM (SELECT sensorId, min(humidity) as min_humidity, cast(avg(humidity) as Long) as avg_humidity, max(humidity) as max_humidity FROM minedSensorData GROUP BY sensorId ORDER BY avg_humidity DESC)");

        sqlResult.show();
        if (spark != null) {
            spark.close();
        }
    }

    private static long getProcessedMeasurements(Dataset<Row> sensorDataset) {
        return sensorDataset.count();
    }

    private static long getFailedMeasurements(Dataset<Row> sensorDataset) {
        Dataset<Row> failedMeasurements = sensorDataset.filter("humidity = 'NaN'");
        return failedMeasurements.count();
    }

    public static int getProcessedFilesCount(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".csv");
                }
            });
            return files.length;
        }
        return 0;
    }
}
