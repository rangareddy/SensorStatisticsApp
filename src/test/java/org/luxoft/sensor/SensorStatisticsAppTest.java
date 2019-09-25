package org.luxoft.sensor;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 * User: Ranga Reddy
 * Date: 9/25/2019
 * Time: 8:02 AM
 * Description:
 */

public class SensorStatisticsAppTest {
    private static SparkSession spark;
    private static String directoryPath = "./src/main/resources";
    private static File directory;
    private SensorStatisticsApp app = new SensorStatisticsApp();

    @BeforeClass
    public static void init() {
        spark = SparkSession.builder()
                .appName("Java Sensor Statistics App")
                .master("local[2]")
                .getOrCreate();
        directory = new File(directoryPath);
        Assert.assertTrue(directory.exists());
    }

    @Test
    public void testFilesCount() {
        Assert.assertEquals(2, app.getProcessedFilesCount(directory));
    }

    @AfterClass
    public static void destroy() {
        if (spark != null) {
            spark.close();
        }
    }
}
