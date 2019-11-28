package com.my.project.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

/**
 * bin/spark-submit --class "com.my.project.spark.SimpleApp" --master local[4] hello-spark-0.0.1.jar
 */
public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md"; // Should be some file on your system
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		Dataset<String> logData = spark.read().textFile(logFile).cache();

		long numAs = logData.filter(s -> s.contains("a")).count();
		long numBs = logData.filter(s -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

		spark.stop();
	}
}