package com.my.project.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HelloSQL {
    public static void main(String[] args) {
        try(SparkSession spark = SparkSession.builder().appName("Hello SQL").config("spark.some.config.option", "some-value").getOrCreate()) {
            // Spark SQL是用于处理结构化数据的Spark模块

            // Spark SQL可以与Hive集成(https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
            // 在其他语言中执行Spark SQL将返回Dataset/DataFrame, 也可以通过命令行或JDBC/ODBC来执行Spark SQL

            // Dataset同时提供了RDD的优势和Spark优化后的执行引擎的优势
            // DataFrame类似数据库表, 可以基于结构化数据文件、Hive表、数据库或RDD来创建DataFrame
            // DataFrame是Dataset行的数据集, Java API中使用Dataset<Row>来表示

            // 基于json文件创建DataFrame
            Dataset<Row> df = spark.read().json("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\people.json");
            df.show();
        }
    }
}
