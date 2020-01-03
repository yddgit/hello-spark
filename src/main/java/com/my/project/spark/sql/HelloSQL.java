package com.my.project.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

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

            // DataFrame提供了一种DSL来操作结构化数据(Scala/Java/Python/R)

            // 输出schema
            df.printSchema();
            // 只查询name列
            df.select("name").show();
            // age + 1
            df.select(col("name"), col("age").plus(1)).show();
            // age > 21
            df.filter(col("age").gt(21)).show();
            // group by age
            df.groupBy("age").count().show();
            // DataFrame的更多操作可以参考API文档: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html

            // 使用sql函数直接执行SQL语句
            df.createOrReplaceTempView("people");
            Dataset<Row> sqlDF = spark.sql("select * from people");
            sqlDF.show();

            // Temporary View在Spark SQL中的会话级别的, 会话结束, Temporary View也会随之消失
            // 如果想在会话间共享view直到Spark应用结束, 可以创建一个Global Temporary View, 它会被绑定到一个系统保留的数据库global_temp上
            // 因此需要用全名来引用它, 如: select * from global_temp.view1
            df.createOrReplaceGlobalTempView("people");
            spark.sql("select * from global_temp.people").show();
            spark.newSession().sql("select * from global_temp.people").show();
        }
    }
}
