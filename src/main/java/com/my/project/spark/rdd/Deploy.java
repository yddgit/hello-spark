package com.my.project.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Deploy {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Deploy");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// 提交应用到集群可以参考文档：http://spark.apache.org/docs/latest/submitting-applications.html
			// 简单来说，Java/Scala打成jar包，Python是一组.py文件或打成zip包，然后用bin/spark-submit脚本提交到集群即可

			// 对Java/Scala来说，org.apache.spark.launcher包下提供的类可以使用Java API以子进程的方式来运行Spark Job
			// http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/launcher/package-summary.html

			// Spark很容易做单元测试，只需要创建一个SparkContext，将master URL设成local，然后执行操作，最后调用SparkContext.stop()结束即可
			// stop()方法必须在finally代码块或单元测试框架的tearDown方法中
			// Spark不支持同一个程序中同时存在两个context

			// 优化Spark程序可以参考以下文档
			// http://spark.apache.org/docs/latest/configuration.html
			// http://spark.apache.org/docs/latest/tuning.html
			// 另外，很重要的一点是确保数据是以有效的格式存储在内存当中的
			// 集群环境：http://spark.apache.org/docs/latest/cluster-overview.html

			// API Documentation
			// Scala: http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.package
			// Java: http://spark.apache.org/docs/latest/api/java/
		}
	}

}
