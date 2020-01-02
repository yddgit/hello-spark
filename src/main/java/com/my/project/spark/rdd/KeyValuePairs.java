package com.my.project.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KeyValuePairs {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Key-Value Pairs");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// 大部分Spark操作都支持RDD中保存任意类型的对象，少量特殊的操作只支持key-value对
			// 最常见就是shuffle操作，对数据元素根据key进行分组聚合操作

			// Java API中key-value对使用scala标准库中的scala.Tuple2
			// 通过new Tuple2(a, b)创建一个key-value对
			// 通过tuple._1()和tuple._2()访问key和value
			
			// key-value对的RDD对应的Java API类型是JavaPairRDD
			// 可以使用特定的map方法从一个RDD构造出一个JavaPairRDD，如：mapToPair、flatMapToPair
			// JavaPairRDD除了RDD的基本方法外还有key-value特有的方法
			
			// 如下：使用reduceByKey来统计每一行的文本在文件中出现的次数
			JavaRDD<String> lines = sc.textFile("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md");
			JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
			JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
			counts.sortByKey(); // 根据key排序
			counts.collect().forEach(pair -> System.out.println(pair._1 + " => " + pair._2));
			// 如果使用自定义类做为key-value类型，必须确保实现equals()方法及对应的hashCode()方法
		}
	}

}
