package com.my.project.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 常用的action操作
 */
public class Actions {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Actions");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String> rdd = sc.textFile("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md");
			// 1. reduce 对集合元素进行聚合运算，两个输入参数一个输出参数，需要考虑并行计算时的正确性
			Integer totalLength = rdd.map(line -> line.length()).reduce((a, b) -> a + b);
			// 2. collect 以数组的形式返回集合中所有元素到driver程序中
			List<String> content = rdd.collect();
			// 3. count 返回集合中元素的个数
			long count = rdd.count();
			// 4. first 返回集合中第1个元素
			String firstLine = rdd.first();
			// 5. take(n) 返回集合中前n个元素
			List<String> firstTenLine = rdd.take(10);
			// 6. takeSample 随机获取集合中n个元素的随机样本
			List<String> sample = rdd.takeSample(false, 10);
			// 7. takeOrdered 获取集合排序后的前n个元素
			List<String> ordered = rdd.takeOrdered(10);
			// 8. saveAsTextFile 将集合中的元素写到一个（或多个）文本文件，Spark会调用每个元素的toString()方法，将返回值写为文件中的一行
			rdd.saveAsTextFile("/path/to/local/OR/hdfs/OR/hadoop/supported/file/system");
			// 9. saveAsSequenceFile 将集合中的元素写到sequence文件中
			//rdd.saveAsSequenceFile(); // not found ?
			// 10.saveAsObjectFile 将集合中的元素以java的序列化方式写到文件中，可以使用SparkContext.objectFile()加载
			rdd.saveAsObjectFile("/path/to/storage");
			// 11.countByKey 根据key分组计数, K-V return K-Integer
			//rdd.countByKey(); // not found ?
			// 12.foreach 对集合的每个元素运行指定的函数，通常用于更新Accumulator或和外部存储系统进行交互
			// 注意不要在foreach循环外修改Accumulator的值
			// Spark还提供了一些action的异步版本，如foreachAsync，它会立即返回FutureAction给调用方不会阻塞
			// 这可用于管理和等待操作的异步执行
		}
	}

}
