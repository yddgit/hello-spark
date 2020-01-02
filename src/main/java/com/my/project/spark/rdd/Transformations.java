package com.my.project.spark.rdd;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 常用的transformation操作
 */
public class Transformations {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Transformation");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String> rdd = sc.textFile("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md");
			// 1. map 返回每个元素经过指定函数后计算产生的新的集合
			JavaRDD<Integer> lineLengths = rdd.map(line -> line.length());
			// 2. filter 返回经过指定函数过滤后产生的新的集合
			JavaRDD<String> noEmptyLines = rdd.filter(line -> line.length() > 0);
			// 3. flatMap 返回每个元素经过指定函数计算后产生的新的集合，每个元素可能对应产生0个或多个输出
			JavaRDD<String> words = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
			// 4. mapPartitions 与map类似，但不是以元素为单位的，而是以partition为单位的计算，所以map函数的输入和输出都是Iterator
			JavaRDD<Integer> lineLengthsByMapPartitions = rdd.mapPartitions(p -> {
				Iterable<String> iterable = () -> p;
				Stream<String> stream = StreamSupport.stream(iterable.spliterator(), false);
				return stream.map(s -> s.length()).iterator();
			});
			// 5. mapPartitionsWithIndex 与mapPartitions类似，但map函数提供一个int类型的参数表示partition的索引
			JavaRDD<Integer> lineLengthsByMapPartitionsWithIndex = rdd.mapPartitionsWithIndex((index, p) -> {
				Iterable<String> iterable = () -> p;
				Stream<String> stream = StreamSupport.stream(iterable.spliterator(), false);
				return stream.map(s -> s.length()).iterator();
			}, true);
			// 6. sample 使用给定的随机数生成器种子，从数据集中根据有或没有替换进行抽样
			List<String> sample = rdd.sample(false, 0.2).take(10);
			// 7. union 合并两个集合
			JavaRDD<String> doubleLine = rdd.union(sc.textFile("/path/to/other/file"));
			// 8. intersection 取交集
			JavaRDD<String> intersection = rdd.intersection(sc.textFile("/path/to/other/file"));
			// 9. distinct 去重
			JavaRDD<String> distinct = rdd.distinct();
			// 10.groupByKey 在一个K-V对的集合上调用，返回K-Iterable<V>对的新集合
			// 注意如果要在groupBy之后进行聚合运算，应该使用reduceByKey或aggregateByKey
			// 默认的并行度依赖于父RDD的partition数量，可以在方法参数中指定新的partition数量
			JavaPairRDD<String, Iterable<Integer>> lineToLengths = rdd.mapToPair(line -> new Tuple2<String, Integer>(line, line.length())).groupByKey();
			// 11.reduceByKey 对K-V对的集合按照key进行分组聚合运算
			JavaPairRDD<String, Integer> wordcount = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey((a, b) -> a + b);
			// 12.aggregateByKey 对K-V对的集合按照key进行分组聚合运算
			//rdd.mapToPair(line -> new Tuple2<String, Integer>(line, line.length())).aggregateByKey(zeroValue, seqFunc, combFunc);
			// 13.sortByKey 对集合进行排序
			JavaPairRDD<String, Integer> lineLengthsOrdered = rdd.mapToPair(line -> new Tuple2<String, Integer>(line, line.length())).sortByKey(true);
			// 14.join 对两个集合进行join操作，外连接支持leftOuterJoin/rightOuterJoin/fullOuterJoin
			// K-V join K-W return K-(V,W)
			//rdd.mapToPair(line -> new Tuple2<String, Integer>(line, line.length())).join(otherPairRDD);
			// 15.cogroup K-V cogroup K-W return K-(Iterable<V>, Iterable<W>)，这一操作又称为groupWith
			// 16.cartesian 笛卡尔积 T cartesian U return (T,U) (所有的T,U对)
			// 17.pipe 将RDD输出到管道，并从管道中获取结果再构造成新的RDD
			JavaRDD<String> newRDD = rdd.pipe("command.sh");
			// 18.coalesce 减少partition数量到指定的数量，通常用于从一个大数据集中过滤完数据后提升运行效率
			JavaRDD<String> decrease = rdd.coalesce(4);
			// 19.repartition 重新分发数据，随机的创建或减少partition达到均衡，这通常会在网络中分发所有的数据
			JavaRDD<String> repartition = rdd.repartition(4);
			// 20.repartitionAndSortWithinPartitions 重新根据指定的partitioner对RDD进行分区，每个分区根据key进行排序
			// 这比先调用repartition，然后再排序的效率要高，因为排序会被推送到分发的机器上
		}
	}

}
