package com.my.project.spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * spark-shell可以传递的参数：./bin/spark-shell --master local[4] --jars code.jar --packages "org.example:example:0.1"
 *   --master 指定master
 *   --jars 逗号分隔的jar包路径
 *   --packages 逗号分隔的maven仓库的jar包groupId:artifactId:version
 *   --repositories 指定maven仓库地址
 * 
 * RDD包含两类操作
 * 
 * transformations: 将一个RDD转换为一个新的RDD，如：map
 * actions: 在RDD上进行计算并返回一个值，如：reduce，但reduceByKey会返回一个RDD
 * 
 * transformation操作不会立即执行，只有action操作需要获得计算结果时才会真正发起计算
 * action操作每调用一次，transformation操作就会执行一次，但可以通过调用RDD的
 * persist/cache方法将RDD数据“持久化”到内存，这些数据整个集群都可以访问，从而加快后续查询的速度，而且也可以将RDD数据持久化到磁盘
 * @author yang
 */
public class HelloRDD {

	public static void main(String[] args) {
		// 首先创建JavaSparkContext
		String appName = "Hello RDD"; // 应用名称
		String master = "local[2]"; // spark、mesos、yarn集群URL, local表示本地模式
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {

			// 创建RDD的方式1: 将已经存在的集合平行化

			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
			// RDD一旦创建好，就可以以并行的方式访问和操作了
			// parallelize方法第2个参数指定将数据集切分为多少个partition, spark会为集群中每个partition运行一个task
			// 通常每个CPU分配2-4个partition比较合适，不设置的话spark会根据集群情况自动设置
			JavaRDD<Integer> distData = sc.parallelize(data, 5);
			Integer sum = distData.reduce((a, b) -> a + b);
			System.out.println("sum(1...10)=" + sum);

			// 创建RDD的方式2: 从Hadoop支持的存储源创建，包括本地文件系统、HDFS、Cassandra、HBase、Amazon S3，支持文本文件、SequenceFile以及任意的Hadoop InputFormat

			String localFile = "D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md";
			// 如果文件是本地路径，那每个worker都必须能够以相同的路径访问到该文件，可将文件复制到集群每个节点或使用共享存储
			// Spark所有基于文件的输入，都支持目录、压缩文件和通配符：
			//   textFile("/my/directory")
			//   textFile("/my/directory/*.txt")
			//   textFile("/my/directory/*.gz")
			// 同样textFile方法第二个参数用于指定partition的数量，通常Spark会为每个block(HDFS默认是128MB)创建一个partition
			// 注意: partition数量不能比block数量少
			JavaRDD<String> distFile = sc.textFile(localFile);
			// 读取目录下所有的文件，返回[ 文件名 > 文件内容 ]的K-V对
			//sc.wholeTextFiles(path);
			// 读取sequence文件，需要设置Key和Value的类型，必须是实现了Hadoop Writable接口的类型，如IntWritable, Text
			//sc.sequenceFile(path, keyClass, valueClass);
			// 将mapreduce job的输出作为输入
			//sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass);
			// 将mapreduce v2 的输出作为输入
			//sc.newAPIHadoopRDD(conf, fClass, kClass, vClass);
			// 读取序列化文件
			//javaRDD.saveAsObjectFile(path); // 将java对象序列化后存储到指定文件
			//sc.objectFile(path); // 读取java对象序列化文件

			// 将文件中每行的长度相加
			Integer size = distFile.map(s -> s.length()).reduce((a, b) -> a + b);
			System.out.println("file size: " + size);
			// word count示例
			JavaPairRDD<String, Integer> counts = distFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b);
			counts.collect().forEach(pair -> System.out.println(pair._1 + " " + pair._2));

			// 关闭SparkContext
			sc.stop();
		}
	}

}
