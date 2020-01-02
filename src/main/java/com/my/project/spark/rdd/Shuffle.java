package com.my.project.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Shuffle {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Shuffle Operations");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// Spark中的某些操作会触发一个shuffle事件
			// shuffle是Spark重新分发数据的一种机制，它可以跨分区进行不同的group操作
			// 显示这会导致跨executor和跨节点的数据拷贝，因此shuffle是一个复杂且代价高昂的操作

			// 这里以reduceByKey为例：
			// reduceByKey会产生一个新的RDD，相同key对应的值被合并到一个tuple中，然后对合并后的所有值应用reduce函数
			// 这里的问题是，每个key对应的值并非都在同一个partition中，甚至都不在同一个节点上，但是必须找到这些value以计算最终结果

			// 在Spark中，针对特定操作的需要，数据通常不会跨partition分布
			// 计算过程中，一个partition上只会有一个任务，因此为了把数据组织好，以例reduceByKey的reduce任务可以执行
			// Spark需要执行一个all-to-all的操作，它必须读取所有partition上的所有key-value
			// 然后跨partition把相同key的value整合到一起，并为每个key计算最终结果，这一操作就称为shuffle

			// 虽然shuffle后的数据每个partition里的数据都是确定的
			// 而且partition的排序也是确定的，但其中的元素排序却并非如此
			// 如果需要shuffle后的数据是有序的，可以用
			// 1. mapPartitions，在拿到分区数据后调用sorted方法
			// 2. repartitionAndSortWithinPartitions，更高效，在重新分区的时候直接排序
			// 3. sortBy，对整个RDD进行排序

			// 会导致shuffle的操作有
			// 1. repartition类的: repartition/coalesce
			// 2. xxByKey类的: groupByKey/reduceByKey
			// 3. join类的: cogroup/join

			// 性能问题
			// shuffle是一个代价高昂的操作，会引入磁盘IO、数据序列化和网络IO
			// 为了shuffle操作能组织好数据，Spark会产生一批map任务组织数据一批reduce任务聚合数据
			// 这里的mapreduce指的是hadoop里的mapreduce操作，不是Spark里的map和reduce操作
			
			// 在内部，各个map任务的结果也保留在内存当中直到其不再适用
			// 之后这些结果会根据目标partition进行排序并写入单个文件
			// 在reduce任务中则直接读取相关的已排好序的数据块
			
			// 某些shuffle操作会消耗大量的堆内存，因为它使用内存中的数据结构在传输之前或之后来组织数据记录
			// 具体的，reduceByKey和aggregateByKey在map任务中创建数据，reduce任务中生成数据
			// 当内存不足时，Spark会将数据写到磁盘，这会导致额外的磁盘IO，增加垃圾回收次数

			// shuffle也会在磁盘上产生大量的中间结果文件
			// Spark 1.3之后，这些文件会一直保留直到对应的RDD不再使用或被当成垃圾回收
			// 这样，在重新计算时shuffle文件就不需要再创建了
			// 如果应用保持这些RDD的引用或者没有频繁的GC发生，GC可能要等很长一段时间
			// 这意味着长时间运行的Spark Job会消耗大量的磁盘空间，SparkContext可以使用spark.local.dir配置临时存储目录
			// shuffle优化可以调整很多配置参数，具体可以查看Spark的配置指南中的Shuffle Behavior一节
			// http://spark.apache.org/docs/latest/configuration.html#shuffle-behavior
		}
	}

}
