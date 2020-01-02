package com.my.project.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Persistence {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Persistence");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// Spark可以大不同操作之间持久化或缓存数据集到内存当中
			// 当一个RDD被持久化之后，每个节点会存储其在内存中的所有partition
			// 并在该数据集（或从中派生的数据集）的其他操作中复用它们
			// 这就使得之后的操作更快（通常是10倍以上）
			// 缓存是迭代算法和快速交互式运算中使用的关键工具
			
			// 可以通过调用persist()或cache()方法持久化一个RDD
			// 当这个RDD在一个action操作中第一次运行后，就会保留在节点的内存当中
			// Spark的cache是支持容错的，RDD中任何partition丢失，都会自动使用创建它时的transformation操作重新计算一下

			// 另外，每个持久化的RDD，都可以有不同的存储级别
			// 如：持久化到磁盘、java对象序列化后持久化到内存（可以节省空间）、在节点间复制等
			// 这些级别通常通过向persist()方法传递一个StorageLevel的参数来指定
			// cache()方法是使用默认存储级别的简化写法（StorageLevel.MEMORY_ONLY，存储未序列化的对象到内存）
			// StorageLevel包括：
			// 1. MEMORY_ONLY，将未序列化的java对象保存到JVM中，如果内存不足，一部分partition将不会被cache，而是在每次用到时重新计算。这也是默认的存储级别
			// 2. MEMORY_AND_DISK，将未序列化的java对象保存到JVM中，如果内存不足，则将剩余的partition写到磁盘，并在需要的时候去磁盘上读取
			// 3. MEMORY_ONLY_SER，将序列化后的java对象保存到内存，即每个partition一个字节数组，这通常比未序列化省空间，但读取时CPU负载会高一些
			// 4. MEMORY_AND_DISK_SER，与MEMORY_ONLY_SER类似，但超出内存部分的数据会写到磁盘上，在需要的时候读取并重新计算
			// 5. DISK_ONLY，将RDD的partition只存储到磁盘
			// 6. MEMORY_ONLY_2/MEMORY_AND_DISK_2，和前面对应的存储级别一样，但是会将每个partition复制到两个集群结点
			// 7. OFF_HEAP，与MEMORY_ONLY_SER类似，但会将数据存储到堆外内存中，这需要堆外内存是开启的
			// Python中序列化对象使用的是Pickle，所以选择序列化级别不会生效，Python中支持的存储级别有：
			// MEMORY_ONLY、MEMORY_ONLY_2、MEMORY_AND_DISK、MEMORY_AND_DISK_2、DISK_ONLY、DISK_ONLY_2
			
			// Spark也会自动持久化shuffle操作的中间结果即使用户不调用persist()方法
			// 这样做是为了避免shuffle过程中一旦一个节点失败，整个输入都重新计算的问题
			// 但推荐的还是在准备复用RDD之前调用一下其persist方法
			
			// Spark的存储级别在内存使用和CPU效率上提供了不同的权衡，建议通过以下方式来选择存储级别
			// 1. 如果RDD使用默认存储级别就很好，那就不要管它，这是一种使CPU效率最高的选项，允许RDD的操作尽可能快地运行
			// 2. 如果默认存储级别不行，尝试用MEMORY_ONLY_SER，然后选择一个快速序列化库，使得对象更节省空间，但访问速度仍然很快
			// 3. 不要把数据写到磁盘，除非数据集上的计算函数很耗资源，或者计算函数会过滤出大量的数据。不然的话，重新计算一个partition可能和与从磁盘读取它一样快
			// 4. 如果想要快速的故障恢复，应该使用replicated存储级别（例如：使用Spark来处理Web应用程序的请求时）。
			// 所有的存储级别都通过重新计算丢失的数据，提供了完全容错，但是replicated存储级别允许继续在RDD上运行任务，而无需等待重新计算丢失的partition
			
			// Spark自动监控每个节点上缓存的使用情况，并以LRU的策略丢弃旧的partition
			// 如果想要手动移除RDD缓存而不想等到缓存过期，可以调用RDD.unpersist()方法
		}
	}

}
