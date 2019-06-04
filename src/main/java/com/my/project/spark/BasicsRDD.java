package com.my.project.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

public class BasicsRDD {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Basics RDD");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// 使用外部文件创建一个RDD，lines仅仅是一个指向该文件的指针
			JavaRDD<String> lines = sc.textFile("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\README.md");
			// 对lines进行map操作，并将转换结果保存到lineLengths，但并不进行实际的map计算
			JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

			// 如果之后还要用到lineLengths，可以在reduce之前调用如下方法
			// 这样可以让lineLengths在首次计算之后就保存到内存当中
			lineLengths.persist(StorageLevel.MEMORY_ONLY());
			
			// 最后执行reduce，因为它是一个action操作，所以spark会将计算分解为不同机器上运行的任务
			// 并且每台机器都运行一部分map和本地reduce，然后将计算结果返回给driver程序
			int totalLength = lineLengths.reduce((a, b) -> a + b);
			System.out.println("total length: " + totalLength);

			// Spark的API重度依赖于传递function给driver程序，以让这些function能在集群上运行
			// Java API的接口都这org.apache.spark.api.java.function包下面
			// 有两种方式创建function:
			// 1. 在自己的类中实现Function接口，可以是匿名类或命名类，然后将其实例传递给Spark
			// 2.使用lambda表达式（推荐）
			// 注意：匿名内部类是可以访问final的局部变量的，Spark会将这些变量的副本发送到每个worker节点上，其他语言也一样

			// 以下是匿名类的写法
			JavaRDD<Integer> lineLengths1 = lines.map(new Function<String, Integer> () {
				private static final long serialVersionUID = 1L;
				public Integer call(String s) {
					return s.length();
				}
			});
			int totalLength1 = lineLengths1.reduce(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = 1L;
				public Integer call(Integer a, Integer b) {
					return a + b;
				}
			});
			System.out.println("total length: " + totalLength1);
			// 以下是命名类的写法
			class GetLength implements Function<String, Integer> {
				private static final long serialVersionUID = -3590108365118773246L;
				@Override
				public Integer call(String s) throws Exception {
					return s.length();
				}
			}
			class Sum implements Function2<Integer, Integer, Integer> {
				private static final long serialVersionUID = -4104628178215811340L;
				@Override
				public Integer call(Integer a, Integer b) throws Exception {
					return a + b;
				}
			}
			JavaRDD<Integer> lineLengths2 = lines.map(new GetLength());
			int totalLength2 = lineLengths2.reduce(new Sum());
			System.out.println("total length: " + totalLength2);
		}
	}

}
