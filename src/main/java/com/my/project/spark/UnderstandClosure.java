package com.my.project.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class UnderstandClosure {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Understanding Closures");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext(conf)) {
			// Spark比较难理解的是跨集群节点执行代码时变量的作用域和生命周期问题
			// RDD操作可以在变量作用域之外修改变量值是难理解的主要原因

			// 这种写法是不对的！！
			// 这段代码的结果是不确定的，可能不能以预期的方式运行
			/*
			int counter = 0;
			JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
			rdd.foreach(x -> counter += x);
			System.out.println("Counter value: " + counter);
			*/

			// 开始执行Job的时候，Spark将RDD操作拆分成task，每个executor执行一个task
			// 在执行之前，Spark会计算task的closure，这个closure就是executor执行这个RDD操作必须要访问的变量和方法
			// 这个closure会被序列化并分发到每个executor上
			// 这样每个executor上拿到的变量只是相当于一个副本，所以foreach代码中引用的counter已然不是driver程序中的counter了
			// 同时，driver程序中仍然有一个counter变量，但executor是访问不到的，executor只能看到序列化之后的closure里的counter的副本
			// 因此最终counter的值仍然是0，因为所有对counter的操作都是在序列化后的closure中进行的，并不能体现到driver程序中
			
			// 在local模式下，有时foreach函数可能会和driver程序在同一个JVM中执行，可以引用到对应的原始counter，并更新它的值
			
			// 为了确保这种场景下的操作行为是确定的，应该使用Accumulator
			// Accumulator提供了一种在集群环境下跨worker节点安全更新变量值的机制

			// 总之，构造循环或自定义方法时，不应该尝试去改变全局状态
			// Spark不保证也不确定对colsure外被引用变量值的变更操作能够生效，有时local模式下可能会生效，但是在分布式场景一定是有问题的
			// 需要使用Accumulator实现对全局变量的变更操作

			// 关于打印RDD中的元素
			// 你可能会想使用rdd.foreach(println)或rdd.map(println)来打印
			// 对于单机模式来说，这样做是可以的
			// 但在集群模式下，打印的内容会被输出到executor的控制台中，而不是driver程序的控制台，所以控制台不会有任何输出
			// 正确的做法是使用collect()方法，如：rdd.collect().foreach(println)
			// 但这可能会导致driver程序内存溢出，因为collect()会获取整个RDD元素到一台机器上
			// 如果只是打印几行内部，更安全的做法是使用task()方法：rdd.take(100).foreach(println)
		}
	}

}
