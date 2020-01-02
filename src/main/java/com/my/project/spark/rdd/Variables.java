package com.my.project.spark;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

public class Variables {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Variables");
		conf.setMaster("local[4]");
		try(JavaSparkContext sc = new JavaSparkContext()) {
			// 通常，当一个函数被传递给Spark在远程集群结点上执行的时候，函数中用到的所有变量都是一份单独的副本
			// 这些变量被复制到每台机器上，而且远程机器上对变量做的任何更改都不会被传递回driver程序
			// 支持通用的跨任务读写共享变量会使程序效率低下，但Spark仍然支持两种共享变量：broadcast变量和accumulators

			// Broadcast变量允许开发者缓存一个只读变量到每台机器上，而不是随着任务分发这个变量的副本
			// 这可以用于，如：以一种有效的方式给每个节点分发一份很大的输入数据集
			// Spark也会尝试以有效的广播算法来分发broadcast变量，以减少通讯成本

			// Spark的action操作通过一组stage来执行，这些stage是由分布式shuffle操作分隔的
			// SparK会在每个stage中自动广播任务需要的公共数据，这些数据是以序列化的形式被缓存的，在任务执行之前再进行反序列化
			// 这意味着只有跨stage的任务需要同一份数据或者数据缓存时不序列化更好时，才需要显式的创建broadcast变量
			// 要通过变量v创建一个broadcast变量时，只需要SparkContext.broadcast(v)即可，可以通过value()方法获取broadcast变量的值
			Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
			System.out.println(Arrays.asList(broadcastVar.value()));
			// 创建好broadcast变量后，就应该在函数中直接使用broadcast变量，而不要使用原始变量v，这样变量v就不会被多次传递到各集群节点
			// 另外，变量v一旦被广播出去，就不要修改它的值，确保所有结点获取到相同的broadcast变量值

			// accumulators是通过关联和变换操作只能累加的变量，因此可以有效的支持并行化
			// 可以用做计数器或求和操作，Spark原生支持数值类型的accumulators，开发者可以扩展以支持其他类型
			// 做为用户，可以创建一个命名的或者匿名的accumulators
			// 命名的accumulators会在修改了该accumulator的Spark Web UI上显示出来，Spark还会显示任务修改的每个accumulator的值
			// 在Spark Web UI上跟踪accumulator的值可以用于了解stage的运行进度（暂不支持Python）

			// 数值类型的accumulator可以通过如下方式创建，支持Long/Double类型
			LongAccumulator longAccum = sc.sc().longAccumulator("LongAccumulator");
			DoubleAccumulator doubleAccum = sc.sc().doubleAccumulator("DoubleAccumulator");
			sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> longAccum.add(x));
			sc.parallelize(Arrays.asList(1.0, 2.0, 3.0, 4.0)).foreach(x -> doubleAccum.add(x));
			System.out.println("long: " + longAccum.value());
			System.out.println("double: " + doubleAccum.value());
			// 集群上运行的任务可以调用add()方法修改accumulator的值，但不能读取它的值
			// 只有driver程序可以使用value()方法读取accumulator的值

			// 除了Spark内建的accumulator以外，开发者可以通过继承AccumulatorV2这个类来实现自定义的accumulator
			// AccumulatorV2有很多抽象方法需要实现：
			// reset：重置accumulator为0
			// add：添加一个值到accumulator
			// merge：合并一个相同类型的accumulator的值到当前accumulator
			// 其他需要实现的方法参考API文档：
			// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.util.AccumulatorV2

			// 例：假设有一个MyVector类代表数学向量
			/*
			class VectorAccumulatorV2 extends AccumulatorV2<MyVector, MyVector> {
				private static final long serialVersionUID = -1903056255413364485L;
				private MyVector myVector = MyVector.createZeroVector();
				@Override
				public void reset() {
					myVector.reset();
				}
				@Override
				public void add(MyVector v) {
					myVector.add(v);
				}
				...
			}
			VectorAccumulatorV2 myVectorAcc = new VectorAccumulatorV2(); // create an accumulator of this type
			sc.sc().register(myVectorAcc, "MyVectorAcc1"); // register it into spark context
			*/

			// 注意：自定义的AccumulatorV2，其结果类型可能与所添加元素的类型不同
			
			// 警告：当一个spark任务完成时，Spark会尝试合并任务中的所有累加操作到一个accumulator中
			// 如果合并失败，Spark会忽略之，仍然标记任务为成功状态并继续执行其他任务
			// 因此一个错误的accumulator不会影响Spark Job，但可能无法得到正确更新，即使Spark Job执行成功了
			
			// 在action操作中，Spark保证每个任务对accumulator的更新只会应用一次，重启的任务将不会更新它的值
			// 在transformation操作中，用户应该注意，如果重新执行任务或stage，每个任务对accumulator的更新可能会执行不止一次

			// accumulator不会修改 the lazy evaluation model of spark
			// 如果在RDD上的一个操作更新中更新accumulator的值，只有RDD上运行的是action操作时，accumulator的更新操作才会执行
			// 在惰性transformation操作（如：map）中，不能保证accumulator的更新操作会立即执行
			LongAccumulator accum = sc.sc().longAccumulator("accum");
			sc.parallelize(Arrays.asList(1, 2, 3, 4, 5)).map(x -> { accum.add(x); return x * 10; });
			// Here, accum is still 0 because no actions have caused the map to be computed
			System.out.println("accum: " + accum.value());

			// wait to view accumulator on spark web UI
			try {
				TimeUnit.SECONDS.sleep(60);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
