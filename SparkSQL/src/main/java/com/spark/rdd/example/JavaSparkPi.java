package com.spark.rdd.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public final class JavaSparkPi {

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger
			.getLogger(LogAnalyizerUsingSpark.class);

	public static final String APP_NAME = "JavaStatusAPIDemo";

	private static final JavaSparkContext jsc = new JavaSparkContext(
			new SparkConf().setAppName(APP_NAME).setMaster("local[*]"));

	public static void main(String[] args) throws Exception {

		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<Integer>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

		int count = dataSet.map(new Function<Integer, Integer>() {
			public Integer call(Integer integer) {
				double x = Math.random() * 2 - 1;
				double y = Math.random() * 2 - 1;
				return (x * x + y * y < 1) ? 1 : 0;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer integer, Integer integer2) {
				return integer + integer2;
			}
		});

		System.out.println("Pi is roughly " + 4.0 * count / n);
		Thread.sleep(10000000);
		jsc.stop();
	}
}