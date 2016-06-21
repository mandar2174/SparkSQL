package com.spark.rdd.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class LogAnalyizerUsingSpark {

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger
			.getLogger(LogAnalyizerUsingSpark.class);

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("LogAnalyzier").setMaster("local[*]"));
	
	public static void main(String[] args) throws InterruptedException {
		JavaRDD<String> lines = sc.textFile("src/main/resources/access_log");
		
		long getRequestCount = lines.filter(new Function<String, Boolean>() {

			public Boolean call(String v1) throws Exception {
				if (v1.contains("GET"))
					return true;
				else
					return false;
			}
		}).persist(StorageLevel.MEMORY_ONLY()).repartition(10).count();
		
		LOGGER.info("Request count for GET : " + getRequestCount);

		//cache the rdd
		//lines.cache();
		
		
		//cache the rdd using persist method based on storage level
		//lines.persist(StorageLevel.MEMORY_ONLY());		
		
		long postRequestCount = lines.filter(new Function<String, Boolean>() {

			public Boolean call(String v1) throws Exception {
				if (v1.contains("POST"))
					return true;
				else
					return false;
			}
		}).repartition(10).count();
		
		//line for running application in infintely
		//Thread.sleep(100000);
		//LOGGER.info("Request count for POST : " + postRequestCount);
	}
}
