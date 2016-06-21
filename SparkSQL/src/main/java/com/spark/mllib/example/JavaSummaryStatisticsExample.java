package com.spark.mllib.example;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

public class JavaSummaryStatisticsExample {
	  public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setAppName("JavaSummaryStatisticsExample").setMaster("local[*]");
	    JavaSparkContext jsc = new JavaSparkContext(conf);

	    JavaRDD<Vector> mat = jsc.parallelize(
	      Arrays.asList(
	        Vectors.dense(1.0, 10.0, 100.0),
	        Vectors.dense(2.0, 20.0, 200.0),
	        Vectors.dense(3.0, 30.0, 300.0)
	      )
	    ); // an RDD of Vectors

	    // Compute column summary statistics.
	    MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
	    System.out.println(summary.mean());  // a dense vector containing the mean value for each column
	    System.out.println(summary.variance());  // column-wise variance
	    System.out.println(summary.numNonzeros());  // number of nonzeros in each column

	    jsc.stop();
	  }
	}
