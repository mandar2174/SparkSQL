package com.spark.sql.example;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkSqlWithMysql {

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger
			.getLogger(SparkSqlWithMysql.class);

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "root";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://127.0.0.1:3306/test?user="
			+ MYSQL_USERNAME + "&password=" + MYSQL_PWD;

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args) {
		
		// Data source options
				Map<String, String> options = new HashMap<String, String>();
				options.put("driver", MYSQL_DRIVER);
				options.put("url", MYSQL_CONNECTION_URL);
				options.put("dbtable",
						"(select empage, empname as full_name from employee) as employees_name");

				// options.put("dbtable", "select empname,empage from employee");
				options.put("partitionColumn", "empage");
				options.put("lowerBound", "10001");
				options.put("upperBound", "499999");
				options.put("numPartitions", "2");

				// Load MySQL query result as DataFrame
				DataFrame jdbcDF = sqlContext.load("jdbc", options);
				
				//show dataframe 
				
				jdbcDF.show();
				
				List<Row> employeeFullNameRows = jdbcDF.collectAsList();

				System.out.println("-------print the data frame schema------");
				// print the data frame schema
				jdbcDF.printSchema();
				
				// print the data frame schema
				printDataFrameScheme(jdbcDF);

				System.out.println("-------display the data frame element------");
				// display the data frame element
				displayDataFrame(employeeFullNameRows);

				System.out
						.println("-------filter only employees with name “ABC”------");
				// filter only employees with name “ABC”
				DataFrame filteredDf = jdbcDF.filter(jdbcDF.col("full_name").equalTo(
						"ABC"));

				// get the data frame column and print
				List<Row> employeeFilterNameRows = filteredDf.collectAsList();

				displayDataFrame(employeeFilterNameRows);

				System.out
						.println("-------Get the total element in the data frame------");
				// Get the total element in the data frame
				long Count = jdbcDF.count();
				LOGGER.info("Total Element is : " + Count);

				System.out.println("-------change the data frame column name------");
				// change the data frame column name
				DataFrame changeColumnName = jdbcDF.withColumnRenamed("full_name",
						"Employee Name");
				displayDataFrame(changeColumnName.collectAsList());
				printDataFrameScheme(changeColumnName);

				System.out
						.println("-------create the data frame by adding the 20 to age column------");
				// create the data frame by adding the 20 to age column
				DataFrame addColumnToDf = jdbcDF.withColumn("New age",
						jdbcDF.col("empage").$plus(20));
				displayDataFrame(addColumnToDf.collectAsList());
				printDataFrameScheme(addColumnToDf);

				System.out
						.println("-------Get the column name and Type from data frame using dType method------");
				// Get the column name and Type from data frame using dType method
				Map<String, String> columnType = new HashMap<String,String>();
				Tuple2<String, String> dfColumnType[] = jdbcDF.dtypes();
				for (int index = 0; index < dfColumnType.length; index++) {
					columnType.put(dfColumnType[index]._1(), dfColumnType[index]._2());
				}

				for (Map.Entry<String, String> element : columnType.entrySet()) {
					System.out.println("ColumnName : " + element.getKey()
							+ " ColumnType : " + element.getValue());
				}

				// Remove the data frame column

				System.out.println("-------Data frame with Drop Column------");
				DataFrame dropComumnDf = addColumnToDf.drop("empage");
				displayDataFrame(dropComumnDf.collectAsList());
				printDataFrameScheme(dropComumnDf);

				System.out.println("-------Data frame  Column Name------");
				// print all data frame column Name
				String[] columnNameList = addColumnToDf.columns();
				for (String columnName : columnNameList) {
					System.out.println("Column Name : " + columnName);
				}

				// create the data frame from the map
				System.out.println("-------Empty Data frame-----------");

				DataFrame createDF = sqlContext.emptyDataFrame();
				printDataFrameScheme(createDF);
				displayDataFrame(createDF.collectAsList());
		
	}

	private static void printDataFrameScheme(DataFrame df) {
		StructType tableType = df.schema();
		StructField[] columnType = tableType.fields();
		Map<String, String> columnElement = new HashMap<String, String>();
		for (StructField element : columnType) {

			columnElement.put(element.name(), element.dataType().toString());

		}

		for (Map.Entry<String, String> metaData : columnElement.entrySet()) {
			System.out.println("Column Name : " + metaData.getKey()
					+ "  ColumnType : " + metaData.getValue());

		}
	}

	private static void displayDataFrame(List<Row> selectItem) {
		for (Row DataItem : selectItem) {
			LOGGER.info(DataItem);
		}

	}
}
