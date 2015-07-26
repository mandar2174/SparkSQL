package com.edu.dataframe;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class OracleDBConnect implements Serializable
{

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(OracleDBConnect.class);

	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

	private static final String ORACLE_USERNAME = "oracle_user_name";

	private static final String ORACLE_PWD = "oracle_user_name";

	private static final String ORACLE_DB_MACHINE_IP = "xxx.xx.xx.x";

	private static final String ORACLE_DB_MACHINE_PORT = "1521";

	private static final String COLON = ":";

	private static final String SEPARATOR = "/";

	private static final String ATTHERATE = "@";

	private static final String ORACLE_VERSION = "ora11g";

	private static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:" + ORACLE_USERNAME + SEPARATOR + ORACLE_PWD
		+ ATTHERATE + ORACLE_DB_MACHINE_IP + COLON + ORACLE_DB_MACHINE_PORT + COLON + ORACLE_VERSION;

	private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs")
		.setMaster("local[*]").set("spark.executor.memory", "1g"));

	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args)
	{

		// Load MySQL query result as DataFrame
		DataFrame jdbcDF = loadDataBase("tableName");

		DataFrame df1 = loadDataBase("employee");

		DataFrame df2 = loadDataBase("employee1");

		long startTime = System.currentTimeMillis();

		System.out.println("Count of element : " + jdbcDF.count());

		long endTime = System.currentTimeMillis();

		System.out.println("Difference : " + (endTime - startTime));
		// displayDataFrame( df1.collectAsList() );
		// displayDataFrame( df2.collectAsList() );

		// join the two table
		/*
		 * DataFrame joinDataFrame = df1.join( df2, df1.col( "EMPNAME" ).equalTo( df2.col( "EMPNAME" ) ), "right_outer"
		 * ); displayDataFrame( joinDataFrame.collectAsList() );
		 */

		Properties properties = new Properties();
		properties.setProperty("allowExisting", " true");
		// DataFrame df3 = df1.groupBy(df1.col("EMPNAME")).count();
		// df1.createJDBCTable(ORACLE_CONNECTION_URL, "employee2", true);
		// df3.write().jdbc( ORACLE_CONNECTION_URL, "testEmployee", properties );

		// working fine to insert the record to existing table
		// df1.insertIntoJDBC( ORACLE_CONNECTION_URL, "employee1", true );
		// System.out.println( joinDataFrame.rdd().toDebugString() );

		long startTime1 = System.currentTimeMillis();
		List<Row> rowInfor = df1.toJavaRDD().map(new Function<Row, Row>()
		{
			public Row call(Row row)
			{
				// System.out.println( "Row : " + row.getString( 0 ) );
				return row;
			}
		}).collect();

		System.out.println("Row Size " + rowInfor.size());

		long endTime1 = System.currentTimeMillis();
		System.out.println("Difference 1: " + (endTime1 - startTime1));
		System.out.println("Printing the mapping row information");
		// displayDataFrame(rowInfor);

		System.out.println("Data inserted successfully into the oracle DB");

		// Join on the two data frame

		// join on the string data type
		/*
		 * DataFrame joinDataFrame = df1.join( df2, df1.col( "EMPNAME" ).equalTo( df2.col( "EMPNAME" ) ), "right_outer"
		 * ); displayDataFrame( joinDataFrame.collectAsList() );
		 */

		// join on the integer data type
		/*
		 * DataFrame joinDataFrame = df1.join( df2, df1.col( "EMPAGE" ).equalTo( df2.col( "EMPNAME" ) ), "right_outer"
		 * ); displayDat  Column Name------aFrame( joinDataFrame.collectAsList() );
		 */
		// getting error - >java.lang.ClassCastException: java.math.BigDecimal cannot be cast to
		// org.apache.spark.sql.types.Decimal
		// displayDataFrame( df1.groupBy().avg( "EMPAGE" ).collectAsList() );

		// Drop duplicates record
		// displayDataFrame( df1.dropDuplicates().collectAsList() );

		// sort the empname in ascending order
		// displayDataFrame( df1.sort( df1.col( "EMPNAME" ).asc() ).collectAsList() );

		// sort the empname in descending order
		// displayDataFrame( df1.sort( df1.col( "EMPNAME" ).desc() ).collectAsList() );

		// displayDataFrame( df1.na().drop().collectAsList() );

		// displayDataFrame( df1.unionAll( df2 ).collectAsList() );

		// displayDataFrame( df1.intersect( df2 ).collectAsList() );

		// displayDataFrame( df1.except( df2 ).collectAsList() );

		// displayDataFrame( df1.repartition( 10 ).collectAsList() );

		// df1.explain( true );

		// fill the null value with default value(Handle the null value from the db)
		Map<String, Object> fillValue = new HashMap<>();
		// fillValue.put( "EMPSALARY", "0" );
		// fillValue.put( "EMPCOUNTRY", "Unknown" );

		// set the particular column null value with default value
		// displayDataFrame( df1.na().fill( fillValue ).collectAsList() );

		// set the all null value with unknown
		// displayDataFrame( df1.na().fill( "Unknown" ).collectAsList() );

		DataFrame dfCopy = df1.as("temp");
		displayDataFrame(dfCopy.collectAsList());
		/*
		 * displayDataFrame( df1.stat().freqItems( new String[] { "EMPCOUNTRY" } ).collectAsList() );
		 */

		// cache the data frame
		// jdbcDF.cache();

		// get the all element from the data frame
		// List<Row> employeeFullNameRows = jdbcDF.collectAsList();

		// displayDataFrame( employeeFullNameRows );

		// Get the total element in the data frame

		// long Count = jdbcDF.count();
		// LOGGER.info( "Total Element is : " + employeeFullNameRows.size() );

		// group the data frame and perform the aggragation on that
		/*
		 * Map<String, String> aggreGateExpression = new HashMap<>(); aggreGateExpression.put( "BILLCITY", "min" );
		 * DataFrame df = jdbcDF.groupBy( jdbcDF.col( "BILLCITY" ) ).agg( aggreGateExpression );
		 */

		/*
		 * Map<String, String> aggreGateExpression = new HashMap<>(); aggreGateExpression.put( "CCCITY", "count" );
		 * DataFrame df = jdbcDF.groupBy( jdbcDF.col( "CCCITY" ) ).agg( aggreGateExpression );
		 */
		// displayDataFrame( df.distinct().collectAsList() );

		// perform the describe operation on data frame
		// displayDataFrame(df.describe( ).collectAsList());

		// df.rdd().saveAsTextFile( args[0] );

	}

	private static void displayDataFrame(List<Row> selectItem)
	{
		for(Row DataItem: selectItem)
		{
			LOGGER.info(DataItem);
		}

	}

	private static DataFrame loadDataBase(String tableName)
	{
		// Data source options
		Map<String, String> options = new HashMap<>();
		options.put("driver", ORACLE_DRIVER);
		options.put("url", ORACLE_CONNECTION_URL);
		options.put("dbtable", tableName);

		// Load Database in sqlContext and return result as DataFrame
		DataFrame jdbcDF = sqlContext.load("jdbc", options);

		return jdbcDF;
	}

}
