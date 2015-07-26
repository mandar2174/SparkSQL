package com.edu.dataframe;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class OracleMainJDBCConnect implements Serializable
{

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger( OracleMainJDBCConnect.class );

	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

	private static final String ORACLE_USERNAME = "username";

	private static final String ORACLE_PWD = "username";

	private static final String ORACLE_DB_MACHINE_IP = "xxx.xx.xx.x";

	private static final String ORACLE_DB_MACHINE_PORT = "1521";

	private static final String COLON = ":";

	private static final String SEPARATOR = "/";

	private static final String ATTHERATE = "@";

	private static final String ORACLE_VERSION = "ora11g";

	private static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:" + ORACLE_USERNAME + SEPARATOR + ORACLE_PWD + ATTHERATE + ORACLE_DB_MACHINE_IP + COLON + ORACLE_DB_MACHINE_PORT + COLON + ORACLE_VERSION;

	private static final JavaSparkContext sc = new JavaSparkContext( new SparkConf().setAppName( "SparkJdbcDs" ).setMaster( "local[*]" ).set( "spark.executor.memory", "1g" ) );

	private static final SQLContext sqlContext = new SQLContext( sc );

	public static void main( String[] args )
	{
		// Data source options
		Map<String, String> options = new HashMap<>();
		options.put( "driver", ORACLE_DRIVER );
		options.put( "url", ORACLE_CONNECTION_URL );
		//options.put( "dbtable", "employee" );
		options.put( "dbtable", "tableName" );

		// Load MySQL query result as DataFrame
		DataFrame jdbcDF = sqlContext.load( "jdbc", options );

		// cache the data frame
		//jdbcDF.cache();

		//List<Row> employeeFullNameRows = jdbcDF.collectAsList();

		//displayDataFrame( employeeFullNameRows );

		// filter only employees with name “ABC”

		/*DataFrame filteredDf = jdbcDF.filter( jdbcDF.col( "BILLCITY" ).equalTo( "PUNE" ) );

		List<Row> employeeFilterNameRows = filteredDf.collectAsList();

		displayDataFrame( employeeFilterNameRows );

		LOGGER.info( "Total Element with City Pune is : " + filteredDf.count() );*/

		// Group By bill city

		/*List<Row> groupByDf = jdbcDF.groupBy( "BILLCITY" ).count().collectAsList();
		displayDataFrame( groupByDf );*/

		// Get the total element in the data frame

		/*long Count = jdbcDF.count();
		LOGGER.info( "Total Element is : " + Count );*/

		sqlContext.registerDataFrameAsTable( jdbcDF, "tempTable" );

		/*DataFrame selectTableDF = sqlContext.sql( "Select * from tempTable" );

		List<Row> selectItem = selectTableDF.collectAsList();

		displayDataFrame( selectItem );*/

		/*DataFrame groupByDF = sqlContext.sql( "Select BILLCITY,count(*) from tempTable group by BILLCITY" );

		displayDataFrame( groupByDF.collectAsList() );

		LOGGER.info( groupByDF.count() );
		*/
		/*DataFrame groupByDF1 = sqlContext.sql( "Select ISNP,BILLCITY,Max(ISNP),AVG(ISNP) from tempTable group by ISNP,BILLCITY" );

		displayDataFrame( groupByDF1.collectAsList() );

		LOGGER.info( groupByDF1.count() );*/

		/*DataFrame groupByDF1 = sqlContext.sql( "Select EMPCOUNTRY,Max(EMPSALARY),EMPNAME from tempTable group by EMPCOUNTRY,EMPNAME" );

		displayDataFrame( groupByDF1.collectAsList() );
		*/
		//groupByDF1.javaRDD().saveAsTextFile( args[0] );

		//Sort the table by BILLCITY

		/*DataFrame sortDF = sqlContext.sql( "select * from tempTable order by BILLCITY asc" );

		displayDataFrame( sortDF.collectAsList() );
		*/

		//get the particular column
		//DataFrame columnDF = jdbcDF.select( "UNBILLED_AMOUNT","BILLCITY" );

		Column[] columns = new Column[2];
		columns[0] = jdbcDF.col( "BILLCITY" );
		columns[1] = jdbcDF.col( "UNBILLED_AMOUNT" );
		DataFrame columnDF = jdbcDF.select( columns );
		displayDataFrame( columnDF.collectAsList() );

		//to print the data frame result into the file
		//columnDF.rdd().saveAsTextFile( args[0] );

		//Aggregate on table 

		Map<String, String> aggregateCondition = new HashMap<>();
		aggregateCondition.put( "count", "CCCITY" );
		//DataFrame aggregateDF = jdbcDF.groupBy( "BILLCITY" ).count();

		//displayDataFrame( aggregateDF.collectAsList() );

		//DataFrame aggregateCountDF = jdbcDF.groupBy(jdbcDF.col("CCCITY")).count();//.agg( aggregateCondition );
		//DataFrame aggregateCountDF = sqlContext.sql("select ISNP ,count(ISNP),min(ISNP),max(ISNP) ,avg(ISNP),Sum(ISNP) from tempTable group by ISNP ");
		//DataFrame aggregateCountDF = sqlContext.sql( "select EMPCOUNTRY ,count(EMPAGE),min(EMPAGE),max(EMPAGE) ,avg(EMPAGE),Sum(EMPAGE) from tempTable group by EMPCOUNTRY");
		//displayDataFrame( aggregateCountDF.collectAsList() );

		//DataFrame aggregate = jdbcDF.filter( jdbcDF.col( "EMPAGE" ).gt( 25 ) ).groupBy( jdbcDF.col("EMPSALARY")).agg(jdbcDF.col( "EMPSALARY" ) );
		//displayDataFrame( sqlContext.sql( "Select count(*) From tempTable Where to_char(CRM_STATUS_DT, 'DD-Mon-YY') > '31-AUG-14'" ).collectAsList() );
	}

	private static void displayDataFrame( List<Row> selectItem )
	{
		for ( Row DataItem : selectItem )
		{
			LOGGER.info( DataItem );
		}

	}
}
