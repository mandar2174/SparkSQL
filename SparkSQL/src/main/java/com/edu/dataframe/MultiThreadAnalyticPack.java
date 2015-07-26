package com.edu.dataframe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

class AnalyticPackThread implements Callable<DataFrame>
{
	private DataFrame dataframe;

	private String operation;

	private int count;

	public AnalyticPackThread( DataFrame dataframe, String operation, int count )
	{
		this.dataframe = dataframe;
		this.operation = operation;
		this.count = count;
	}

	@Override
	public DataFrame call() throws Exception
	{
		System.out.println( "Executing the Thread : " + count );
		if ( operation.equals( "select" ) )
		{
			return dataframe.select( "ColumnName" );
		}
		else if ( operation.equals( "groupBy" ) )
		{
			return dataframe.groupBy( "ColumnName" ).count();
		}
		else
		{
			Map<String, String> aggExp = new HashMap<>();
			aggExp.put( "ColumnName", "count" );
			return dataframe.groupBy( "ColumnName" ).agg( aggExp );

		}
	}
}

public class MultiThreadAnalyticPack
{
	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger( MultiThreadAnalyticPack.class );

	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

	private static final String ORACLE_USERNAME = "xxxx";

	private static final String ORACLE_PWD = "xxxx";

	private static final String ORACLE_DB_MACHINE_IP = "xx.xxx.xx.x";

	private static final String ORACLE_DB_MACHINE_PORT = "1521";

	private static final String COLON = ":";

	private static final String SEPARATOR = "/";

	private static final String ATTHERATE = "@";

	private static final String ORACLE_VERSION = "ora11g";

	private static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:" + ORACLE_USERNAME + SEPARATOR + ORACLE_PWD + ATTHERATE + ORACLE_DB_MACHINE_IP + COLON + ORACLE_DB_MACHINE_PORT + COLON + ORACLE_VERSION;

	private static final JavaSparkContext sc = new JavaSparkContext( new SparkConf().setAppName( "SparkJdbcDs" ).setMaster( "local[*]" ).set( "spark.executor.memory", "1g" ) );

	private static final SQLContext sqlContext = new SQLContext( sc );

	private static final int MAX_THREAD = 3;

	public static void main( String[] args )
	{
		LOGGER.info( "Executing Main Thread" );
		ExecutorService executorService = Executors.newFixedThreadPool( MAX_THREAD );
		Stack<DataFrame> dataFrame = new Stack<>();
		String tableName = "employee";
		String tableName1 = "tableName";
		String operation[] =
		{ "groupBy", "select", "agg" };

		sc.accumulator( 10, "accumulator" );
		Broadcast<int[]> broadcastVar = sc.broadcast( new int[]
		{ 1, 2, 3 } );
		int[] broadCaseElement = broadcastVar.value();
		for ( int index = 0; index < broadCaseElement.length; index++ )
		{
			System.err.println( broadCaseElement[index] );
		}

		DataFrame df = loadDataBase( tableName1 );

		for ( int index = 0; index < operation.length; index++ )
		{
			Callable<DataFrame> worker = new AnalyticPackThread( df, operation[index], index );
			java.util.concurrent.Future<DataFrame> result = executorService.submit( worker );
			try
			{
				dataFrame.add( result.get() );
				displayDataFrame( result.get().collectAsList() );
			}
			catch ( InterruptedException | ExecutionException e )
			{
				System.out.println( "Exception" );
			}

		}

		executorService.shutdown();
		// Wait until all threads are finish
		while ( !executorService.isTerminated() )
		{

		}
		LOGGER.info( "\nFinished all threads" );
	}

	private static DataFrame loadDataBase( String tableName )
	{
		// Data source options
		Map<String, String> options = new HashMap<>();
		options.put( "driver", ORACLE_DRIVER );
		options.put( "url", ORACLE_CONNECTION_URL );
		options.put( "dbtable", tableName );

		// Load Database in sqlContext and return result as DataFrame
		DataFrame jdbcDF = sqlContext.load( "jdbc", options );

		return jdbcDF;
	}

	private static void displayDataFrame( List<Row> selectItem )
	{
		for ( Row DataItem : selectItem )
		{
			LOGGER.info( DataItem );
		}

	}
}
