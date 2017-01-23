package com.spark.sql.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WhenCaseStmtSparkExample1 {

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger
			.getLogger(WhenCaseStmtSparkExample1.class);

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

	// create sql context for loading sql database
	private static final SQLContext sqlContext = new SQLContext(sc);

	// create hive context for loading hive database
	private static SQLContext hiveContext = new HiveContext(sc.sc());

	private static Map<String, DataType> dataTypeMap;

	static {

		dataTypeMap = new HashMap<String, DataType>();
		dataTypeMap.put("StringType", DataTypes.StringType);
		dataTypeMap.put("IntegerType", DataTypes.IntegerType);
		dataTypeMap.put("LongType", DataTypes.LongType);
		dataTypeMap.put("DoubleType", DataTypes.DoubleType);
		dataTypeMap.put("DateType", DataTypes.DateType);
		dataTypeMap.put("TimestampType", DataTypes.TimestampType);
	}

	public static void main(String[] args) throws InterruptedException {

		// create dataframe by loading hive table(Uncomment only when loading data from hive)
		/*DataFrame df = loadHiveTable(hiveContext, "dbName", "tableName");

		DataFrame tempDF = df.select(df.col("individual_exact_age"), functions
				.explode(df.col("amft")).as("amft"));

		DataFrame dataFrame = tempDF.select(df.col("individual_exact_age"),
				tempDF.col("amft").getField("trans_date").as("trans_date"),
				tempDF.col("amft").getField("activity_type")
						.as("activity_type"));*/
		DataFrame dataFrame = formDataFrame();

		System.out.println("Displaying dataframe : ");
		// display the data frame using take action
		dataFrame.show();

		// get count(*) of activity_type = "D"
		long recordWithDType = dataFrame.filter(
				dataFrame.col("activity_type").equalTo("D")).count();

		// get max transaction
		Map<String, String> mostRecentTranscation = new HashMap<String, String>();
		mostRecentTranscation.put("trans_date", "max");

		List<Row> mostTrsnList = dataFrame.agg(mostRecentTranscation)
				.collectAsList();

		// broadcast value to add custom column d_type_count and
		// most_recent_trans_v
		final Broadcast<Long> activityTupeCount = sc.broadcast(recordWithDType);
		final Broadcast<String> recenTrsnValue = sc.broadcast(mostTrsnList.get(
				0).getString(0));

		List<Row> rowList = dataFrame.toJavaRDD().map(new Function<Row, Row>() {

			public Row call(Row arg0) throws Exception {
				Object element[] = new Object[arg0.length() + 2];
				int i = 0;
				for (i = 0; i < arg0.length(); i++) {
					element[i] = arg0.get(i);
				}
				element[i] = activityTupeCount.value();
				element[++i] = recenTrsnValue.value();
				return RowFactory.create(element);
			}
		}).collect();

		Map<String, String> columnNameType = new LinkedHashMap<String, String>();
		columnNameType.put("trans_date", "StringType");
		columnNameType.put("activity_type", "StringType");
		columnNameType.put("individual_exact_age", "IntegerType");
		columnNameType.put("d_type_count", "LongType");
		columnNameType.put("most_recent_trans_v", "StringType");

		dataFrame = createDataFrame(rowList, columnNameType);

		dataFrame.show();

		// find the difference between currentdate and transaction date
		DataFrame d1 = dataFrame.withColumn("currentdate",
				functions.current_date());

		d1 = d1.withColumn("transaction_2_y",
				functions.datediff(d1.col("currentdate"), d1.col("trans_date")));

		// case logic
		d1 = d1.withColumn(
				"New_Column",
				functions
						.when(d1.col("transaction_2_y").leq(730)
								.and(d1.col("activity_type").equalTo("D")),
								functions.when(
										d1.col("individual_exact_age").between(
												18, 54), "PHILANTHROPIST")
										.otherwise("NEW KID ON THE BLOCK"))
						.otherwise(
								functions
										.when(d1.col("activity_type")
												.equalTo("D")
												.and(d1.col("d_type_count")
														.equalTo(1)),
												functions
														.when(d1.col(
																"individual_exact_age")
																.between(18, 54),
																"future Fans")
														.otherwise(
																"just not my scene"))
										.otherwise(
												functions
														.when(functions
																.datediff(
																		d1.col("currentdate"),
																		d1.col("most_recent_trans_v"))
																.gt(730),
																"Downshifting and DownSizing")
														.otherwise(
																"HouseholdDonatedCondition"))));

		// print the record
		d1.show();

	}

	private static DataFrame loadHiveTable(SQLContext hiveContext,
			String dbName, String tableName) {
		DataFrame df = hiveContext.sql("select * from " + dbName + "."
				+ "tableName");
		return df;
	}

	/**
	 * Function to form dataframe for Accumulator
	 * 
	 * @return dataFrame
	 */
	private static DataFrame formDataFrame() {
		List<Row> rowElement = new ArrayList<Row>();
		rowElement.add(RowFactory.create("2015-01-01", "D", 60));
		rowElement.add(RowFactory.create("2015-02-05", "P", 45));
		rowElement.add(RowFactory.create("2015-05-23", "M", 39));
		rowElement.add(RowFactory.create("2015-04-12", "G", 55));
		rowElement.add(RowFactory.create("2015-07-11", "D", 20));
		rowElement.add(RowFactory.create("2015-01-11", "D", 47));
		rowElement.add(RowFactory.create("2015-05-23", "M", 30));
		rowElement.add(RowFactory.create("2015-07-11", "D", 100));
		rowElement.add(RowFactory.create("2015-08-11", "D", 47));

		Map<String, String> columnNameType = new LinkedHashMap<String, String>();
		columnNameType.put("trans_date", "StringType");
		columnNameType.put("activity_type", "StringType");
		columnNameType.put("individual_exact_age", "IntegerType");

		DataFrame dataFrame = createDataFrame(rowElement, columnNameType);

		return dataFrame;
	}

	/**
	 * Function to create the dataframe based on rowlist data
	 * 
	 * @param rowList
	 *            row for dataframe
	 * @param getColumnNameType
	 *            map contains column name and type
	 * @return dataFrame
	 */
	private static DataFrame createDataFrame(List<Row> rowList,
			Map<String, String> getColumnNameType) {
		List<StructField> fields = new ArrayList<StructField>();
		int indx = 0;
		List<String> columnName = new ArrayList<String>(
				getColumnNameType.keySet());

		DataType[] types = new DataType[getColumnNameType.size()];
		for (; indx < getColumnNameType.size(); indx++) {
			types[indx] = getColumnDataType(getColumnNameType.get(columnName
					.get(indx)));
		}
		indx = 0;
		for (DataType fieldType : types)
			fields.add(DataTypes.createStructField(columnName.get(indx++),
					fieldType, true));

		StructType schema = DataTypes.createStructType(fields);

		DataFrame dataFrame = sqlContext.createDataFrame(
				sc.parallelize(rowList), schema);

		return dataFrame;
	}

	/**
	 * Function to return spark datatype based on dataType
	 * 
	 * @param dataType
	 *            input type for which corresponding spark datatype to be get
	 * @return spark sql datatype
	 */
	private static DataType getColumnDataType(String dataType) {

		if (dataTypeMap.containsKey(dataType)) {
			return dataTypeMap.get(dataType);
		}
		return null;
	}

}
