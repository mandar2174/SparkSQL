package com.spark.sql.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class AccumulatorExample {

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger
			.getLogger(AccumulatorExample.class);

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));
	
	private static final SQLContext sqlContext = new SQLContext(sc);

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

	public static void main(String[] args) {

		DataFrame dataFrame = formDataFrame();
		
		System.out.println("Displaying dataframe : ");
		//display the data frame using take action
		dataFrame.show();
		
		//create the accumulator for counting number of complaint 
		final Accumulator<Integer> count = sc.accumulator(0);
		
		dataFrame.toJavaRDD().map(new Function<Row, Row>() {

			public Row call(Row record) throws Exception {
				count.add(record.getInt(3));
				return record;
			}
		}).count();
		
		System.out.println("Total Complaint : " + count.value());
		
	}

	/**
	 * Function to form dataframe for Accumulator
	 * 
	 * @return dataFrame
	 */
	private static DataFrame formDataFrame() {
		List<Row> rowElement = new ArrayList<Row>();
		rowElement.add(RowFactory.create("2015-01-01 12:12:21", 10,
				"2015-07-13 12:12:21", 100));
		rowElement.add(RowFactory.create("2015-02-05 12:12:21", 20,
				"2015-12-15 12:12:21", 85));
		rowElement.add(RowFactory.create("2015-05-23 12:12:21", 30,
				"2015-03-23 12:12:21", 250));
		rowElement.add(RowFactory.create("2015-04-12 12:12:21", 40,
				"2015-01-23 12:12:21", 520));
		rowElement.add(RowFactory.create("2015-07-11 12:12:21", 50,
				"2015-09-23 12:12:21", 96));
		rowElement.add(RowFactory.create("2015-01-11 12:12:21", 50,
				"2018-09-20 12:12:21", 70));
		rowElement.add(RowFactory.create("2015-05-23 12:12:21", 50,
				"2017-09-12 12:12:21", 30));

		Map<String, String> columnNameType = new LinkedHashMap<String, String>();
		columnNameType.put("call_day", "StringType");
		columnNameType.put("call_count", "DoubleType");
		columnNameType.put("complaint_day", "StringType");
		columnNameType.put("complaint_count", "DoubleType");

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

		DataFrame dataFrame = sqlContext.createDataFrame(sc.parallelize(rowList),
				schema);

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
