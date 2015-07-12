package com.edu.dataframe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.metadata.model.DataFrameMetaDataModel;
import com.metadata.model.MetaData;

public class MetaDataGenerator {

	public static DataFrame generateDataFrame(String fileName,
			SQLContext sqlContext) {

		DataFrame newDataFrame = sqlContext.read().json(fileName);
		return newDataFrame;
	}

	public static DataFrameMetaDataModel generateMetaDataModel(
			Map<String, String> elementList) {

		DataFrameMetaDataModel dataFrameMetaDataModel = new DataFrameMetaDataModel();
		List<MetaData> metaDataList = new ArrayList<>();
		MetaData metaData = null;
		for (Map.Entry<String, String> element : elementList.entrySet()) {

			metaData = new MetaData();
			metaData.setColumnName(element.getKey());
			metaData.setDataType(element.getValue());
			metaData.setLabelValue("Yes");
			metaData.setDistType("continous");

			metaDataList.add(metaData);
		}
		dataFrameMetaDataModel.setMetaDataList(metaDataList);
		return dataFrameMetaDataModel;
	}
}
