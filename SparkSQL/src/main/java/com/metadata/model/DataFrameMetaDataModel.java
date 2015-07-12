package com.metadata.model;

import java.util.List;

public class DataFrameMetaDataModel {

	List<MetaData> metaDataList;

	public List<MetaData> getMetaDataList() {
		return metaDataList;
	}

	public void setMetaDataList(List<MetaData> metaDataList) {
		this.metaDataList = metaDataList;
	}

	@Override
	public String toString() {
		return "DataFrameMetaDataModel [metaDataList=" + metaDataList + "]";
	}

}
