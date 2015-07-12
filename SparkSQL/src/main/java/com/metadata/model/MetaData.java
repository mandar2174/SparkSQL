package com.metadata.model;

public class MetaData {

	private String columnName;

	private String dataType;

	private String labelValue;

	private String distType;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getLabelValue() {
		return labelValue;
	}

	public void setLabelValue(String labelValue) {
		this.labelValue = labelValue;
	}

	public String getDistType() {
		return distType;
	}

	public void setDistType(String distType) {
		this.distType = distType;
	}

	@Override
	public String toString() {
		return "MetaData [columnName=" + columnName + ", dataType=" + dataType
				+ ", labelValue=" + labelValue + ", distType=" + distType + "]";
	}

}
