package com.datasphere.parser;

import gudusoft.gsqlparser.EDbVendor;
/**
 *  数据类型转换工具
 * @author Jack
 *
 */
public class DataTypeUtils {

	/**
	 * 得到新的数据类型
	 * @param type
	 * @return
	 */
	public static String getDataType(EDbVendor targetDbType,String sourceDataType) {
		String dataType = null;
		String type = sourceDataType.toUpperCase().replace("UNSIGNED", "");
		type = type.replace("AUTO_INCREMENT", "");
		type = type.trim();
		type = type.indexOf("(") > -1?type.substring(0, type.indexOf("(")):type;
		String eType = sourceDataType.indexOf("(") > -1? sourceDataType.substring(sourceDataType.indexOf("(")+1,sourceDataType.indexOf(")")): "";
		
		switch (type) {
		case "BIGINT":
		{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "BIGINT";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "BIGINT";
			}else {
				dataType = type;
			}
		}
		case "DATE":
		{
			dataType = type;
		}
		case "NUMBER":
		case "INT":
		case "INTEGER":
		{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "INT";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "INT8";
			}else {
				dataType = type;
			}
		}
		case "SMALLINT":
		case "TINYINT":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "SMALLINT";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "INT2";
			}else {
				dataType = type;
			}
			break;
		}
		case "MEDIUMINT":
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "MEDIUMINT";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "INT4";
			}else {
				dataType = type;
			}
			break;
		case "BINARY_DOUBLE":
		case "DOUBLE PRECISION":
		case "DOUBLE":
		{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "DOUBLE";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "FLOAT8";
			}else {
				dataType = type;
			}
			break;
		}
		case "BINARY_FLOAT":
		case "FLOAT":
		{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "DOUBLE";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "FLOAT";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "FLOAT4";
			}else {
				dataType = type;
			}
			break;
		}
		case "YEAR":
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "YEAR";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "CHAR(4)";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "CHAR(4)";
			}else {
				dataType = type;
			}
		case "NUMERIC":
		case "DEC":
		case "DECIMAL":
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "DECIMAL";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "NUMBER";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "NUMERIC";
			}else {
				dataType = type;
			}
			break;
		case "CHAR":
			dataType = "CHAR" +"("+eType+")";
			break;
		case "CHARACTER":
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "CHARACTER("+eType+")";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "VARCHAR2("+eType+")";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "CHARACTER("+eType+")";
			}else {
				dataType =  "CHARACTER("+eType+")";
			}
			break;
		case "VARCHAR2":
		case "VARCHAR":
		case "ENUM":
		case "SET":{
			try {
				int length = Integer.parseInt("".equalsIgnoreCase(eType)?"0":eType.trim());
				if(length >=4000) {
					if(targetDbType == EDbVendor.dbvmysql) {
						dataType = "TEXT";
					}else if(targetDbType == EDbVendor.dbvoracle) {
						dataType = "CLOB";
					}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
						dataType = "TEXT";
					}else {
						dataType = "TEXT";
					}
				}else {
					if(length == 0){
						dataType = "VARCHAR";
					}else {
						if(targetDbType == EDbVendor.dbvmysql) {
							dataType = "VARCHAR("+eType+")";
						}else if(targetDbType == EDbVendor.dbvoracle) {
							dataType = "VARCHAR2("+eType+")";
						}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
							dataType = "VARCHAR("+eType+")";
						}else {
							dataType = "VARCHAR";
						}
					}
				}
			}catch(Exception e) {
				dataType = type +"("+eType+")";
			}
			break;
		}
		case "TIMETZ":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "TIMETZ";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "VARCHAR2";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "VARCHAR";
			}else {
				dataType = "VARCHAR";
			}
			break;
		}
		case "TIME":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "TIME";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "DATE";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "TIME";
			}else {
				dataType = "TIME";
			}
			break;
		}
		case "TIMESTAMP WITHOUT TIME ZONE":
		case "TIMESTAMP WITH LOCAL TIME ZONE":
		{
			if(targetDbType == EDbVendor.dbvmysql) {
				if("TIMESTAMP WITHOUT TIME ZONE".equalsIgnoreCase(type)) {
					dataType = "VARCHAR";
				}else {
					dataType = "DATETIME";
				}
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "TIMESTAMP WITH LOCAL TIME ZONE";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "VARCHAR";
			}else {
				dataType = "VARCHAR";
			}
			break;
		}
		case "TIMESTAMP":
		case "DATETIME":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "DATETIME";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "TIMESTAMP";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "TIMESTAMP";
			}else {
				dataType = "TIMESTAMP";
			}
			break;
		}
		case "TINYTEXT":
		case "TEXT":
		case "MEDIUMTEXT":
		case "LONGTEXT":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "TEXT";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "CLOB";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "TEXT";
			}else {
				dataType = "TEXT";
			}
			break;
		}
		case "VARBINARY":
		case "TINYBLOB":
		case "BLOB":
		case "MEDIUMBLOB":
		case "LONGBLOB":
		case "BINARY":
		case "RAW":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "BYTEA";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "BLOB";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "BYTEA";
			}else {
				dataType = "BYTEA";
			}
			break;
		}
		case "BOOL":{
			if(targetDbType == EDbVendor.dbvmysql) {
				dataType = "VARCHAR";
			}else if(targetDbType == EDbVendor.dbvoracle) {
				dataType = "VARCHAR";
			}else if(targetDbType == EDbVendor.dbvgreenplum || targetDbType == EDbVendor.dbvpostgresql) {
				dataType = "BOOL";
			}else {
				dataType = "VARCHAR";
			}
			break;
		}
		default:
			dataType = type.toUpperCase();
			break;
		}
		return dataType;
	}
	
	class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
}
