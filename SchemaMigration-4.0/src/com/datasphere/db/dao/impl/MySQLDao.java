package com.datasphere.db.dao.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.common.utils.O;
import com.datasphere.db.config.SnappyConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.datasphere.db.exception.ColumnTypeUnsurported;

public class MySQLDao extends AbstractBaseDao {

	BasicDataSource bds;
	
	public List<Schema> getSchemas() throws Exception{
		try(Connection conn = getConnection()) {
			
			return null;
		}
	}

	/**
	 * 创建schema
	 */
	public void createSchemas(List<Schema> destSchemas) throws Exception{
		try (Connection conn = getConnection(); Statement statement = conn.createStatement();) {
			for (Schema schema : destSchemas) {
				try {
					String isScheamsExistQuery="select count(*) mun from sys.sysschemas where schemaname= '"+schema.getName().toUpperCase()+"'";
					//创建mysql 的Schemas
					ResultSet isScheamsExistRset = statement.executeQuery(isScheamsExistQuery);
					if (isScheamsExistRset.next()) {
						int isScheamsExist = isScheamsExistRset.getInt(1);
						if(isScheamsExist==0){
							statement.executeUpdate("create schema " + schema.getName());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public List<Table> getTables() throws Exception{
		try(Connection conn = getConnection()) {
			
			return null;
		}
	}

	/**
	 * 创建表
	 */
	public void createTables(List<Table> destTables) throws Exception{
		try (Connection conn = getConnection(); Statement statement = conn.createStatement();) {
			for (Table table : destTables) {
				try(Statement tempStat = conn.createStatement()) {
					tempStat.executeUpdate("drop table " + table.getSchema() + "." + table.getName());
				} catch(Throwable t) {}
				
				StringBuilder sb = new StringBuilder();
				sb.append("create table " + table.getSchema() + "." + table.getName());
				sb.append("(");
				for(Column column: table.getColumns()) {
					sb.append(column.getName() + " ");
					switch(column.getType().toLowerCase()) {
						case "varchar": {
							sb.append("varchar(" + column.getLength() + ")");
							break;
						}
						case "decimal": {
							sb.append("decimal(" + column.getPrecision()+ "," + column.getScale() +")");
							break;
						}
						case "numeric": {
							sb.append("numeric(" + column.getPrecision() + "," + column.getScale() +")");
							break;
						}
						case "char": {
							sb.append("char(" + column.getLength() +")");
							break;
						}
						default: {
							sb.append(column.getType());
						}
					}
					sb.append(",");
				}
				sb.deleteCharAt(sb.length() - 1);
				sb.append(")");
				O.log(sb.toString());
				statement.executeUpdate(sb.toString());
			}
		}
	}

	public String[][] getData(Table table) throws Exception{
		try(Connection conn = getConnection()) {
			
			return null;
		}
	}

	public void putData(Table table, Object[][] data) throws Exception{
		String insertSql = "INSERT INTO " + table.getSchema()+"."+ table.getName() + "( "+JDBCUtils.csColumnNames(table.getColumns()) +") VALUES(";
		for (int i = 0; i <table.getColumns().size(); i++) {
			insertSql = insertSql + "?,";
		}
		insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";
		
		try (Connection conn = getConnection(); PreparedStatement insertStmt = conn.prepareStatement(insertSql);) {
			for (int i = 0; i < data.length; i++) {
				Object[] col = data[i];
				for (int j = 0; j < col.length; j++) {
					Object value = col[j];
					addFieldValue(table.getColumns().get(j).getType(),j+1,value,insertStmt);
				}
				insertStmt.addBatch();
			}
			O.log(insertSql);
			insertStmt.executeBatch();
		}
	}

	public void checkTable(Table table) throws Exception {
		for(Column col: table.getColumns()) {
			switch(col.getType().toLowerCase()) {
				case "decimal": {
					assertIsTrue(col.getPrecision() < 128, "The precision of decimal should be less than 128!");
					break;
				}
				case "numeric": {
					assertIsTrue(col.getPrecision() < 128, "The precision of numeric should be less than 128!");
					break;
				}
			}
		}
	}
	
	public void assertIsTrue(boolean b) {
		assertIsTrue(b, "Unsurported type!");
	}
	
	public void assertIsTrue(boolean b, String message) {
		if(!b) {
			throw new ColumnTypeUnsurported(message);
		}
	}

	public Connection getConnection() throws SQLException {
		SnappyConfig config = getConfig();
		if(bds == null) {
			bds = new BasicDataSource();
			bds.setUrl(config.getUrl());
			bds.setDriverClassName("com.mysql.jdbc.Driver");
			bds.setUsername(config.getUser());
			bds.setPassword(config.getPassword());
			bds.setMaxActive(100);
		}
		return bds.getConnection();
	}
	
 
	public void addFieldValue(String fieldType, int fieldPosition, Object fieldValue,
			PreparedStatement commandStatement) throws Exception {
		switch (fieldType.toLowerCase()) {
		case "blob":
		case "char for big data":
		case "long varchar for big data":
		case "varchar for big data":
			byte[] byteValue = (byte[]) fieldValue;
			if (byteValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BLOB);
			} else {
				commandStatement.setBytes(fieldPosition, byteValue);
			}
			break;
		case "char":
			String strValue = (String) fieldValue;
			if ((strValue == null || strValue.isEmpty()) || (strValue.equals("null")) || (strValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CHAR);
			} else {
				commandStatement.setString(fieldPosition, strValue);
			}
			break;
		case "varchar":
			String strValues = (String) fieldValue;
			if ((strValues == null || strValues.isEmpty()) || (strValues.equals("null"))
					|| (strValues.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.VARCHAR);
			} else {
				commandStatement.setString(fieldPosition, strValues);
			}
			break;
		case "long varchar":
			String strLongValues = (String) fieldValue;
			if ((strLongValues == null || strLongValues.isEmpty()) || (strLongValues.equals("null"))
					|| (strLongValues.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.LONGNVARCHAR);
			} else {
				commandStatement.setString(fieldPosition, strLongValues);
			}
			break;
		case "xml":
			String txtValue = (String) fieldValue;
			if ((txtValue == null || txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
			} else {
				commandStatement.setString(fieldPosition, txtValue);
			}
			break;
		case "smallint":
			Short shortValue = (Short) fieldValue;
			if (shortValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.SMALLINT);
			} else {
				commandStatement.setInt(fieldPosition, shortValue);
			}
			break;
		case "integer":
			Integer intValue = (Integer) fieldValue;
			if (intValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.INTEGER);
			} else {
				commandStatement.setInt(fieldPosition, intValue);
			}
			break;

		case "bigint":
			Long longValue = (Long) fieldValue;
			if (longValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.BIGINT);
			} else {
				commandStatement.setLong(fieldPosition, longValue);
			}
			break;

		case "real":
			Float realValue = (Float) fieldValue;
			if (realValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.REAL);
			} else {
				commandStatement.setFloat(fieldPosition, realValue);
			}
			break;
		case "float":
			Float floatValue = (Float) fieldValue;
			if (floatValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.FLOAT);
			} else {
				commandStatement.setFloat(fieldPosition, floatValue);
			}
			break;
		case "double":
			Double doubleValue = (Double) fieldValue;
			if (doubleValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DOUBLE);
			} else {
				commandStatement.setDouble(fieldPosition, doubleValue);
			}
			break;
		case "decimal":
		case "numeric":
			BigDecimal numbericValue = (BigDecimal) fieldValue;
			if ((numbericValue == null) || (numbericValue.equals("null")) || (numbericValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
			} else {
				commandStatement.setBigDecimal(fieldPosition, numbericValue);
			}
			break;
		case "date":
			Date dateValue = (Date) fieldValue;
			if (dateValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.DATE);
			} else {
				commandStatement.setDate(fieldPosition, dateValue);
			}
			break;
		case "time":
			Time timeValue = (Time) fieldValue;

			if (timeValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIME);
			} else {
				commandStatement.setTime(fieldPosition, timeValue);
			}
			break;
		case "timestamp":
			Timestamp timestampValue = (Timestamp) fieldValue;
			if (timestampValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP);
			} else {
				commandStatement.setTimestamp(fieldPosition, timestampValue);
			}
			break;
		default: {
			if(fieldValue==null){
				commandStatement.setNull(fieldPosition, java.sql.Types.LONGNVARCHAR);
			}else{
				commandStatement.setString(fieldPosition, (String)fieldValue);
			}
		}
		
		}
	}
}
