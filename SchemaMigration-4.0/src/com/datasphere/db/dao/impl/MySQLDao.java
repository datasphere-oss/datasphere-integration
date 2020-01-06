package com.datasphere.db.dao.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.common.utils.O;
import com.datasphere.db.config.MySQLConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.datasphere.db.exception.ColumnTypeUnsurported;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;


public class MySQLDao extends AbstractBaseDao {

	private final static String SQL_SELECT_TABLE_ATTR = "select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE,NUMERIC_PRECISION,NUMERIC_SCALE,column_comment from information_schema.columns where table_schema = ? and table_name = ?";
	MysqlConnectionPoolDataSource  mysqlConnectionPoolDataSource;
	
	public Connection getConnection() {
		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	MysqlConnectionPoolDataSource getDataSource() throws SQLException {
		if(mysqlConnectionPoolDataSource == null) {
			MySQLConfig mysqlConfig = getConfig();
			mysqlConnectionPoolDataSource = new MysqlConnectionPoolDataSource();
			mysqlConnectionPoolDataSource.setServerName(mysqlConfig.getHost());
			mysqlConnectionPoolDataSource.setPortNumber(mysqlConfig.getPort());
			mysqlConnectionPoolDataSource.setDatabaseName(mysqlConfig.getDatabaseName());
			mysqlConnectionPoolDataSource.setUser(mysqlConfig.getUser());
			mysqlConnectionPoolDataSource.setPassword(mysqlConfig.getPassword());
		}
		return mysqlConnectionPoolDataSource;
	}
	
	public List<Schema> getSchemas() throws Exception{
		List<Schema> schemas = new LinkedList<Schema>();
		for(String schema : getSchemaTables().keySet()){
			schemas.add(new Schema(schema));
		}
		return schemas;
	}


	/**
	 * 创建schema
	 */
	public void createSchemas(List<Schema> destSchemas) throws Exception{
		try (Connection conn = getConnection(); Statement statement = conn.createStatement();) {
			for (Schema schema : destSchemas) {
				try {
					String isScheamsExistQuery="select count(distinct(table_schema)) from information_schema.tables where table_schema= '"+schema.getName().toLowerCase()+"'";
					//创建MySQL的Schemas
					ResultSet isScheamsExistRset = statement.executeQuery(isScheamsExistQuery);
					if (isScheamsExistRset.next()) {
						int isScheamsExist = isScheamsExistRset.getInt(1);
						if(isScheamsExist==0){
							statement.executeUpdate("create schema " + schema.getName() +" default character set utf8");
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 查询库中所有表
	 * 
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Set<String>> getSchemaTables() throws Exception{
		HashMap<String,Set<String>> tableMap = new HashMap<>();
		try(Connection conn = getConnection();PreparedStatement pst = conn.prepareStatement("select table_schema,table_name from information_schema.tables");) {
			ResultSet rset = pst.executeQuery();
			while (rset.next()) {
				String schemaName = rset.getString(1);
				String tableName = rset.getString(2);
				if(!tableMap.containsKey(schemaName)){
					Set<String> tns = new HashSet<String>();
					tns.add(tableName);
					tableMap.put(schemaName, tns);
				}else{
					Set<String> tns = tableMap.get(schemaName);
					tns.add(tableName);
				}
			}
			
			return tableMap;
		}

	}
	
	@Override
	public List<Table> getTables() throws Exception{
		try(
				Connection conn = getConnection();
				PreparedStatement pst = conn.prepareStatement(SQL_SELECT_TABLE_ATTR)) {
				List<Table> tables = new LinkedList<Table>();
				HashMap<String,Set<String>> schemaTables = getSchemaTables();
				for(String schemaName : schemaTables.keySet()){
					for(String tableName : schemaTables.get(schemaName)){
						Table table = new Table(tableName,schemaName);
						pst.setString(1, schemaName);
						pst.setString(2, tableName);
						ResultSet set = pst.executeQuery();
						while(set.next()) {
							Column column = new Column();
							String columnName = set.getString(1);
							String columnType = set.getString(2);
							Long columnLength = set.getLong(3);
							Boolean isNotNull = "YES".equals(set.getString(4)) ? true : false;
							Integer precision = set.getInt(5);
							Integer scale = set.getInt(6);
							String comments = set.getString(7);
							
							column.setTableName(tableName);
							column.setName(columnName);
							column.setType(columnType);
							column.setLength(columnLength);
							column.setNotnull(isNotNull);
							column.setPrecision(precision);
							column.setScale(scale);
							column.setComment(comments);
							O.log(column);
							table.addColumn(column);
						}
						tables.add(table);
					}
				}
				return tables;
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

	
	public void addFieldValue(String fieldType, int fieldPosition, Object fieldValue,
			PreparedStatement commandStatement) throws Exception {
		String type = fieldType.toLowerCase().substring(0,fieldType.indexOf("(")>-1?fieldType.indexOf("("):fieldType.length());
		switch (type) {
		case "boolean":
			Integer booleanValue = (Integer) fieldValue;
			if (booleanValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TINYINT);
			} else {
				commandStatement.setInt(fieldPosition, booleanValue);
			}
			break;
		case "blob":
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
		case "text":
			String txtValue = (String) fieldValue;
			if ((txtValue == null || txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL"))) {
				commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
			} else {
				commandStatement.setString(fieldPosition, txtValue);
			}
			break;
			
		case "tinyint":
			Short tinyValue = (Short) fieldValue;
			if (tinyValue == null) {
				commandStatement.setNull(fieldPosition, java.sql.Types.TINYINT);
			} else {
				commandStatement.setInt(fieldPosition, tinyValue);
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
		case "int":
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
		case "datetime":	
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
	
	public String getMySQLType(String sourceType, Integer sourceLength)
	   {
	     String MySQLType = "";
	     switch (sourceType.toUpperCase())
	     {
	   
		  case "BINARY":
		  case "VARBINARY":
			  	  
			 MySQLType = "blob";
	        break;
		  case "BIT":
		  case "BOOLEAN":
			 MySQLType = "boolean";
			 break;
		  case "CHAR":
			 MySQLType = "char(" + sourceLength + ")";
			 break; 
		  case "NCHAR":
		  case "VARCHAR":
		  case "NVARCHAR":  
		  case "SYSNAME":
			  MySQLType = "varchar(" + sourceLength + ")";
		    break;

		  case "TEXT":
		  case "NTEXT":
			  MySQLType = "text";
	       break;
		  case "TINYINT":
			  MySQLType = "tinyint";
			break;
		  case "SMALLINT":
			  MySQLType = "smallint";
			break;
		  case "INT":
		  case "INTEGER":
			  MySQLType = "int";
				break;
		  case "BIGINT":
			  MySQLType = "bigint";
				break;
		  case "DOUBLE":
			  MySQLType = "double";
				break;
						
		  case "FLOAT":
			  MySQLType = "float";
				break;
		  case "REAL":
		  case "SMALLMONEY":
		  case "MONEY":
		  case "NUMERIC":
		  case "DECIMAL":
			  MySQLType = "decimal";
				break;
		  case "DATE":
			  MySQLType = "date";
				break;
		  case "TIME":
			  MySQLType = "time";
				break;
		  case "SMALLDATETIME":
		  case "DATETIME":
			  MySQLType = "datetime";
				break;
		  case "TIMESTAMP":
			  MySQLType = "timestamp";
				break;
		      	
	     }
	 
	     return MySQLType;
	   }
	
}
