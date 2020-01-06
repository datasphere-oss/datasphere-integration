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
import java.util.List;
import java.util.concurrent.Executors;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.common.utils.O;
import com.datasphere.db.config.HiveConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.datasphere.db.exception.ColumnTypeUnsurported;

public class HiveDao extends AbstractBaseDao {

	public static int HIVE_CHAR_MAX_LENGTH = 254;

	Connection conn;
	
	public List<Schema> getSchemas() throws Exception{
		return null;
	}

	/**
	 * 创建schema
	 */
	public void createSchemas(List<Schema> destSchemas) throws Exception{
		HiveConfig config = getConfig();

		Class.forName("org.apache.hive.jdbc.HiveDriver");
		conn = DriverManager.getConnection("jdbc:hive2://"+config.getHost()+":"+config.getPort()+"/"+config.getDatabaseName(), config.getUser(), config.getPassword());
		Statement statement = conn.createStatement();
		try {
			for (Schema schema : destSchemas) {
				try {
					String isSchemasExistQuery="SHOW DATABASES";
					//创建Hive的Databases/Schemas
					ResultSet isScheamsExistRset = statement.executeQuery(isSchemasExistQuery);
					if (isScheamsExistRset.next()) {
						String scheamExist = isScheamsExistRset.getString(1);
						
						if(!scheamExist.equals(schema.getName())){
							statement.executeUpdate("create database " + schema.getName());
						    System.out.println("Database "+ schema.getName() +"created successfully.");

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		finally {
			statement.close();
			conn.close();
		}
	}

	public List<Table> getTables() throws Exception{
		return null;
	}

	/**
	 * 创建表- Hive
	 */
	public void createTables(List<Table> destTables) throws Exception{
		HiveConfig config = getConfig();
		
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		conn = DriverManager.getConnection("jdbc:hive2://"+config.getHost()+":"+config.getPort()+"/"+config.getDatabaseName(), config.getUser(), config.getPassword());
			
		
		Statement statement = conn.createStatement();
		try{
			for (Table table : destTables) {
				// 表名关键字替换
				excludeKeywords(table);
				
				statement.executeUpdate("drop table if exists " + table.getSchema() + "." + table.getName());
				System.out.println("schema:"+table.getSchema());
				StringBuilder sb = new StringBuilder();
				sb.append("create table if not exists " + table.getSchema() + "." + table.getName());

				sb.append("(");
				for(Column column: table.getColumns()) {
					// 列名关键字替换
					excludeKeywords(column);
					System.out.println("column_type:"+column.getType());
					sb.append(column.getName() + " ");
					String type = column.getType().toLowerCase().substring(0,column.getType().indexOf("(")>-1?column.getType().indexOf("("):column.getType().length());
					switch(type) {
						// oracle datatype
						
						case "tinyblob":
						case "mediumblob":
						case "longblob":
						case "blob":
						case "binary":
						case "bytea":	
						case "varbinary": {
							sb.append("binary");
							break;
						}
					
						case "tinyint": {
							sb.append("tinyint");
							break;
						}
						case "bitint":
						case "smallint": {
							sb.append("smallint");
							break;
						}
						case "int2":
						case "int4":
						case "int8":
						case "int": 
					    case "integer":

						{
							sb.append("int");
							break;
						}
						case "bigint": {
							sb.append("bigint");
							break;
						}
						case "float": {
							sb.append("float");
							break;
						}
						case "double": {
							sb.append("double");
							break;
						}
						case "bool":
						case "bit":
						case "boolean": {
							sb.append("boolean");
							break;
						}
						// oracle datatype 
						case "graphic":
					    case "character":
					    	 
						case "char": {
							if(column.getLength() == null || column.getLength() <= 0) {
								sb.append("char(1)");
							}else if( column.getLength()  <= HIVE_CHAR_MAX_LENGTH){
								sb.append("char(" + column.getLength() +")");
							} else if(column.getLength()  > HIVE_CHAR_MAX_LENGTH)
								sb.append("char(254)");
							break;
						}
						// oracle datatype
					     case "nchar varying":
					     case "nvarchar":
					     case "clob":
					     case "ntext":
					    	 
					     case "nchar":
					     case "nvarchar2":
					     case "tinytext":
					     case "text":
					     case "mediumtext":
					     case "longtext":
					     case "json":
					     case "string":
					     case "timestamptz": {
							sb.append("string");
							break;
						}
					     // oracle datatype
					     case "money":
					     case "smallmoney":
					     case "decfloat":
					     case "real":
					     case "number":
					    	 
						case "numeric": 
						case "decimal": {
							if(column.getLength() == null || column.getLength() <= 0) {
								sb.append("decimal(" + 1 + "," + 1 +")");
							}else if( column.getLength()  <= 38){
								if(column.getScale() >= column.getLength())
									sb.append("decimal(" + column.getLength() + "," + column.getLength() +")");
								if(column.getScale() < column.getLength())
									sb.append("decimal(" + column.getLength() + "," + column.getScale() +")");
							} else if(column.getLength()  > 38)
								if(column.getScale() >= column.getLength())
									sb.append("decimal(" + 38 + "," + 38 +")");
								if(column.getScale() < column.getLength())
									sb.append("decimal(" + column.getLength() + "," + column.getScale() +")");
								
							break;
						}
						
						// oracle datatype
						case "varchar2":
						case "sysname":
						case "vargraphic":
						case "varg":
						case "uniqueidentifier":
						    	 
						case "varchar": {
							if(column.getLength() == null || column.getLength() <= 0) {
								sb.append("varchar(1)");
							}else if( column.getLength()  <= 65535){
								sb.append("varchar(" + column.getLength() +")");
							} else if(column.getLength()  > 65535)
								sb.append("varchar(" + 65535 +")");
							break;
						}
					    	 
						case "date": 
							sb.append("timestamp");
							break;
						case "time": 
						case "datetime": 
						case "timestamp":
						
						//oracle datatype
						case "time with time zone":
						case "timestamp with time zone":
						case "timestamp with local time zone":
						case "timestmp":
						{
							sb.append("timestamp");
							break;
						}
						
						
						case "enum":
						case "set":	{
							sb.append("string");
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
				sb.append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' "
						+ "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' "
						+ "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' ");
				O.log(sb.toString());
				statement.executeUpdate(sb.toString());
			}
			
		}finally {
			conn.close();
		}
	}

	
	public void excludeKeywords(Object obj) {
		// 表名转换
		if(obj instanceof Table) {
			switch(((Table)obj).getName().toUpperCase()) {
				case "ALL":
					((Table)obj).setName("ALL_");
					break;
				case "ALTER":
					((Table)obj).setName("ALTER_");
					break;
				case "AND":
					((Table)obj).setName("AND_");
					break;
				case "ARRAY":
					((Table)obj).setName("ARRAY_");
					break;
				case "AS":
					((Table)obj).setName("AS_");
					break;
				case "AUTHORIZATION":
					((Table)obj).setName("AUTHORIZATION_");
					break;
				case "BETWEEN":
					((Table)obj).setName("BETWEEN_");
					break;
				case "BIGINT":
					((Table)obj).setName("BIGINT_");
					break;
				case "BINARY":
					((Table)obj).setName("BINARY_");
					break;
				case "BOOLEAN":
					((Table)obj).setName("BOOLEAN_");
					break;
				case "BOTH":
					((Table)obj).setName("BOTH_");
					break;
				case "BY":
					((Table)obj).setName("BY_");
					break;
				case "CASE":
					((Table)obj).setName("CASE_");
					break;
				case "CAST":
					((Table)obj).setName("CAST_");
					break;
				case "CHAR":
					((Table)obj).setName("CHAR_");
					break;
				case "COLUMN":
					((Table)obj).setName("COLUMN_");
					break;
				case "CREATE":
					((Table)obj).setName("CREATE_");
					break;
				case "CROSS":
					((Table)obj).setName("CROSS_");
					break;
				case "CUBE":
					((Table)obj).setName("CUBE_");
					break;
				case "CURRENT":
					((Table)obj).setName("CURRENT_");
					break;
				case "CURRENT_DATE":
					((Table)obj).setName("CURRENT_DATE_");
					break;
				case "CURSOR":
					((Table)obj).setName("CURSOR_");
					break;
				case "DATABASE":
					((Table)obj).setName("DATABASE_");
					break;
				case "DATE":
					((Table)obj).setName("DATE_");
					break;
				case "DECIMAL":
					((Table)obj).setName("DECIMAL_");
					break;
				case "DELETE":
					((Table)obj).setName("DELETE_");
					break;
				case "DESCRIBE":
					((Table)obj).setName("DESCRIBE_");
					break;
				case "DISTINCT":
					((Table)obj).setName("DISTINCT_");
					break;
				case "DIV":
					((Table)obj).setName("DIV_");
					break;
				case "DOUBLE":
					((Table)obj).setName("DOUBLE_");
					break;
				case "DROP":
					((Table)obj).setName("DROP_");
					break;
				case "ELSE":
					((Table)obj).setName("ELSE_");
					break;
				case "END":
					((Table)obj).setName("END_");
					break;
				case "END-EXEC":
					((Table)obj).setName("END-EXEC_");
					break;
				case "EXISTS":
					((Table)obj).setName("EXISTS_");
					break;
				case "EXTERNAL":
					((Table)obj).setName("EXTERNAL_");
					break;
				case "FALSE":
					((Table)obj).setName("FALSE_");
					break;
				case "FETCH":
					((Table)obj).setName("FETCH_");
					break;
				case "FLOAT":
					((Table)obj).setName("FLOAT_");
					break;
				case "FOLLOWING":
					((Table)obj).setName("FOLLOWING_");
					break;
				case "FOR":
					((Table)obj).setName("FOR_");
					break;
				case "FROM":
					((Table)obj).setName("FROM_");
					break;
				case "FULL":
					((Table)obj).setName("FULL_");
					break;
				case "FUNCTION":
					((Table)obj).setName("FUNCTION_");
					break;
				case "GRANT":
					((Table)obj).setName("GRANT_");
					break;
				case "GROUP":
					((Table)obj).setName("GROUP_");
					break;
				case "GROUPING":
					((Table)obj).setName("GROUPING_");
					break;
				case "HAVING":
					((Table)obj).setName("HAVING_");
					break;
				case "IF":
					((Table)obj).setName("IF_");
					break;
				case "IN":
					((Table)obj).setName("IN_");
					break;
				case "INNER":
					((Table)obj).setName("INNER_");
					break;
				case "INSERT":
					((Table)obj).setName("INSERT_");
					break;
				case "INT":
					((Table)obj).setName("INT_");
					break;
				case "INTERSECT":
					((Table)obj).setName("INTERSECT_");
					break;
				case "INTERVAL":
					((Table)obj).setName("INTERVAL_");
					break;
				case "INTO":
					((Table)obj).setName("INTO_");
					break;
				case "IS":
					((Table)obj).setName("IS_");
					break;
				case "JOIN":
					((Table)obj).setName("JOIN_");
					break;
				case "LATERAL":
					((Table)obj).setName("LATERAL_");
					break;
				case "LEFT":
					((Table)obj).setName("LEFT_");
					break;
				case "LESS":
					((Table)obj).setName("LESS_");
					break;
				case "LIKE":
					((Table)obj).setName("LIKE_");
					break;
				case "LOCAL":
					((Table)obj).setName("LOCAL_");
					break;
				case "MAP":
					((Table)obj).setName("MAP_");
					break;
				case "MERGE":
					((Table)obj).setName("MERGE_");
					break;
				case "MINUS":
					((Table)obj).setName("MINUS_");
					break;
				case "MORE":
					((Table)obj).setName("MORE_");
					break;
				case "NONE":
					((Table)obj).setName("NONE_");
					break;
					
				case "NOT":
					((Table)obj).setName("NOT_");
					break;
				case "NULL":
					((Table)obj).setName("NULL_");
					break;
				case "OF":
					((Table)obj).setName("OF_");
					break;
				case "ON":
					((Table)obj).setName("ON_");
					break;
				case "OR":
					((Table)obj).setName("OR_");
					break;
				case "ORDER":
					((Table)obj).setName("ORDER_");
					break;
				case "OUT":
					((Table)obj).setName("OUT_");
					break;
				case "OUTER":
					((Table)obj).setName("OUTER_");
					break;
				case "OVER":
					((Table)obj).setName("OVER_");
					break;
				case "PARTITION":
					((Table)obj).setName("PARTITION_");
					break;
				case "PERCENT":
					((Table)obj).setName("PERCENT_");
					break;
				case "PRECEDING":
					((Table)obj).setName("PRECEDING_");
					break;
				case "PRESERVE":
					((Table)obj).setName("PRESERVE_");
					break;
				case "PROCEDURE":
					((Table)obj).setName("PROCEDURE_");
					break;
				case "RANGE":
					((Table)obj).setName("RANGE_");
					break;
				case "READS":
					((Table)obj).setName("READS_");
					break;
				case "REGEXP":
					((Table)obj).setName("REGEXP_");
					break;
				case "REVOKE":
					((Table)obj).setName("REVOKE_");
					break;
				case "RIGHT":
					((Table)obj).setName("RIGHT_");
					break;
				case "RLIKE":
					((Table)obj).setName("RLIKE_");
					break;
				case "ROLLUP":
					((Table)obj).setName("ROLLUP_");
					break;
				case "ROW":
					((Table)obj).setName("ROW_");
					break;
				case "ROWS":
					((Table)obj).setName("ROWS_");
					break;
				case "SELECT":
					((Table)obj).setName("SELECT_");
					break;
				case "SET":
					((Table)obj).setName("SET_");
					break;
				case "SMALLINT":
					((Table)obj).setName("SMALLINT_");
					break;
				case "TABLE":
					((Table)obj).setName("TABLE_");
					break;
				case "TABLESAMPLE":
					((Table)obj).setName("TABLESAMPLE_");
					break;
				case "THEN":
					((Table)obj).setName("THEN_");
					break;
				case "TIMESTAMP":
					((Table)obj).setName("TIMESTAMP_");
					break;
				case "TO":
					((Table)obj).setName("TO_");
					break;
				case "TRANSFORM":
					((Table)obj).setName("TRANSFORM_");
					break;
				case "TRIGGER":
					((Table)obj).setName("TRIGGER_");
					break;
				case "TRUE":
					((Table)obj).setName("TRUE_");
					break;
				case "TRUNCATE":
					((Table)obj).setName("TRUNCATE_");
					break;
				case "UNBOUNDED":
					((Table)obj).setName("UNBOUNDED_");
					break;
				case "UNION":
					((Table)obj).setName("UNION_");
					break;
				case "UPDATE":
					((Table)obj).setName("UPDATE_");
					break;
				case "USER":
					((Table)obj).setName("USER_");
					break;
				case "USING":
					((Table)obj).setName("USING_");
					break;
				case "VALUES":
					((Table)obj).setName("VALUES_");
					break;
				case "VARCHAR":
					((Table)obj).setName("VARCHAR_");
					break;
				case "WHEN":
					((Table)obj).setName("WHEN_");
					break;
				case "WHERE":
					((Table)obj).setName("WHERE_");
					break;
				case "WINDOW":
					((Table)obj).setName("WINDOW_");
					break;
				case "WITH":
					((Table)obj).setName("WITH_");
					break;
				
			}
			
			if(((Table)obj).getName().contains("$"))
				((Table)obj).setName(((Table)obj).getName().replace("$", "_"));
			
		}
		// 列名转换
		if(obj instanceof Column) {
			switch(((Column)obj).getName().toUpperCase()) {
				case "ALL":
					((Column)obj).setName("ALL_");
					break;
				case "ALTER":
					((Column)obj).setName("ALTER_");
					break;
				case "AND":
					((Column)obj).setName("AND_");
					break;
				case "ARRAY":
					((Column)obj).setName("ARRAY_");
					break;
				case "AS":
					((Column)obj).setName("AS_");
					break;
				case "AUTHORIZATION":
					((Column)obj).setName("AUTHORIZATION_");
					break;
				case "BETWEEN":
					((Column)obj).setName("BETWEEN_");
					break;
				case "BIGINT":
					((Column)obj).setName("BIGINT_");
					break;
				case "BINARY":
					((Column)obj).setName("BINARY_");
					break;
				case "BOOLEAN":
					((Column)obj).setName("BOOLEAN_");
					break;
				case "BOTH":
					((Column)obj).setName("BOTH_");
					break;
				case "BY":
					((Column)obj).setName("BY_");
					break;
				case "CASE":
					((Column)obj).setName("CASE_");
					break;
				case "CAST":
					((Column)obj).setName("CAST_");
					break;
				case "CHAR":
					((Column)obj).setName("CHAR_");
					break;
				case "COLUMN":
					((Column)obj).setName("COLUMN_");
					break;
				case "CREATE":
					((Column)obj).setName("CREATE_");
					break;
				case "CROSS":
					((Column)obj).setName("CROSS_");
					break;
				case "CUBE":
					((Column)obj).setName("CUBE_");
					break;
				case "CURRENT":
					((Column)obj).setName("CURRENT_");
					break;
				case "CURRENT_DATE":
					((Column)obj).setName("CURRENT_DATE_");
					break;
				case "CURSOR":
					((Column)obj).setName("CURSOR_");
					break;
				case "DATABASE":
					((Column)obj).setName("DATABASE_");
					break;
				case "DATE":
					((Column)obj).setName("DATE_");
					break;
				case "DECIMAL":
					((Column)obj).setName("DECIMAL_");
					break;
				case "DELETE":
					((Column)obj).setName("DELETE_");
					break;
				case "DESCRIBE":
					((Column)obj).setName("DESCRIBE_");
					break;
				case "DISTINCT":
					((Column)obj).setName("DISTINCT_");
					break;
				case "DIV":
					((Column)obj).setName("DIV_");
					break;
				case "DOUBLE":
					((Column)obj).setName("DOUBLE_");
					break;
				case "DROP":
					((Column)obj).setName("DROP_");
					break;
				case "ELSE":
					((Column)obj).setName("ELSE_");
					break;
				case "END":
					((Column)obj).setName("END_");
					break;
				case "END-EXEC":
					((Column)obj).setName("END-EXEC_");
					break;
				case "EXISTS":
					((Column)obj).setName("EXISTS_");
					break;
				case "EXTERNAL":
					((Column)obj).setName("EXTERNAL_");
					break;
				case "FALSE":
					((Column)obj).setName("FALSE_");
					break;
				case "FETCH":
					((Column)obj).setName("FETCH_");
					break;
				case "FLOAT":
					((Column)obj).setName("FLOAT_");
					break;
				case "FOLLOWING":
					((Column)obj).setName("FOLLOWING_");
					break;
				case "FOR":
					((Column)obj).setName("FOR_");
					break;
				case "FROM":
					((Column)obj).setName("FROM_");
					break;
				case "FULL":
					((Column)obj).setName("FULL_");
					break;
				case "FUNCTION":
					((Column)obj).setName("FUNCTION_");
					break;
				case "GRANT":
					((Column)obj).setName("GRANT_");
					break;
				case "GROUP":
					((Column)obj).setName("GROUP_");
					break;
				case "GROUPING":
					((Column)obj).setName("GROUPING_");
					break;
				case "HAVING":
					((Column)obj).setName("HAVING_");
					break;
				case "IF":
					((Column)obj).setName("IF_");
					break;
				case "IN":
					((Column)obj).setName("IN_");
					break;
				case "INNER":
					((Column)obj).setName("INNER_");
					break;
				case "INSERT":
					((Column)obj).setName("INSERT_");
					break;
				case "INT":
					((Column)obj).setName("INT_");
					break;
				case "INTERSECT":
					((Column)obj).setName("INTERSECT_");
					break;
				case "INTERVAL":
					((Column)obj).setName("INTERVAL_");
					break;
				case "INTO":
					((Column)obj).setName("INTO_");
					break;
				case "IS":
					((Column)obj).setName("IS_");
					break;
				case "JOIN":
					((Column)obj).setName("JOIN_");
					break;
				case "LATERAL":
					((Column)obj).setName("LATERAL_");
					break;
				case "LEFT":
					((Column)obj).setName("LEFT_");
					break;
				case "LESS":
					((Column)obj).setName("LESS_");
					break;
				case "LIKE":
					((Column)obj).setName("LIKE_");
					break;
				case "LOCAL":
					((Column)obj).setName("LOCAL_");
					break;
				case "MAP":
					((Column)obj).setName("MAP_");
					break;
				case "MERGE":
					((Column)obj).setName("MERGE_");
					break;
				case "MINUS":
					((Column)obj).setName("MINUS_");
					break;
				case "MORE":
					((Column)obj).setName("MORE_");
					break;
				case "NONE":
					((Column)obj).setName("NONE_");
					break;
					
				case "NOT":
					((Column)obj).setName("NOT_");
					break;
				case "NULL":
					((Column)obj).setName("NULL_");
					break;
				case "OF":
					((Column)obj).setName("OF_");
					break;
				case "ON":
					((Column)obj).setName("ON_");
					break;
				case "OR":
					((Column)obj).setName("OR_");
					break;
				case "ORDER":
					((Column)obj).setName("ORDER_");
					break;
				case "OUT":
					((Column)obj).setName("OUT_");
					break;
				case "OUTER":
					((Column)obj).setName("OUTER_");
					break;
				case "OVER":
					((Column)obj).setName("OVER_");
					break;
				case "PARTITION":
					((Column)obj).setName("PARTITION_");
					break;
				case "PERCENT":
					((Column)obj).setName("PERCENT_");
					break;
				case "PRECEDING":
					((Column)obj).setName("PRECEDING_");
					break;
				case "PRESERVE":
					((Column)obj).setName("PRESERVE_");
					break;
				case "PROCEDURE":
					((Column)obj).setName("PROCEDURE_");
					break;
				case "RANGE":
					((Column)obj).setName("RANGE_");
					break;
				case "READS":
					((Column)obj).setName("READS_");
					break;
				case "REGEXP":
					((Column)obj).setName("REGEXP_");
					break;
				case "REVOKE":
					((Column)obj).setName("REVOKE_");
					break;
				case "RIGHT":
					((Column)obj).setName("RIGHT_");
					break;
				case "RLIKE":
					((Column)obj).setName("RLIKE_");
					break;
				case "ROLLUP":
					((Column)obj).setName("ROLLUP_");
					break;
				case "ROW":
					((Column)obj).setName("ROW_");
					break;
				case "ROWS":
					((Column)obj).setName("ROWS_");
					break;
				case "SELECT":
					((Column)obj).setName("SELECT_");
					break;
				case "SET":
					((Column)obj).setName("SET_");
					break;
				case "SMALLINT":
					((Column)obj).setName("SMALLINT_");
					break;
				case "TABLE":
					((Column)obj).setName("TABLE_");
					break;
				case "TABLESAMPLE":
					((Column)obj).setName("TABLESAMPLE_");
					break;
				case "THEN":
					((Column)obj).setName("THEN_");
					break;
				case "TIMESTAMP":
					((Column)obj).setName("TIMESTAMP_");
					break;
				case "TO":
					((Column)obj).setName("TO_");
					break;
				case "TRANSFORM":
					((Column)obj).setName("TRANSFORM_");
					break;
				case "TRIGGER":
					((Column)obj).setName("TRIGGER_");
					break;
				case "TRUE":
					((Column)obj).setName("TRUE_");
					break;
				case "TRUNCATE":
					((Column)obj).setName("TRUNCATE_");
					break;
				case "UNBOUNDED":
					((Column)obj).setName("UNBOUNDED_");
					break;
				case "UNION":
					((Column)obj).setName("UNION_");
					break;
				case "UPDATE":
					((Column)obj).setName("UPDATE_");
					break;
				case "USER":
					((Column)obj).setName("USER_");
					break;
				case "USING":
					((Column)obj).setName("USING_");
					break;
				case "VALUES":
					((Column)obj).setName("VALUES_");
					break;
				case "VARCHAR":
					((Column)obj).setName("VARCHAR_");
					break;
				case "WHEN":
					((Column)obj).setName("WHEN_");
					break;
				case "WHERE":
					((Column)obj).setName("WHERE_");
					break;
				case "WINDOW":
					((Column)obj).setName("WINDOW_");
					break;
				case "WITH":
					((Column)obj).setName("WITH_");
					break;
				
			}
			
			if(((Column)obj).getName().contains("$"))
				((Column)obj).setName(((Column)obj).getName().replace("$", "_"));
			
		}
		
	}

	public String[][] getData(Table table) throws Exception{
		return null;
		
	}

	public void putData(Table table, Object[][] data) throws Exception{
		HiveConfig config = getConfig();

		if(conn == null) {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			conn = DriverManager.getConnection("jdbc:hive2://"+config.getHost()+":"+config.getPort()+"/"+config.getDatabaseName(), config.getUser(), config.getPassword());
		
		}
		
		String insertSql = "INSERT INTO " + table.getSchema()+"."+ table.getName() + "( "+JDBCUtils.csColumnNames(table.getColumns()) +") VALUES(";
		for (int i = 0; i <table.getColumns().size(); i++) {
			insertSql = insertSql + "?,";
		}
		insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";
		
		PreparedStatement insertStmt = conn.prepareStatement(insertSql);
		try {
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
		} finally {
			conn.close();
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
