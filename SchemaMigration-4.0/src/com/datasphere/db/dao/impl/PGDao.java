package com.datasphere.db.dao.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import org.postgresql.ds.PGConnectionPoolDataSource;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.common.utils.O;
import com.datasphere.db.config.HiveConfig;
import com.datasphere.db.config.PGConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;

public class PGDao extends AbstractBaseDao {
	
	PGConnectionPoolDataSource pgConnectionPoolDataSource;
	
	public Connection getConnection() {
		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	PGConnectionPoolDataSource getDataSource() {
		if(pgConnectionPoolDataSource == null) {
			PGConfig config = getConfig();
			pgConnectionPoolDataSource = new PGConnectionPoolDataSource();
			pgConnectionPoolDataSource.setServerName(config.getHost());
			pgConnectionPoolDataSource.setPortNumber(config.getPort());
			pgConnectionPoolDataSource.setDatabaseName(config.getDatabaseName());
			pgConnectionPoolDataSource.setUser(config.getUser());
			pgConnectionPoolDataSource.setPassword(config.getPassword());
		}
		return pgConnectionPoolDataSource;
	}
	
	public boolean isType(Column col, String type) {
		return col.getType().equalsIgnoreCase(type);
	}
	
	public List<Schema> getSchemas() throws Exception{
		List<Schema> schemas = new LinkedList<Schema>();
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			ResultSet set = conn.createStatement().executeQuery("select schemaname from pg_tables where schemaname != 'pg_catalog' and schemaname != 'information_schema'");
			while(set.next()) {
				schemas.add(new Schema(set.getString(1)));
			}
			return schemas;
		}
	}

	public void createSchemas(List<Schema> destSchemas) throws Exception{
		PGConfig config = getConfig();

		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+config.getHost()+":"+config.getPort()+"/"+config.getDatabaseName(), config.getUser(), config.getPassword());
		Statement statement = conn.createStatement();
		try {
			for (Schema schema : destSchemas) {
				try {
					String isSchemasExistQuery="select * from information_schema.tables";
					//创建Hive的Databases/Schemas
					ResultSet isScheamsExistRset = statement.executeQuery(isSchemasExistQuery);
					if (isScheamsExistRset.next()) {
						String scheamExist = isScheamsExistRset.getString(2);
						
						if(!scheamExist.equals(schema.getName())){
							statement.executeUpdate("drop schema if exists " + schema.getName().toLowerCase() +" cascade");

							statement.executeUpdate("create schema " + schema.getName().toLowerCase());
						    System.out.println("Schema "+ schema.getName() +" created successfully.");

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
		List<Table> tables = new LinkedList<Table>();
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			ResultSet set = stat.executeQuery("select * from pg_tables where schemaname != 'pg_catalog' and schemaname != 'information_schema'");
			while(set.next()) {
				Table table = new Table(set.getString(2), set.getString(1));
				try(Statement tempStat = conn.createStatement()) {
					String sql = "select ordinal_position, column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale, is_nullable from information_schema.columns where table_name = '" + table.getName() + "' and table_schema = '" + table.getSchema() + "'";
					ResultSet tempSet = tempStat.executeQuery(sql);
					while(tempSet.next()) {
						Column column = new Column();
						column.setName(tempSet.getString(2));
						column.setTableName(table.getName());
						column.setType(tempSet.getString(3));
						column.setLength(tempSet.getLong(4));
						column.setPrecision(tempSet.getInt(5));
						column.setScale(tempSet.getInt(6));
						column.setNotnull(tempSet.getBoolean(7));
						table.addColumn(column);
					}
				}
				tables.add(table);
			}
			return tables;
		}
	}

	public void createTables(List<Table> destTables) throws Exception{
		try (Connection conn = getConnection(); Statement statement = conn.createStatement();) {
			for (Table table : destTables) {
				// 如果表中字段不为空
				if(!table.getColumns().isEmpty()) {
					
					try(Statement tempStat = conn.createStatement()) {
						tempStat.executeUpdate("drop table if exists table " + table.getSchema().toLowerCase() + "." + table.getName().toLowerCase());
					} catch(Throwable t) {}
					
					StringBuilder sb = new StringBuilder();
					sb.append("create table " + table.getSchema().toLowerCase() + "." + table.getName().toLowerCase());

					sb.append("(");
					for(Column column: table.getColumns()) {
						// 列名关键字替换
						excludeKeywords(column);
						sb.append(column.getName() + " ");
						// 数据类型转换
						sb.append(getPGType(column.getType().toUpperCase(), column));
						
						// 设置主键
						if(column.getConstraint()!=null)
							sb.append(" PRIMARY KEY ");
						
						if(column.getNotnull())
							sb.append(" NULL ");
						else
							sb.append(" NOT NULL ");
						
//						sb.append(column.getComment());
						
						sb.append(",");
					}
					sb.deleteCharAt(sb.length() - 1);
					sb.append(") ");
					
					PGConfig pgConfig = getConfig();
					if(pgConfig.isDistribution())
						sb.append("distributed randomly");
						
					O.log(sb.toString());
					statement.executeUpdate(sb.toString());
				}
				
				
				
			}
		}
	}

	public Object[][] getData(Table table) throws Exception{
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			int row = 0;
			int col = table.getColumns().size();
			ResultSet set = stat.executeQuery("select count(*) from " + table.getSchema().toLowerCase() + ".\"" + table.getName().toLowerCase() + "\"");
			if(set.next()) {
				row = Long.valueOf(set.getLong(1)).intValue();
			}
			if(row == 0) {
				return null;
			}
			set = stat.executeQuery("select " + JDBCUtils.csColumnNames(table.getColumns()) +" from " + table.getSchema().toLowerCase() + ".\"" + table.getName().toLowerCase() + "\"");
			Object[][] data = new Object[row][col];
			int rowIndex = 0;
			while(set.next()) {
				if(rowIndex >= row) {
					break;
				}
				for(int colIndex = 0; colIndex < col; colIndex++) {
					if(isType(table.getColumns().get(colIndex), "int2") || isType(table.getColumns().get(colIndex), "serial2")) {
						data[rowIndex][colIndex] = set.getShort(colIndex + 1);
					} else {
						data[rowIndex][colIndex] = set.getObject(colIndex + 1);
					}
				}
				rowIndex++;
			}
			return data;
		}
	}

	public void putData(Table table, Object[][] data) throws Exception{
		try(Connection conn = getConnection()) {
			
		}
	}
	
	
	public void checkTable(Table table) throws Exception {
		
	}

	public void copyData() throws Exception{
		
	}
	
	public String getPGType(String sourceType, Column column)
	   {
	     String PGType = "";
	     String type = sourceType.toUpperCase().substring(0,sourceType.indexOf("(")>-1?sourceType.indexOf("("):sourceType.length());
	     switch (type)
	     {
	      case "TINYBLOB":
	      case "MEDIUMBLOB":
		  case "LONGBLOB":
	      case "BLOB":
		  case "BYTEA":	
		  case "BINARY":
		  case "VARBINARY":
		  case "IMAGE":
		  case "LONG RAW":
		  case "RAW":
			  PGType = "BYTEA";
			  break;
		  case "BOOL":
		  case "BIT":
		  case "BOOLEAN":
			  PGType = "BOOLEAN";
			  break;
		  case "GRAPHIC":
		  case "CHARACTER":
		  case "CHAR":
			  PGType = "CHAR(" + column.getLength() + ")";
			  break;
		  case "VARGRAPHIC":
		  case "VARG":
		  case "NCHAR":
		  case "NCHAR VARYING":
		  case "VARCHAR":
		  case "NVARCHAR":  
		  case "SYSNAME":
			  PGType = "VARCHAR(" + column.getLength() + ")";
		    break;

		  case "LONG VARCHAR":
		  case "NCLOB":
 
		  // oracle datatype
		  case "CLOB":
		  case "VARCHAR2":  
		  case "NVARCHAR2":
		  case "TINYTEXT":
		  case "MEDIUMTEXT":
		  case "LONGTEXT":
		  case "JSON":
		  case "STRING":
			  
		  case "TEXT":
		  case "NTEXT":
			  PGType = "TEXT";
	       break;
		  case "TINYINT":
		  case "SMALLINT":
			  PGType = "SMALLINT";
			break;
		  case "INT2":
		  case "INT4":
		  case "INT8":
		  case "INT":
			  PGType = "INT";
			break;
		  case "INTEGER":
			  PGType = "INTEGER";
				break;
		  case "BIGINT":
			  PGType = "BIGINT";
				break;
		  case "DOUBLE":
		  case "BINARY_FLOAT":
		  case "BINARY_DOUBLE":
			  PGType = "DOUBLE PRECISION";
				break;
		  // oracle datatype		
		  case "DECFLOAT":
		  case "NUMBER":				
		  case "FLOAT":
		  case "REAL":
		  case "SMALLMONEY":
		  case "MONEY":
		  case "NUMERIC":
			  PGType = "NUMERIC";
				break;
		  case "DECIMAL":
			  PGType = "DECIMAL";
				break;
		  case "SMALLDATETIME":
			  PGType = "VARCHAR(100)";
				break;	
		  case "DATETIME":
			  PGType = "DATE";
				break;
		  case "DATE":
			  PGType = "DATE";
				break;
		  case "TIME":
		  case "TIME WITH TIME ZONE":
			  PGType = "TIME";
				break;
		  
		  case "TIMESTAMP WITH LOCAL TIME ZONE":
		  case "TIMESTAMP":
		  case "TIMESTMP":
//			  	if(dbType=="MSSQL")
//			  		PGType = "BYTEA";
//			  	else
//			  		PGType = "TIMESTAMP";
			  	PGType = "TIMESTAMP";
				break;
		  
		  case "UNIQUEIDENTIFIER":
			  PGType = "TEXT";
			  	break;
		  case "ROWID":
			  PGType = "VARCHAR";
			  	break;
		  case "XMLTYPE":
			  PGType = "TEXT";
		  case "SDO_GEOMETRY":
			  PGType = "TEXT";
			  
		  default:{
			  if(column.getType().toLowerCase().contains("interval"))
		    	 	PGType = "TEXT";
			  PGType = "TEXT";
		  }
			  	
	     }
	     
	     
	     return PGType;
	   }
	
	public void addFieldValue(String fieldType, int fieldPosition, Object fieldValue, PreparedStatement commandStatement, SimpleDateFormat inputDateFormat) throws Exception
	   {
		String type = fieldType.toUpperCase().substring(0,fieldType.indexOf("(")>-1?fieldType.indexOf("("):fieldType.length());
	     switch (type)
	     {
		  	case "TINYBLOB":
         	case "MEDIUMBLOB":
         	case "LONGBLOB":
         	case "BLOB":
         	case "BYTEA":	
         	case "BINARY":
         	case "VARBINARY":
         	case "IMAGE":
         	case "LONG RAW":
         	case "RAW":
			  byte[] byteValue = (byte[])fieldValue;
			  if (byteValue == null)
			  {
				  commandStatement.setNull(fieldPosition, java.sql.Types.BINARY);
			  }
			  else
			  {
				  commandStatement.setBytes(fieldPosition, byteValue);
			  }
			  break;
         	case "BOOL":
           	case "BIT":
           	case "BOOLEAN":
			  boolean boolValue = (boolean)fieldValue;
			  	  commandStatement.setBoolean(fieldPosition, boolValue);
			  break;
           	case "CHAR":
  		    case "GRAPHIC":
  		    case "CHARACTER":
  		    	String charValue = (String)fieldValue;
  		       if ((charValue == null || charValue.isEmpty()) || (charValue.equals("null")) || (charValue.equals("NULL")))
  		       {
  		         commandStatement.setNull(fieldPosition, java.sql.Types.CHAR);
  		       }
  		       else
  		       {
  		         commandStatement.setString(fieldPosition, charValue);
  		       }
  		       break;
  		  case "NCHAR VARYING":
  		     String ncharValue = (String)fieldValue;
  		     if ((ncharValue == null || ncharValue.isEmpty()) || (ncharValue.equals("null")) || (ncharValue.equals("NULL")))
	  	       {
	  	         commandStatement.setNull(fieldPosition, java.sql.Types.NCHAR);
	  	       }
	  	       else
	  	       {
	  	         commandStatement.setString(fieldPosition, ncharValue);
	  	       }
	  	       break;
  		    case "NVARCHAR2":
	     	case "NVARCHAR":
	     		String nvarcharValue = (String)fieldValue;
	  		    if ((nvarcharValue == null || nvarcharValue.isEmpty()) || (nvarcharValue.equals("null")) || (nvarcharValue.equals("NULL")))
		  	       {
		  	         commandStatement.setNull(fieldPosition, java.sql.Types.NVARCHAR);
		  	       }
		  	       else
		  	       {
		  	         commandStatement.setString(fieldPosition, nvarcharValue);
		  	       }
		  	       break;
	     	case "VARCHAR2":	     
  	     	case "VARCHAR":
  		    case "SYSNAME":
  		    case "VARGRAPHIC":
  		    case "VARG":
  		    case "UNIQUEIDENTIFIER":
  		    		String strValue = (String)fieldValue;
  		    		if ((strValue == null || strValue.isEmpty()) || (strValue.equals("null")) || (strValue.equals("NULL")))
  		    		{
  		    			commandStatement.setNull(fieldPosition, java.sql.Types.VARCHAR);
  		    		}
  		    		else
  		    		{
  		    			commandStatement.setString(fieldPosition, strValue);
  		    		}
  		    		break;

	     	case "LONG VARCHAR":
	     	case "TINYTEXT":
	     	case "MEDIUMTEXT":
	     	case "LONGTEXT":
	     	case "JSON":
	     	case "STRING":
	     	case "ROWID":
	     	case "XMLTYPE":
	     	case "SDO_GEOMETRY": 
			case "TEXT":
			case "NTEXT":
			case "CLOB":
			case "NCLOB":
			String txtValue = (String)fieldValue;
	       if ((txtValue == null ||txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
	       }
	       else
	       {
	         commandStatement.setString(fieldPosition, txtValue);
	       }
	       break;

	     case "TINYINT":
	     case "SMALLINT":
			Short sintValue =  (Short)fieldValue;
	       if ((sintValue == null) || (sintValue.equals("null")) || (sintValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.TINYINT);
	       }
	       else
	       {
	         commandStatement.setShort(fieldPosition, sintValue);
	       }
	       break;
	     case "INT2":
	    	 case "INT4":
    		 case "INT8":
    		 case "INT":
    		 case "INTEGER": 
			Integer intValue =  (Integer)fieldValue;
	       if ((intValue == null) || (intValue.equals("null")) || (intValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.INTEGER);
	       }
	       else
	       {
	         commandStatement.setInt(fieldPosition, intValue);
	       }
	       break;

		  case "BIGINT":
			Long longValue =  (Long)fieldValue;  
	       if ((longValue == null) || (longValue.equals("null")) || (longValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.BIGINT);
	       }
	       else
	       {
	         commandStatement.setLong(fieldPosition, longValue);
	       }
	       break;
		  
		  case "REAL":
		  case "FLOAT":
			Float floatValue =  (Float)fieldValue;    
	       if ((floatValue == null) || (floatValue.equals("null")) || (floatValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.FLOAT);
	       }
	       else
	       {
	         commandStatement.setFloat(fieldPosition, floatValue);
	       }
	       break;

		  
	     case "MONEY":
	 		BigDecimal moneyValue = (BigDecimal)fieldValue;

	         if ((moneyValue == null) || (moneyValue.equals("null")) || (moneyValue.equals("NULL")))
	         {
	           commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
	         }
	         else
	         {
	           commandStatement.setBigDecimal(fieldPosition, moneyValue);
	         }
	         break;
	     case "SMALLMONEY":
	     case "DECIMAL":
			BigDecimal decimalValue = (BigDecimal)fieldValue;
	       if ((decimalValue == null) || (decimalValue.equals("null")) || (decimalValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.DECIMAL);
	       }
	       else
	       {
	         commandStatement.setBigDecimal(fieldPosition, decimalValue);
	       }
	       break;	  
	     case "NUMBER":
	     case "NUMERIC":
			BigDecimal numbericValue = (BigDecimal)fieldValue;
	       if ((numbericValue == null) || (numbericValue.equals("null")) || (numbericValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
	       }
	       else
	       {
	         commandStatement.setBigDecimal(fieldPosition, numbericValue);
	       }
	       break;
         case "DECFLOAT":
         case "BINARY_FLOAT":
		 case "BINARY_DOUBLE":
		 case "DOUBLE":
			Double doubleValue = (Double)fieldValue;  
	       if ((doubleValue == null) || (doubleValue.equals("null")) || (doubleValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.DOUBLE);
	       }
	       else
	       {
	         commandStatement.setDouble(fieldPosition, doubleValue);
	       }
	       break;
	     case "DATE":
			String dateValue = (String)fieldValue; 
	       if ((dateValue == null) || (dateValue.equals("null")) || (dateValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.DATE);
	       }
	       else
	       {
	         commandStatement.setDate(fieldPosition, new java.sql.Date(inputDateFormat.parse(dateValue).getTime()));
	       }
	       break;
	     case "TIME":
			String timeValue = (String)fieldValue; 

	       if ((timeValue == null) || (timeValue.equals("null")) || (timeValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.TIME);
	       }
	       else
	       {
	         commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(timeValue).getTime()));
	       }
	       break;
	     case "TIME WITH TIME ZONE":
	    	 	String timewithzoneValue = (String)fieldValue; 

		       if ((timewithzoneValue == null) || (timewithzoneValue.equals("null")) || (timewithzoneValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, java.sql.Types.TIME_WITH_TIMEZONE);
		       }
		       else
		       {
		         commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(timewithzoneValue).getTime()));
		       }
		       break;
	     case "DATETIME":
	     
	     
	     case "TIMESTAMP":
	     case "TIMESTMP":
			String timestampValue = (String)fieldValue; 
	       if ((timestampValue == null || timestampValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
	       {
	         commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP);
	       }
	       else
	       {
	         commandStatement.setTimestamp(fieldPosition, new Timestamp(inputDateFormat.parse(timestampValue).getTime()));
	       }
	       break;
	       
	     case "TIMESTAMP WITH TIME ZONE":
	     case "TIMESTAMP WITH LOCAL TIME ZONE":

	    	 String timestampwithzoneValue = (String)fieldValue; 
		       if ((timestampwithzoneValue == null || timestampwithzoneValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
		       }
		       else
		       {
		         commandStatement.setTimestamp(fieldPosition, new Timestamp(inputDateFormat.parse(timestampwithzoneValue).getTime()));
		       }
		       break;
	     
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
			
//			if(((Table)obj).getName().contains("$"))
//				((Table)obj).setName(((Table)obj).getName().replace("$", "_"));
			
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
			
			if(((Column)obj).getName().contains("#"))
				((Column)obj).setName(((Column)obj).getName().replace("#", "_"));
			
		}
		
	}
	
}
