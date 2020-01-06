package com.datasphere.db.dao.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.common.utils.O;
import com.datasphere.db.config.OracleConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.datasphere.db.migration.scheduler.impl.Oracle2HiveMigrationScheduler;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;

public class OracleDao extends AbstractBaseDao {
	
	 
	private final static String SQL_SELECT_SCHEMA_AND_TABLE = "SELECT * FROM DBA_TABLES WHERE OWNER NOT IN ('MDSYS','OUTLN','FLOWS_FILES','SYSTEM','EXFSYS','APEX_030200','DBSNMP','ORDSYS','APPQOSSYS','XDB','ORDDATA','SYS','WMSYS','CTXSYS','SYSMAN','ANONYMOUS','HR','OE','PM','SCOTT','SH')";
	private final static String SQL_SELECT_TABLE_ATTR = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE ,DATA_PRECISION ,DATA_SCALE FROM DBA_TAB_COLUMNS WHERE  OWNER = ? AND  TABLE_NAME = ? ORDER BY COLUMN_ID";
	private final static String SQL_SELECT_CONSTRAINTS = "select a.column_name , a.constraint_name from user_cons_columns a, user_constraints b  where a.constraint_name = b.constraint_name and b.constraint_type = 'P' and a.table_name = ? ";
	private final static String SQL_SELECT_COMMENTS = "SELECT COLUMN_NAME,COMMENTS FROM dba_col_comments WHERE TABLE_NAME = ? AND OWNER = ?";

	public final static String SQL_SELECT_ROWID_CREATE = "select DBMS_ROWID.ROWID_CREATE (1,?,?,?,?) myrowid from dual";
	public final static String SQL_SELECT_TOTAL_BLOCKS = "SELECT sum(blocks) as total_blocks FROM dba_extents WHERE OWNER=upper(?) and SEGMENT_NAME = upper(?)";
	public final static String SQL_SELECT_OBJECT_ID = "SELECT data_object_id FROM dba_objects WHERE owner=upper(?) and object_name=upper(?)";
	public final static String SQL_SELECT_DBA_EXTENDS = "select file_id,block_id from dba_extents  where owner=upper(?) AND SEGMENT_NAME=upper(?) ORDER BY FILE_ID,BLOCK_ID";
	
	public String startScn;
	
	OracleConnectionPoolDataSource oracleConnectionPoolDataSource;
	
	ArrayList<ArrayList> tablesToSync = new ArrayList();
	public List<Table> tables = new LinkedList<Table>();
	
	OracleConnectionPoolDataSource getDataSource() throws SQLException {
		
		if(oracleConnectionPoolDataSource == null) {
			OracleConfig oracleConfig = getConfig();
			oracleConnectionPoolDataSource = new OracleConnectionPoolDataSource();
			oracleConnectionPoolDataSource.setDriverType("thin");
			oracleConnectionPoolDataSource.setServerName(oracleConfig.getHost());
			oracleConnectionPoolDataSource.setPortNumber(oracleConfig.getPort());
			oracleConnectionPoolDataSource.setDatabaseName(oracleConfig.getDatabaseName());
			oracleConnectionPoolDataSource.setUser(oracleConfig.getUser());
			oracleConnectionPoolDataSource.setPassword(oracleConfig.getPassword());
		}
		return oracleConnectionPoolDataSource;
	}
	
	/**
	 * 获取表的主键约束
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public Map<String,String> getConstraints(String tableName)throws Exception{
		Map<String,String> constraints = new HashMap<>();
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			PreparedStatement pst = conn.prepareStatement(SQL_SELECT_CONSTRAINTS);
			pst.setString(1, tableName);
			ResultSet set = pst.executeQuery();
			while(set.next()){
				constraints.put(set.getString(1),set.getString(2));
			}
			return constraints;
		}
	}
	
	/**
	 * 获取表字段的注释
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public Map<String,String> getComments(String schemaName,String tableName)throws Exception{
		Map<String,String> commnets = new HashMap<>();
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			PreparedStatement pst = conn.prepareStatement(SQL_SELECT_COMMENTS);
			pst.setString(1, schemaName);
			pst.setString(2, tableName);
			ResultSet set = pst.executeQuery();
			while(set.next()){
				commnets.put(set.getString(1),set.getString(2));
			}
			return commnets;
		}
	}
	
	/**
	 * 查询库中所有非系统表
	 * 
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Set<String>> getSchemaTables() throws Exception{
		HashMap<String,Set<String>> schemaTables = new HashMap<>();
		try(Connection conn = getConnection();PreparedStatement pst = conn.prepareStatement(SQL_SELECT_SCHEMA_AND_TABLE);) {
			ResultSet rset = pst.executeQuery();
			while (rset.next()) {
				String schemaName = rset.getString(1);
				String tableName = rset.getString(2);
				if(!schemaTables.containsKey(schemaName)){
					Set<String> tns = new HashSet<String>();
					tns.add(tableName);
					schemaTables.put(schemaName, tns);
				}else{
					Set<String> tns = schemaTables.get(schemaName);
					tns.add(tableName);
				}
			}
			
			return schemaTables;
		}

	}
	
	
	@Override
	public List<Schema> getSchemas() throws Exception{
		List<Schema> schemas = new LinkedList<Schema>();
		for(String schema : getSchemaTables().keySet()){
			schemas.add(new Schema(schema));
		}
		return schemas;
	}

	@Override
	public void createSchemas(List<Schema> destSchemas) throws Exception{
		try(Connection conn = getConnection()) {
			
		}
	}

	@Override
	public List<Table> getTables() throws Exception{
		Connection conn = getConnection();
		PreparedStatement pst = conn.prepareStatement(SQL_SELECT_TABLE_ATTR);
		try{
			
			HashMap<String,Set<String>> schemaTables = getSchemaTables();
			for(String schemaName : schemaTables.keySet()){
				for(String tableName : schemaTables.get(schemaName)){
					Table table = new Table(tableName,schemaName);
					Map<String,String> constraints = getConstraints(tableName);
					Map<String,String> comments = getComments(tableName,schemaName);
					pst.setString(1, schemaName);
					pst.setString(2, tableName);
					ResultSet set = pst.executeQuery();
					while(set.next()) {
						Column column = new Column();
						String columnName = set.getString(1);
						Boolean isNotNull = "Y".equals(set.getString(4)) ? true : false;
						String columnType = set.getString(2);
						Long columnLength = set.getLong(3);
						Integer precision = set.getInt(5);
						Integer scale = set.getInt(6);
						column.setName(columnName);
						column.setTableName(tableName);
						column.setType(columnType);
						column.setLength(columnLength);
						column.setConstraint(constraints.get(columnName));
						column.setNotnull(isNotNull);
						column.setComment(comments.get(columnName));
						column.setPrecision(precision);
						column.setScale(scale);
						O.log(column);
						table.addColumn(column);
					}
					tables.add(table);
				}
			}
			return tables;
		}finally {
			pst.close();
			conn.close();
		}
	}

	@Override
	public void createTables(List<Table> destTables) throws Exception{}

	
	
	@Override
	public Object[][] getData(Table table) throws Exception{
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
	/*		for(Column newCol : table.getColumns()){
				if(newCol.getType().toUpperCase().startsWith("TIMESTAMP")){
					newCol.setName("DECODE("+newCol.getName()+",null,null,TO_CHAR("+newCol.getName()+",'YYYY-MM-DD HH24:MI:SS.FF6'))");
				}
			}*/
			
			List<Column> columns = table.getColumns();
			
			int row = 0;
			int col = columns.size();
			ResultSet set = stat.executeQuery("select count(1) from " + table.getSchema() + "." + table.getName() + "");
			if(set.next()) {
				row = Long.valueOf(set.getLong(1)).intValue();
			}
			if(row == 0) {
				return null;
			}
			
			set = stat.executeQuery("select " + JDBCUtils.csColumnNames(columns) +" from " + table.getSchema() + "." + table.getName());
			Object[][] data = new Object[row][col];
			int rowIndex = 0;
			while(set.next()) {
				if(rowIndex >= row) {
					break;
				}
				for(int colIndex = 0; colIndex < col; colIndex++) {
					Column column = columns.get(colIndex);
					data[rowIndex][colIndex] = resultsetGetBytype(column.getType(),set,colIndex + 1);
				}
				rowIndex++;
			}
			return data;
		}
	}
	// 通过类型获得结果集
	private Object resultsetGetBytype(String fieldType,ResultSet rs,int index) throws SQLException{
		if(fieldType.startsWith(Oracle2HiveMigrationScheduler.TYPE_TIMESTAMP)){
			return rs.getTimestamp(index);
		}
		Object data = null;
		String type = fieldType.toUpperCase().substring(0,fieldType.indexOf("(")>-1?fieldType.indexOf("("):fieldType.length());
		switch(type){
			case "BLOB" :
				data = rs.getBytes(index);
				break;
			case "LONG":
			case "CLOB" :
			case "NCLOB" :
				data = rs.getString(index);
				break;
			default :{
				data = rs.getObject(index);
			}
		}
		return data;
	}
	
	public Connection getConnection() {
		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	

	@Override
	public void checkTable(Table table) throws Exception {}

	@Override
	public void putData(Table table, Object[][] data) throws Exception {}
	
	
	
	
	
	
	   public String getOracleType(String sourceType, String sourceLength)
	   {
	     String oraType = "";
	     switch (sourceType.toUpperCase())
	     {
	     case "VARCHAR2":
	     case "VARCHAR":
	     case "SYSNAME":
	     case "VARGRAPHIC":
	     case "VARG":
	     case "UNIQUEIDENTIFIER":
	       oraType = "VARCHAR2(" + sourceLength + ")";
	       break;
	     case "NVARCHAR2":
	     case "NCHAR VARYING":
	     case "NVARCHAR":
	       oraType = "NVARCHAR2(" + sourceLength + ")";
	       break;
	     case "NCHAR":
	       oraType = "NCHAR(" + sourceLength + ")";
	       break;
	     case "CHAR":
	     case "GRAPHIC":
	     case "CHARACTER":
	       oraType = "CHAR(" + sourceLength + ")";
	       break;
	     case "CLOB":
	     case "TEXT":
	       oraType = "CLOB";
	       break;
	     case "NTEXT":
	       oraType = "NCLOB";
	       break;
	     case "MONEY":
	     case "SMALLMONEY":
	     case "DECFLOAT":
	     case "TINYINT":
	     case "SMALLINT":
	     case "INT":
	     case "BIGINT":
	     case "FLOAT":
	     case "REAL":
	     case "DECIMAL":
	     case "NUMERIC":
	     case "NUMBER":
	     case "INTEGER":
	     case "DOUBLE":
	       oraType = "NUMBER";
	       break;
	     case "DATE":
	     case "DATETIME":
	     case "TIME":
	       oraType = "DATE";
	       break;
	     case "TIME WITH TIME ZONE":
	       oraType = "TIME WITH TIME ZONE";
	       break;
	     case "TIMESTAMP WITH TIME ZONE":
	       oraType = "TIMESTAMP WITH TIME ZONE";
	       break;
	     case "TIMESTAMP WITH LOCAL TIME ZONE":
	       oraType = "TIMESTAMP WITH LOCAL TIME ZONE";
	       break;
	     case "TIMESTAMP":
	     case "TIMESTMP":
	       oraType = "TIMESTAMP";
	     }
	 
	     return oraType;
	   }
	 
	   public void addFieldValue(String fieldType, int fieldPosition, String fieldValue, PreparedStatement commandStatement, SimpleDateFormat inputDateFormat) throws Exception
	   {
	     switch (fieldType.toUpperCase())
	     {
	     	case "NVARCHAR2":
	     	case "NCHAR VARYING":
	     	case "NVARCHAR":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, -9);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "VARCHAR2":
	     	case "VARCHAR":
	     	case "SYSNAME":
	     	case "VARGRAPHIC":
	     	case "VARG":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 12);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "CLOB":
	     	case "TEXT":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 2005);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "NTEXT":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 2011);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "NCHAR":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, -15);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "UNIQUEIDENTIFIER":
	     	case "CHAR":
	     	case "GRAPHIC":
	     	case "CHARACTER":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 1);
		       }
		       else
		       {
		         commandStatement.setString(fieldPosition, fieldValue);
		       }
		       break;
	     	case "NUMERIC":
	     	case "NUMBER":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 2);
		       }
		       else
		       {
		         commandStatement.setBigDecimal(fieldPosition, new BigDecimal(fieldValue));
		       }
		       break;
		     case "MONEY":
		     case "SMALLMONEY":
		     case "DECFLOAT":
		     case "DECIMAL":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 3);
		       }
		       else
		       {
		         commandStatement.setBigDecimal(fieldPosition, new BigDecimal(fieldValue));
		       }
		       break;
		     case "DATE":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 91);
		       }
		       else
		       {
		         commandStatement.setDate(fieldPosition, new java.sql.Date(inputDateFormat.parse(fieldValue).getTime()));
		       }
		       break;
		     case "TIME":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 92);
		       }
		       else
		       {
		         commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(fieldValue).getTime()));
		       }
		       break;
		     case "DATETIME":
		     case "TIME WITH TIME ZONE":
		     case "TIMESTAMP WITH TIME ZONE":
		     case "TIMESTAMP WITH LOCAL TIME ZONE":
		     case "TIMESTAMP":
		     case "TIMESTMP":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 93);
		       }
		       else
		       {
		         commandStatement.setTimestamp(fieldPosition, new Timestamp(inputDateFormat.parse(fieldValue).getTime()));
		       }
		       break;
		     case "FLOAT":
		     case "DOUBLE":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 8);
		       }
		       else
		       {
		         commandStatement.setDouble(fieldPosition, Double.parseDouble(fieldValue));
		       }
		       break;
		     case "BIGINT":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, -5);
		       }
		       else
		       {
		         commandStatement.setLong(fieldPosition, Long.parseLong(fieldValue));
		       }
		       break;
		     case "TINYINT":
		     case "SMALLINT":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 5);
		       }
		       else
		       {
		         commandStatement.setShort(fieldPosition, Short.parseShort(fieldValue));
		       }
		       break;
		     case "INT":
		     case "INTEGER":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 4);
		       }
		       else
		       {
		         commandStatement.setInt(fieldPosition, Integer.parseInt(fieldValue));
		       }
		       break;
		     case "REAL":
		       if ((fieldValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
		       {
		         commandStatement.setNull(fieldPosition, 7);
		       }
		       else
		       {
		         commandStatement.setFloat(fieldPosition, Float.parseFloat(fieldValue));
		       }
		       break;
	     }
	   }
	   
	   
	   public String getStartScn() {
			return startScn;
		}

		public void setStartScn(String startScn) {
			this.startScn = startScn;
		}
	
}
