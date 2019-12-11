package com.datasphere.db.dao.impl;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.db.config.MSSQLConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.MSSQLSchema;
import com.datasphere.db.entity.MSSQLTable;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
public class MSSQLDao extends AbstractBaseDao {
	
	SQLServerDataSource mssqlDataSource;
  
    public List<Schema> getSchemas() throws Exception{
		List<Schema> schemas = new LinkedList<Schema>();
		//获取过滤后的数据库名和schemas和表名
		List<String> dblist= selectDatabases();
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			for (String dbName: dblist) {
				// 从DB列表中取得每个dbName
				MSSQLSchema currentDbObjects = new MSSQLSchema("");
				// 设置 Catalog 数据库
				conn.setCatalog(dbName);
				// 查询 SchemaName.TableName, 并将当前表添加到同步列表中
				String tablesQuery = "SELECT SCHEMA_NAME(Schema_id), t.name FROM sys.tables  t LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id WHERE (ep.class_desc IS NULL OR (ep.class_desc <> 'OBJECT_OR_COLUMN' AND ep.name <> 'microsoft_database_tools_support')) AND  t.object_id in "
						+ " (select object_id from sys.tables where is_ms_shipped=0 and type = 'U') ";
				ResultSet tablesRset = stat.executeQuery(tablesQuery);
				while (tablesRset.next()) {
					String schemaName = tablesRset.getString(1);
					String tablesName = tablesRset.getString(2);
					
					currentDbObjects.setName(schemaName);
					currentDbObjects.setDbName(dbName);
					currentDbObjects.addTable(new MSSQLTable(tablesName, schemaName,dbName));
				}
				schemas.add(currentDbObjects);
			}
			return schemas;
		}
	}

	public void createSchemas(Set<Schema> destSchemas) throws Exception{
		try(Connection conn = getConnection()) {
			
		}
	}

	public List<Table> getTables() throws Exception{
		List<Table> tableList = new LinkedList<Table>();
		try(Connection conn = getConnection();Statement stmt = conn.createStatement();) {
			for (Schema Schema : this.getSchemas()) {
				MSSQLSchema currentSchema=(com.datasphere.db.entity.MSSQLSchema) Schema;
				int databaseId = -1;
				String lsn = "";
				String startLsn = "-1";
				String dbName = currentSchema.getDbName();
				this.getConnection().setCatalog(dbName);
				// 查询最大LSN号
				String lsnQuery = "select db_id('" + dbName
						+ "'), max([Current Lsn]) from fn_dblog(null,null) where Operation = 'LOP_COMMIT_XACT'";
				ResultSet lsnRset = stmt.executeQuery(lsnQuery);
				if (lsnRset.next()) {
					databaseId = lsnRset.getInt(1);
					lsn = lsnRset.getString(2);
					startLsn = this.convertLsnToDecString(lsn);
				}
				// 得到 Schema, table名称
				List<Table> tablesSet = currentSchema.getTables();
				for (Table tables : tablesSet) {
					String prepQuery = "SELECT distinct scols.colid, scols.NAME as columnName, types.name AS colType , "
							+ "ISNULL(scols.length, cols.max_length) AS colLen, "
							+ "ISNULL(scols.xprec, cols.precision) AS precision, "
							+ "ISNULL(scols.xscale, cols.scale) AS scale, " + " scols.isnullable " + " FROM  " + dbName
							+ ".sys.tables t " + "INNER JOIN " + dbName + ".sys.schemas s ON s.schema_id=t.schema_id "
							+ "INNER JOIN " + dbName + ".sys.partitions partitions ON t.object_id=partitions.object_id "
							+ "INNER JOIN " + dbName
							+ ".sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id "
							+ "LEFT OUTER JOIN " + dbName
							+ ".sys.syscolumns scols ON scols.id = t.object_id AND scols.colid = cols.partition_column_id "
							+ "INNER JOIN  " + dbName + ".sys.systypes types ON scols.xusertype = types.xusertype "
							+ "WHERE is_ms_shipped = 0 " + "AND cols.is_dropped=0 " + "AND t.type = 'U' "
							+ "AND schema_name(t.schema_id) = ? " + "AND t.name = ? " + "ORDER BY scols.colid";
					// 通过schemaName, tableName 来查询表中的字段信息
					PreparedStatement objectColumns = this.getConnection().prepareStatement(prepQuery);
					objectColumns.setString(1, tables.getSchema()); // 模式名称
					objectColumns.setString(2, tables.getName()); // 表名称

					ResultSet rsetObjectColumns = objectColumns.executeQuery();
					String columnName = "";
					String columnType = "";
					String columnLength = "";
					Integer precision = 0;
					Integer scale = 0;
					int columnNulls = 1;
					
					while (rsetObjectColumns.next()) {
						columnName = rsetObjectColumns.getString(2);
						columnType = rsetObjectColumns.getString(3).trim();
						columnLength = rsetObjectColumns.getString(4);
						precision = rsetObjectColumns.getInt(5);
						scale = rsetObjectColumns.getInt(6);
						columnNulls = rsetObjectColumns.getInt(7);
						Column column=new Column();
						column.setName(columnName);
						column.setTableName(tables.getName());
						column.setType(columnType);
						column.setLength(Integer.valueOf(columnLength));
						column.setPrecision(precision);
						column.setScale(scale);
						column.setNotnull(Boolean.valueOf(""+columnNulls));
						tables.getColumns().add(column);
					}
					MSSQLTable mssqlTable=new MSSQLTable(tables.getName(), tables.getSchema(),dbName);
					mssqlTable.setColumns(tables.getColumns());
					tableList.add(mssqlTable);
			   }
			}
			return tableList;
		}
			
	}

	public void createTables(Set<Table> destTables) throws Exception{
		try(Connection conn = getConnection()) {
			
		}
	}

	public Object[][] getData(Table table) throws Exception{
		MSSQLTable mssqlTable = (MSSQLTable)table;
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			int row = 0;
			int col = table.getColumns().size();
			String  queryStr = "select count(*) from " + "\""+mssqlTable.getDbName()+"\".\""+ mssqlTable.getSchema() + "\".\"" + mssqlTable.getName() + "\"";
			ResultSet set = stat.executeQuery(queryStr);
			if(set.next()) {
				row = Long.valueOf(set.getLong(1)).intValue();
			}
			if(row == 0) {
				return null;
			}
			String  queryDataStr ="select " + JDBCUtils.csColumnNames(mssqlTable.getColumns()) +" from "+ "\""+ mssqlTable.getDbName()+"\".\"" + mssqlTable.getSchema() + "\".\"" + mssqlTable.getName() + "\"";
			set = stat.executeQuery(queryDataStr);
			Object[][] data = new Object[row][col];
			int rowIndex = 0;
			while(set.next()) {
				if(rowIndex >= row) {
					break;
				}
				for(int colIndex = 0; colIndex < col; colIndex++) {
					data[rowIndex][colIndex] = set.getObject(colIndex+1);
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
	
	public Connection getConnection() {
		try {
			return getDataSource().getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	SQLServerDataSource getDataSource() {
		if(mssqlDataSource == null) {
			MSSQLConfig mssqlConfig = getConfig();
			mssqlDataSource = new SQLServerDataSource();
			mssqlDataSource.setServerName(mssqlConfig.getHost());
			mssqlDataSource.setPortNumber(mssqlConfig.getPort());
			mssqlDataSource.setDatabaseName(mssqlConfig.getDatabaseName());
			mssqlDataSource.setUser(mssqlConfig.getUser());
			mssqlDataSource.setPassword(mssqlConfig.getPassword());
		}
		return mssqlDataSource;
	}

	@Override
	public void createSchemas(List<Schema> destSchemas) throws Exception {
		try(Connection conn = getConnection()) {
				
			}
	}

	@Override
	public void createTables(List<Table> destTables) throws Exception {
		try(Connection conn = getConnection()) {
				
			}
	}

	@Override
	public void checkTable(Table table) throws Exception {
		// TODO Auto-generated method stub
		
	}
	public String convertLsnToDecString(String hexLsn) {
		if (hexLsn == null)
			return "-1";
		long l1 = Long.parseLong(hexLsn.substring(0, 8), 16);
		long l2 = Long.parseLong(hexLsn.substring(9, 17), 16);
		long l3 = Long.parseLong(hexLsn.substring(18, hexLsn.length()), 16);
		String decLsn = String.valueOf(l1) + String.format("%010d", new Object[] { Long.valueOf(l2) })
				+ String.format("%05d", new Object[] { Long.valueOf(l3) });

		return decLsn;
	}
	
    public List<String> selectDatabases() throws Exception{
    	List<String> dbbaseList= new ArrayList<>();
    	 try(Connection conn = getConnection();Statement stmt = conn.createStatement();) {
		     String dbName = "";
		     int dbId = -1;
		     String query = "SELECT DB_ID(name) as dbId, name FROM sys.databases where name not in ('master','tempdb','model','msdb')";
		     this.getConnection().setCatalog("master");
		     ResultSet rset = stmt.executeQuery(query);
		     while (rset.next())
		     {
		       dbId = rset.getInt(1);
		       dbName = rset.getString(2);
		       dbbaseList.add(dbName);
		     }
    	 }
	     return  dbbaseList;
	   }

}
