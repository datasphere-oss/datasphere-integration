package com.datasphere.db.dao.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.postgresql.ds.PGConnectionPoolDataSource;

import com.datasphere.common.utils.JDBCUtils;
import com.datasphere.db.config.PGConfig;
import com.datasphere.db.dao.AbstractBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;

public class PGDao extends AbstractBaseDao {
	
	PGConnectionPoolDataSource pgConnectionPoolDataSource;

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
		try(Connection conn = getConnection()) {
			
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
						column.setLength(tempSet.getInt(4));
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
		try(Connection conn = getConnection()) {
			
		}
	}

	public Object[][] getData(Table table) throws Exception{
		try(Connection conn = getConnection();Statement stat = conn.createStatement();) {
			int row = 0;
			int col = table.getColumns().size();
			ResultSet set = stat.executeQuery("select count(*) from " + table.getSchema() + ".\"" + table.getName() + "\"");
			if(set.next()) {
				row = Long.valueOf(set.getLong(1)).intValue();
			}
			if(row == 0) {
				return null;
			}
			set = stat.executeQuery("select " + JDBCUtils.csColumnNames(table.getColumns()) +" from " + table.getSchema() + ".\"" + table.getName() + "\"");
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
}
