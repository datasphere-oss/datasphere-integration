package com.datasphere.db.startup;

import com.datasphere.db.config.MSSQLConfig;
import com.datasphere.db.config.MySQLConfig;
import com.datasphere.db.config.PGConfig;
import com.datasphere.db.config.HiveConfig;
import com.datasphere.db.migration.Migration;
import com.datasphere.db.migration.MigrationManager;

public class Entrance {

	public static void main(String[] args) {
		MigrationManager manager = new MigrationManager();
		Migration m = new Migration();
		m.setSourceConfig(getMySQLConfig());
//		m.setSourceConfig(getPGConfig());
		
		m.setDestConfig(getHiveConfig());
		manager.addMigration(m);
		manager.shutdown();
	}
	
	public static PGConfig getPGConfig() {
		PGConfig pgConfig = new PGConfig();
		pgConfig.setHost("localhost");
		pgConfig.setPort(5432);
		pgConfig.setDatabaseName("datasphere");
		pgConfig.setUser("postgres");
		pgConfig.setPassword("");
		return pgConfig;
	}
	
	public static MySQLConfig getMySQLConfig() {
		MySQLConfig mysqlConfig = new MySQLConfig();
		mysqlConfig.setHost("106.75.17.70");
		mysqlConfig.setPort(3306);
		mysqlConfig.setDatabaseName("test");
		mysqlConfig.setUser("root");
		mysqlConfig.setPassword("Root@123456");
		return mysqlConfig;
	}
	
	public static HiveConfig getHiveConfig() {
		HiveConfig hiveConfig = new HiveConfig();
		hiveConfig.setHost("117.50.95.184");
		hiveConfig.setPort(10000);
		hiveConfig.setDatabaseName("default");
		hiveConfig.setUser("root");
		hiveConfig.setPassword("hhdata_123");
		return hiveConfig;
	}
	
	
	public static MSSQLConfig getMSSQLConfig() {
		MSSQLConfig mssqlConfig = new MSSQLConfig();
		mssqlConfig.setHost("192.168.16.103");
		mssqlConfig.setPort(1433);
		mssqlConfig.setDatabaseName("DBSyncer");
		mssqlConfig.setUser("sa");
		mssqlConfig.setPassword("123456");
		return mssqlConfig;
	}
}
