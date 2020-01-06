package com.datasphere.db.config;

public class OracleConfig extends DBConfig{

	String databaseName;
	
	String user;
	
	String password;

	public String tzOffset;
	public int fullSyncBatchSize;
	public int fullSyncLoggingBatchSize;
	public String fullSyncTargetTableExistsAction;
	public int parallelism;
	public String dateTimeFormat;
	
	

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	public String getTzOffset() {
		return tzOffset;
	}

	public void setTzOffset(String tzOffset) {
		this.tzOffset = tzOffset;
	}

	public int getFullSyncBatchSize() {
		return fullSyncBatchSize;
	}

	public void setFullSyncBatchSize(int fullSyncBatchSize) {
		this.fullSyncBatchSize = fullSyncBatchSize;
	}

	public int getFullSyncLoggingBatchSize() {
		return fullSyncLoggingBatchSize;
	}

	public void setFullSyncLoggingBatchSize(int fullSyncLoggingBatchSize) {
		this.fullSyncLoggingBatchSize = fullSyncLoggingBatchSize;
	}
	
	public String getFullSyncTargetTableExistsAction() {
		return fullSyncTargetTableExistsAction;
	}

	public void setFullSyncTargetTableExistsAction(String fullSyncTargetTableExistsAction) {
		this.fullSyncTargetTableExistsAction = fullSyncTargetTableExistsAction;
	}
	

	public String getDateTimeFormat() {
		return dateTimeFormat;
	}

	public void setDateTimeFormat(String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}
	
	
}
