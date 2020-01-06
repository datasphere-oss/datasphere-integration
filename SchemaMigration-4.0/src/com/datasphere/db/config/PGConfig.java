package com.datasphere.db.config;

public class PGConfig extends DBConfig {

	String databaseName;
	
	String user;
	
	String password;
	
	

	boolean isDistribution;

	public String getDatabaseName() {
		return databaseName;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public boolean isDistribution() {
		return isDistribution;
	}

	public void setDistribution(boolean isDistribution) {
		this.isDistribution = isDistribution;
	}
}
