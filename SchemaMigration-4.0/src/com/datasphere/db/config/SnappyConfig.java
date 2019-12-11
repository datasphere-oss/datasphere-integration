package com.datasphere.db.config;

public class SnappyConfig extends DBConfig {

	String user;
	
	String password;

	public String getUrl() {
		return "jdbc:snappydata:thrift://" + getHost() + "[" + getPort() + "]";
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
}
