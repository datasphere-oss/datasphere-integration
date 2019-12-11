package com.datasphere.db.dao;

import com.datasphere.db.config.DBConfig;

public abstract class AbstractBaseDao implements IBaseDao {

	DBConfig config;
	
	@SuppressWarnings("unchecked")
	public <T extends DBConfig> T getConfig() {
		return (T) config;
	}

	public void setConfig(DBConfig config) {
		this.config = config;
	}
}
