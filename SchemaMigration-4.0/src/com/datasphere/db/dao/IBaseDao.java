package com.datasphere.db.dao;

import java.util.List;

import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;

public interface IBaseDao {

	List<Schema> getSchemas() throws Exception;

	void createSchemas(List<Schema> destSchemas) throws Exception;

	List<Table> getTables() throws Exception;

	void createTables(List<Table> destTables) throws Exception;
	
	void checkTable(Table table) throws Exception;

	Object[][] getData(Table table) throws Exception;

	void putData(Table table, Object[][] data) throws Exception;
}
