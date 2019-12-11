package com.datasphere.db.migration.scheduler;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.datasphere.db.config.DBConfig;
import com.datasphere.db.dao.IBaseDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;

public abstract class AbstractMigrationScheduler extends MigrationScheduler {

	IBaseDao sourceDao;
	
	IBaseDao destDao;

	@Override
	public void doSchedule() throws Exception {
		sourceDao = getSourceDao();
		destDao = getDestDao();
		
		List<Schema> srcSchemas = sourceDao.getSchemas();
		List<Schema> destSchemas = new LinkedList<Schema>();
		for(Schema schema: srcSchemas) {
			destSchemas.add(convert(schema));
		}
		destDao.createSchemas(destSchemas);
		
		List<Table> srcTables = sourceDao.getTables();
		List<Table> destTables = new LinkedList<Table>();
		for(Table table: srcTables) {
			Table temp = convert(table);
			destDao.checkTable(table);
			destTables.add(temp);
		}
		destDao.createTables(destTables);
		
		for(int index = 0; index < srcTables.size(); index++) {
			Table srcTable = srcTables.get(index);
			Table destTable = destTables.get(index);
			Object[][] data = sourceDao.getData(srcTable);
			if(data != null) {
				convert(srcTable.getColumns(), destTable.getColumns(), data);
				destDao.putData(destTable, data);
			}
		}
	}
	
	public Schema convert(Schema schema) {
		return new Schema(convertSchemaName(schema.getName()));
	}

	public Table convert(Table table) {
		Table res = new Table(convertTableName(table.getName()), convertSchemaName(table.getSchema()));
		for(Column col: table.getColumns()) {
			res.addColumn(convert(col));
		}
		return res;
	}

	public Column convert(Column column) {
		Column res = (Column) column.clone();
		res.setName(convertColumnName(column.getName()));
		res.setTableName(convertTableName(column.getTableName()));
		return res;
	}
	
	public String convertSchemaName(String schemaName) {
		DBConfig srcConfig = this.getMigration().getSourceConfig();
		String prefix = srcConfig.getHost().replace(".", "_");
		return "HOSTT_" + prefix + "_" + srcConfig.getPort() + "_" + schemaName + "_";
	}
	
	public String convertTableName(String tableName) {
		return tableName + "_";
	}
	
	public String convertColumnName(String columnName) {
		return columnName + "_";
	}
	
	public abstract void convert(Collection<Column> srcColumns, Collection<Column> destColumns, Object[][] data);
	
	public abstract IBaseDao createSourceDao();
	
	public abstract IBaseDao createDestDao();
	
	public IBaseDao getDestDao() {
		if(destDao == null) {
			destDao = createDestDao();
		}
		return destDao;
	}
	
	public IBaseDao getSourceDao() {
		if(sourceDao == null) {
			sourceDao = createSourceDao();
		}
		return sourceDao;
	}
}
