package com.datasphere.db.migration.scheduler.impl;

import java.util.Collection;
import java.util.Iterator;

import com.datasphere.db.dao.IBaseDao;
import com.datasphere.db.dao.impl.MSSQLDao;
import com.datasphere.db.dao.impl.MySQLDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.entity.MSSQLSchema;
import com.datasphere.db.entity.MSSQLTable;
import com.datasphere.db.entity.Schema;
import com.datasphere.db.entity.Table;
import com.datasphere.db.exception.ColumnTypeUnsurported;
import com.datasphere.db.migration.scheduler.AbstractMigrationScheduler;

import microsoft.sql.DateTimeOffset;

public class MSSQLHiveMigrationScheduler extends AbstractMigrationScheduler {
	long SNAPPY_NUMERIC_MAX_LENGTH = 20;
	long SNAPPY_VARCHAR_DEFAULT_LENGTH = 100;
	int SNAPPY_VARCHAR_MAX_LENGTH = 4000;
	long SNAPPY_CHAR_MAX_LENGTH = 254;
	
	@Override
	public Schema convert(Schema schema) {
		MSSQLSchema mssqlSchema=(MSSQLSchema)schema;
		return new Schema(super.convertSchemaName(mssqlSchema.getDbName() + "_" + mssqlSchema.getName()));
	}

	@Override
	public Table convert(Table table) {
		MSSQLTable mssqlTable=(MSSQLTable)table;
		Table res = new Table(convertTableName(mssqlTable.getName()), convertSchemaName(mssqlTable.getDbName() + "_" + mssqlTable.getSchema()));
		for(Column col: table.getColumns()) {
			res.addColumn(convert(col));
		}
		return res;
	}

	@Override
	public Column convert(Column column) {
		Column res = super.convert(column);
		switch(column.getType().toLowerCase()) {
		    case "bit":
		    	res.setType("integer");
				break;
			case "binary":
			case "varbinary":
			case "image":
			case "timestamp":
				res.setType("blob");
				break;
			case "char":
				if(column.getLength() == null || column.getLength() <= 0) {
					res.setLength(SNAPPY_CHAR_MAX_LENGTH);
				} else if(column.getLength() > SNAPPY_CHAR_MAX_LENGTH) {
					res.setType("clob");
				}
				break;
			case "varchar":
				if(column.getLength() == null || column.getLength() <= 0) {
					res.setLength(SNAPPY_VARCHAR_DEFAULT_LENGTH);
				} else if(column.getLength() > SNAPPY_VARCHAR_MAX_LENGTH) {
					res.setType("long varchar");
				}
				break;
				
			case "nchar":
			case "nvarchar":
			case "sysname":
				res.setType("long varchar");
				break;
			case "text":
			case "ntext":
			case "uniqueidentifier":
			case "xml":
				res.setType("clob");
				break;
			case "tinyint":
			case "smallint":
				res.setType( "smallint");
				break;
			case "int":
			case "integer":
				res.setType( "integer");
				break;
			case "bigint":
				res.setType("bigint");
				break;
			case "double":
				res.setType( "double");
				break;
			case "real":
				res.setType("real");
				break;
			case "float":
				res.setType( "double");
				break;
			case "smallmoney":
			case "money":
			case "decimal":
				if(column.getLength() > SNAPPY_NUMERIC_MAX_LENGTH) {
					throw new ColumnTypeUnsurported("The length of decimal exceeds!column=" + column.getName() + ",table=" + column.getTableName());
				}
                if((column.getPrecision()==null || column.getPrecision() == 0)){
                	res.setPrecision(SNAPPY_VARCHAR_MAX_LENGTH);
                	if(column.getScale()==null || column.getScale()==0){
                		res.setScale(4);
                	}
				}else{
					res.setPrecision(column.getPrecision());
					res.setScale(column.getScale());
				}
				res.setType( "decimal");
				break;
			case "numeric":
				if(column.getLength() > SNAPPY_NUMERIC_MAX_LENGTH) {
					throw new ColumnTypeUnsurported("The length of decimal exceeds!column=" + column.getName() + ",table=" + column.getTableName());
				}
				if(column.getPrecision()==null || column.getPrecision() == 0){
                	res.setPrecision(SNAPPY_VARCHAR_MAX_LENGTH);
                	if(column.getScale()==null || column.getScale()==0){
					     res.setScale(SNAPPY_VARCHAR_MAX_LENGTH-1);
                	}
				}else{
					res.setPrecision(column.getPrecision());
					res.setScale(column.getScale());
				}
				res.setType( "numeric");
				break;
			case "date":
				res.setType("date");
				break;
			case "time":
				res.setType("time");
				break;
			case "datetime":
			case "datetime2":
			case "smalldatetime":
			case "datetimeoffset":
			case "time with time zone":
			case "timestamp with time zone":
				res.setType( "timestamp");
				break;
			default: {
				throw new ColumnTypeUnsurported("Unsupported type!");
			}
		}
		return res;
	}
	
	@Override
	public void convert(Collection<Column> srcColumns, Collection<Column> destColumns, Object[][] data) {
		Iterator<Column> it1 = srcColumns.iterator();
		int colIndex = 0;
		while(it1.hasNext()) {
			Column srcCol = it1.next();
			switch(srcCol.getType().toLowerCase()) {
				case "bit": {
					for(Object[] rowData: data) {
						if(rowData[colIndex] != null) {
							if("true".equalsIgnoreCase(rowData[colIndex].toString())) {
								rowData[colIndex] = 1;
							} else {
								rowData[colIndex] = 0;
							}
						}
					}
					break;
				}
				case "datetimeoffset":{
						for(Object[] rowData: data) {
							DateTimeOffset dateTimeOffset=(DateTimeOffset) rowData[colIndex];
							rowData[colIndex]=dateTimeOffset.getTimestamp();
						}
						break;
					}
			}
			colIndex++;
		}
	}
	@Override
	public IBaseDao createSourceDao() {
		MSSQLDao mssqlDao = new MSSQLDao();
		mssqlDao.setConfig(this.getMigration().getSourceConfig());
		return mssqlDao;
	}

	@Override
	public IBaseDao createDestDao() {
		MySQLDao mySQLDao = new MySQLDao();
		mySQLDao.setConfig(this.getMigration().getDestConfig());
		return mySQLDao;
	}
	
}
