package com.datasphere.db.migration.scheduler.impl;

import static com.datasphere.db.definition.SnappyDef.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import com.datasphere.db.dao.IBaseDao;
import com.datasphere.db.dao.impl.PGDao;
import com.datasphere.db.dao.impl.SnappyDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.exception.ColumnTypeUnsurported;
import com.datasphere.db.migration.scheduler.AbstractMigrationScheduler;

public class PGSql2SnappyMigrationScheduler extends AbstractMigrationScheduler {
	
	@Override
	public Column convert(Column column) {
		Column res = super.convert(column);
		System.out.println(column.getTableName()+"&&&&&&&"+column.getName()+"$$$$"+column.getType());
		switch(column.getType()) {
			case "varchar": {
				if(column.getLength() == null || column.getLength() <= 0) {
					res.setLength(SNAPPY_VARCHAR_DEFAULT_LENGTH);
				} else if(column.getLength() > SNAPPY_VARCHAR_MAX_LENGTH) {
					res.setType("long varchar");
				}
				break;
			}
			case "bpchar": {
				res.setType("char");
				if(column.getLength() == null || column.getLength() <= 0) {
					res.setLength(SNAPPY_CHAR_MAX_LENGTH);
				} else if(column.getLength() > SNAPPY_CHAR_MAX_LENGTH) {
					res.setType("clob");
				}
				break;
			}
			case "serial2":
			case "int2": {
				res.setType("smallint");
				break;
			}
			case "serial4":
			case "int4": {
				res.setType("integer");
				break;
			}
			case "serial8":
			case "int8": {
				res.setType("bigint");
				break;
			}
			
			case "float4": {
				res.setType("float");
				break;
			}
			case "float8": {
				res.setType("double");
				break;
			}
			
			case "numeric": {
				if (column.getPrecision()==0||column.getPrecision()==null) {
					res.setPrecision(SNAPPY_NUMERIC_MAX_LENGTH);
					res.setScale(column.getScale());
				}else {
					res.setPrecision(column.getPrecision());
					res.setScale(column.getScale());
				}
				break;
			}
			
			case "bool": {
				res.setType("smallint");
				break;
			}
			
			case "bytea": {
				res.setType("blob");
				break;
			}
			case "decimal": {
				if(column.getLength() > SNAPPY_NUMERIC_MAX_LENGTH) {
					throw new ColumnTypeUnsurported("The length of decimal exceeds!column=" + column.getName() + ",table=" + column.getTableName());
				}
				if (column.getPrecision()==0||column.getPrecision()==null) {
					res.setPrecision(SNAPPY_NUMERIC_MAX_LENGTH);
					res.setScale(column.getScale());
				}else {
					res.setPrecision(column.getPrecision());
					res.setScale(column.getScale());
				}
				break;
			}
			case "text": {
				res.setType("clob");
				break;
			}
			
			case "timetz": {
				res.setType("time");
				break;
			}
			case "timestamptz": {
				res.setType("timestamp");
				break;
			}
			case "uuid": {
				res.setType("varchar");
				res.setLength(50);
				break;
			}
			case "xml":
			case "timestamp":
			case "time":
			case "date": {
				break;
			}
			default: {
				throw new ColumnTypeUnsurported("Unsupported type!column=" + column.getName() + ",table=" + column.getTableName());
			}
				
		}
		return res;
	}

	@Override
	public void convert(Collection<Column> srcColumns, Collection<Column> destColumns, Object[][] data) {
		Iterator<Column> it1 = srcColumns.iterator();
		Iterator<Column> it2 = destColumns.iterator();
		int colIndex = 0;
		while(it1.hasNext()) {
			Column srcCol = it1.next();
			Column destCol = it2.next();
			switch(srcCol.getType().toLowerCase() + "_" + destCol.getType().toLowerCase()) {
				case "bool_smallint": {
					for(Object[] rowData: data) {
						if(rowData[colIndex] != null) {
							Boolean bv = (Boolean)rowData[colIndex];
							if(bv) {
								rowData[colIndex] = Short.valueOf((short) 1);
							} else {
								rowData[colIndex] = Short.valueOf((short) 0);
							}
						}
					}
					break;
				}
				case "uuid_varchar": {
					for(Object[] rowData: data) {
						if(rowData[colIndex] != null) {
							UUID v = (UUID)rowData[colIndex];
							rowData[colIndex] = v.toString();
						}
					}
					break;
				}
			}
			colIndex++;
		}
	}

	@Override
	public IBaseDao createSourceDao() {
		PGDao pgDao = new PGDao();
		pgDao.setConfig(this.getMigration().getSourceConfig());
		return pgDao;
	}

	@Override
	public IBaseDao createDestDao() {
		SnappyDao snappyDao = new SnappyDao();
		snappyDao.setConfig(this.getMigration().getDestConfig());
		return snappyDao;
	}
}
