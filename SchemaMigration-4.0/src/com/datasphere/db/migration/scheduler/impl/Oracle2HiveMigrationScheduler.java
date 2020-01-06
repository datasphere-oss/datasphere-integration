package com.datasphere.db.migration.scheduler.impl;

import java.util.Collection;

import com.datasphere.db.dao.IBaseDao;
import com.datasphere.db.dao.impl.HiveDao;
import com.datasphere.db.dao.impl.OracleDao;
import com.datasphere.db.entity.Column;
import com.datasphere.db.exception.ColumnTypeUnsurported;
import com.datasphere.db.migration.scheduler.AbstractMigrationScheduler;

public class Oracle2HiveMigrationScheduler extends AbstractMigrationScheduler {
	
	public static long DEFAULT_VARCHAR_LENGTH = 500;
	public static int HIVE_CHAR_MAX_LENGTH = 254;
	//public static int HIVE_VARCHAR_MAX_LENGTH = 32672;
	public static int HIVE_NUMERIC_MAX_PRECISION = 127;
	public final static String TYPE_TIMESTAMP = "TIMESTAMP";
	public final static String TYPE_TIME_ZONE = "WITH TIME ZONE";
	public final static String TYPE_LOCAL_TIME_ZONE = "WITH LOCAL TIME ZONE";
	
	@Override
	public Column convert(Column column) {
		Column newColumn = super.convert(column);
		String columnType = column.getType();
		columnType = column.getType().startsWith(TYPE_TIMESTAMP) ? TYPE_TIMESTAMP : columnType;
		String type = columnType.toLowerCase().substring(0,columnType.indexOf("(")>-1?columnType.indexOf("("):columnType.length());
		switch (type) {
			//处理字符类型
			case "char":
				//目前hive最大支持存放254个长度的字符,大于其最大长度的字段使用varchar
				if(newColumn.getLength() == null || newColumn.getLength() <= 0) {
					newColumn.setLength(DEFAULT_VARCHAR_LENGTH);
					newColumn.setType("varchar");
				}else{
					if( newColumn.getLength() <= HIVE_CHAR_MAX_LENGTH){
						newColumn.setType("char");
					}else{
						newColumn.setType("varchar");
					}
				}
				break;
			case "nchar":
			case "nvarchar2":
			case "varchar2":
				if(newColumn.getLength() == null || newColumn.getLength() <= 0) {
					newColumn.setLength(DEFAULT_VARCHAR_LENGTH);
				} 
				newColumn.setType("varchar");
				break;
			//处理数值类型
			case "number":
				//precision为null时，设置目标数据库的默认精度
				if(newColumn.getPrecision() == null || newColumn.getPrecision() <= 0){
					newColumn.setPrecision(HIVE_NUMERIC_MAX_PRECISION);
				}
				//hive中 numeric scale不能小于0
				if(newColumn.getScale() == null || newColumn.getScale() < 0){
					newColumn.setScale(0);
				}
				//hive中 numeric scale不能大于 precision
				if(newColumn.getScale() > newColumn.getPrecision()){
					newColumn.setScale(0);
				}
				newColumn.setType("numeric");
				break;
				
			case "float":
				//oracle总float类型，是按二进制来算的精度,其中精度范围为二进制的1到126,在转化为我们常说的进十进制是，需要乘以0.30103
				int percision = newColumn.getPrecision();
				newColumn.setPrecision(HIVE_NUMERIC_MAX_PRECISION);
				Integer scale = (int) Math.round(percision*0.30103);
				newColumn.setScale(scale);
				newColumn.setType("numeric");
				break;
				
			case "binary_float":
				newColumn.setType("float");
				break;
			case "binary_double":
				newColumn.setType("double");
				break;
			case "date":
			case "timestamp":
				newColumn.setType("timestamp");
				break;
			case "long":
			case "long varchar":
			case "clob" : 
			case "nclob":
				newColumn.setType("clob");
				break;
			case "blob" :
			case "long raw":
			case "raw":
				newColumn.setType("blob");
				break;
/*			case "raw" :
				newColumn.setType("varchar for big data");
				break;*/
	
			default: {
				//不支持的类型 INTERVAL DAY,INTERVAL YEAR ROWID UROWID BFILE 
				throw new ColumnTypeUnsurported("Unsurport Type ["+columnType.toUpperCase()+"]");
			}	
		}

		return newColumn;
	}

	@Override
	public IBaseDao createSourceDao() {
		OracleDao oracleDao = new OracleDao();
		oracleDao.setConfig(this.getMigration().getSourceConfig());
		return oracleDao;
	}

	@Override
	public IBaseDao createDestDao() {
		HiveDao hiveDao = new HiveDao();
		hiveDao.setConfig(this.getMigration().getDestConfig());
		return hiveDao;
	}

	@Override
	public void convert(Collection<Column> srcColumns, Collection<Column> destColumns, Object[][] data) {}
}