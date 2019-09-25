package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.mahout.math.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.source.lib.meta.DatabaseColumn;
import com.datasphere.BitPattern.BitPatternMap;
import com.datasphere.Table.Table;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.TypeHandler.TypeHandler;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.UnsupportedMapping;
import com.datasphere.exception.UpdateTypeMapException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class InsertHandler extends OperationHandler
{
    private Logger logger;
    
    public InsertHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)InsertHandler.class);
    }
    
    @Override
    public void validate(final HDEvent event) throws DatabaseWriterException {
        super.validate(event);
    }
    
    public long convertTimeToLong(String time) {
    	Date date = null;
    	try {
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    		date = sdf.parse(time);
    		return date.getTime();
    	} catch (Exception e) {
    		e.printStackTrace();
    		return 0L;
    	}
    }

    
    @Override
    public void bind(final String targetTable, final HDEvent event, final PreparedStatement stmt) throws SQLException, DatabaseWriterException {
        final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTable, event);
        TypeHandler[] columns = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
        int itr = 0;
        int columnIndex = 0;
        final Object[] data = event.data;
        final Integer[] dataFiled = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
        final Table tableDef = (Table)TargetTableMap.getInstance(this.callbackIntf).getTableMeta(targetTable);
        DatabaseColumn[] cols = tableDef.getColumns();
        String targetType = event.metadata.get("TargetType") != null ? event.metadata.get("TargetType").toString() : "";
        while (true) {
            try {
                for (itr = 0; itr < data.length; ++itr) {
                    columnIndex = itr;
                    if (data[columnIndex] == null) {
                        this.bindNull(columns[columnIndex], stmt, itr + 1);
                    }
                    else {
	                    columns[columnIndex].bind(stmt, itr + 1, data[columnIndex]);
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (int xitr = itr; xitr < data.length; ++xitr) {
                        columnIndex = xitr;
                        if (data[columnIndex] != null) {
                            columns = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, columnIndex, data[columnIndex].getClass());
                        }
                    }
                }
                catch (UnsupportedMapping mappingExp) {
                    final String srcTable = (String)event.metadata.get(Constant.TABLE_NAME);
                    final String errMsg = "源端类型 {" + mappingExp.sourceType + "} 转换为目标端类型 {" + mappingExp.targetType + "} 不支持。源端表：{" + srcTable + "} ，目标端表：{" + targetTable + "}, 字段索引： {" + columnIndex + "}";

                	//System.out.println(errMsg);
                    throw new DatabaseWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                this.logger.error("Unhandled exception occured while biding data ", (Throwable)uExp);
                this.logger.error("Event caused this issue = " + event.toJSON());
                this.logger.error("Columns which are referred " + Arrays.toString((Object[])dataFiled));
                try {
                    final String debugQuery = this.generateDML(event, targetTable);
                    this.logger.error("Debug SQL Statement {" + debugQuery + "}");
                }
                catch (Exception ex) {}
                throw uExp;
            }
            break;
        }
    }
    
    @Override
    public String generateDML(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        final String bitMapStr = new String(event.dataPresenceBitMap);
        final String key = targetTable + "-" + bitMapStr;
        String query = this.statementCache.get(key);
//        String sql = (String)event.metadata.get("SQL");
//        logger.info("====InsertHandler generateDML=====sql:"+sql);
//        if(sql != null) {
//        		return sql;
//        }
        String targetType = "oracle";
        if(event.metadata.get("TargetType")!=null) {
        		if("MySQL".equalsIgnoreCase(event.metadata.get("TargetType").toString())) {
        			targetType = "mysql";
        		}
        }
        	
        if (query == null) {
            String table = targetTable;
            if(!"mysql".equalsIgnoreCase(targetType)) {
	            if(table.indexOf(".")>-1) {
	            		String[] tabs = table.split("\\.");
	            		if(tabs.length == 2) {
	            			table = tabs[0] +".\""+ tabs[1]+"\"";
	            		}
	            }
            }
            final StringBuilder dml = new StringBuilder();
            dml.append("INSERT INTO ");
            dml.append(table);
            String fieldList = "";
            String bindList = "";
            final Table tableDef = (Table)TargetTableMap.getInstance(this.callbackIntf).getTableMeta(targetTable);
            DatabaseColumn[] cols = tableDef.getColumns();
            Integer[] colIndex = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
            
            //System.out.println("----字段数：" + cols.length);
            try {
                for (int itr = 0; itr < cols.length; ++itr) {
                    if(!"mysql".equalsIgnoreCase(targetType)) {
	                    if (itr == 0) {
	                        fieldList = "\"" + cols[itr].getName() + "\" ";
	                        bindList = " ? ";
	                    }
	                    else {
	                        fieldList = fieldList + ", \"" + cols[itr].getName() + "\" ";
	                        bindList += ", ? ";
	                    }
	                	}else {
	                		if (itr == 0) {
	                        fieldList = "" + cols[itr].getName() + " ";
	                        bindList = " ? ";
	                    }
	                    else {
	                        fieldList = fieldList + ", " + cols[itr].getName() + " ";
	                        bindList += ", ? ";
	                    }
	                	}
                }
            }
            catch (Exception exp) {
                throw exp;
            }
            dml.append("( ");
            dml.append(fieldList);
            dml.append(") values (");
            dml.append(bindList);
            dml.append(")");
            query = dml.toString();
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Query prepared and cahced {" + query + "}");
            }
            this.statementCache.put(key, query);
            String keys = this.statementCache.get(targetTable);
            if (keys == null) {
                keys = key;
            }
            else {
                keys = keys + ";" + key;
            }
            this.statementCache.put(targetTable, keys);
        }
        return query;
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}
