package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.mahout.math.Arrays;
import org.joda.time.DateTime;
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
import com.datasphere.exception.Error;
import com.datasphere.exception.UnsupportedMapping;
import com.datasphere.exception.UpdateTypeMapException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class UpdateHandler extends OperationHandler
{
    private Logger logger;
    
    public UpdateHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)UpdateHandler.class);
    }
    
    @Override
    public void validate(final HDEvent event) throws DatabaseWriterException {
        super.validate(event);
        if (event.before == null) {
            final String source = event.getIDString();
            final String table = (String)event.metadata.get("TableName");
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_BEFORE_IMG, ("Did not get Before Image from {" + source != null) ? source : (("} Table {" + table != null) ? table : "}"));
            this.logger.error(exception.getMessage());
            throw exception;
        }
        if (event.beforePresenceBitMap == null) {
            final DatabaseWriterException exception2 = new DatabaseWriterException(Error.MISSING_BEFOREPRESENCEBITMAP, "");
            this.logger.error(exception2.getMessage());
            throw exception2;
        }
    }
    
    @Override
    public void bind(final String targetTable, final HDEvent event, final PreparedStatement stmt) throws SQLException, DatabaseWriterException {
        final String key = TableToTypeHandlerMap.formKey(targetTable, event);
        final TypeHandler[] columns = this.tableToTypeHandlerMap.getColumnMapping(key);
        int bindIndex = this.bindAfter(targetTable, columns, event, stmt);
        bindIndex = this.bindBefore(targetTable, columns, event, stmt, bindIndex);
    }
    
    protected int bindAfter(final String targetTable, TypeHandler[] columns, final HDEvent event, final PreparedStatement stmt) throws SQLException, DatabaseWriterException {
        int bindIndex = 0;
        int itr = 0;
        int columnIndex = 0;
        Table tableDef = null;
        DatabaseColumn[] dbColumns = null;
        final String key = TableToTypeHandlerMap.formKey(targetTable, event);
        tableDef = (Table)TargetTableMap.getInstance(this.callbackIntf).getTableMeta(targetTable);
        dbColumns = tableDef.getColumns();
        final Integer[] dataFields = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
        final Object[] data = event.data;
        while (true) {
            try {
                bindIndex = 0;
                for (itr = 0; itr < dataFields.length; ++itr) {
                    columnIndex = dataFields[itr];
                    if (!dbColumns[columnIndex].isKey()) {
                        if (data[columnIndex] == null) {
                            this.bindNull(columns[columnIndex], stmt, ++bindIndex);
                        }
                        else {
                            columns[columnIndex].bind(stmt, ++bindIndex, data[columnIndex]);
                        }
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (int xitr = itr; xitr < dataFields.length; ++xitr) {
                        columnIndex = dataFields[xitr];
                        if (data[columnIndex] != null) {
                            columns = this.tableToTypeHandlerMap.updateColumnMapping(key, columnIndex, data[columnIndex].getClass());
                        }
                    }
                }
                catch (UnsupportedMapping mappingExp) {
                    final String srcTable = (String)event.metadata.get(Constant.TABLE_NAME);
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + targetTable + "}, Column index {" + columnIndex + "}";
                    throw new DatabaseWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                this.logger.error("Unhandled exception occured while biding after image", (Throwable)uExp);
                this.logger.error("Event caused this issue = " + event.toJSON());
                final Integer[] afterImageCols = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
                final Integer[] beforeIamgeCols = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
                this.logger.error("After Image column referred " + Arrays.toString((Object[])afterImageCols));
                this.logger.error("Before Image column referred " + Arrays.toString((Object[])beforeIamgeCols));
                try {
                    final String debugQuery = this.generateDML(event, targetTable);
                    this.logger.error("Debug SQL Statement {" + debugQuery + "}");
                }
                catch (Exception ex) {}
                throw uExp;
            }
            break;
        }
        return bindIndex;
    }
    
    protected int bindBefore(final String targetTable, TypeHandler[] columns, final HDEvent event, final PreparedStatement stmt, int startIndex) throws SQLException, DatabaseWriterException {
        //final int bindIndex = 0;
        int itr = 0;
        int columnIndex = 0;
        final Integer[] dataFields = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
        final Object[] data = event.before;
        while (true) {
            try {
                for (itr = 0; itr < dataFields.length; ++itr) {
                    columnIndex = dataFields[itr];
//            			System.out.println("----bindBefore length:" + data.length + "  fields length:" + dataFields.length +" columns:"+ columns.length +" columnIndex:" + columnIndex  + " startIndex:"+startIndex);
//            			System.out.println("----bindBefore columnIndex:"+columnIndex+"----"+data[columnIndex]);
            			++startIndex;
            			if (data[columnIndex] != null) {
                			columns[columnIndex].bind(stmt, startIndex, data[columnIndex]);
                    }
                }
            } catch (UpdateTypeMapException exp) {
                try {
                    final String key = TableToTypeHandlerMap.formKey(targetTable, event);
                    for (int xitr = itr; xitr < dataFields.length; ++xitr) {
                        columnIndex = dataFields[xitr];
                        if (data[columnIndex] != null) {
                            columns = this.tableToTypeHandlerMap.updateColumnMapping(key, columnIndex, data[columnIndex].getClass());
                        }
                    }
                }
                catch (UnsupportedMapping mappingExp) {
                    final String srcTable = (String)event.metadata.get(Constant.TABLE_NAME);
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + targetTable + "}";
                    throw new DatabaseWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                this.logger.error("Unhandled exception occured while biding before image", (Throwable)uExp);
                this.logger.error("Event caused this issue = " + event.toJSON());
                final Integer[] afterImageCols = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
                final Integer[] beforeIamgeCols = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
                this.logger.error("After Image column referred " + Arrays.toString((Object[])afterImageCols));
                this.logger.error("Before Image column referred " + Arrays.toString((Object[])beforeIamgeCols));
                try {
                    final String debugQuery = this.generateDML(event, targetTable);
                    this.logger.error("Debug SQL Statement {" + debugQuery + "}");
                }
                catch (Exception ex) {}
                throw uExp;
            }
            break;
        }
        return startIndex;
    }
    
    @Override
    public String generateDML(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        final StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(TableToTypeHandlerMap.formKey(targetTable, event));
        keyBuilder.append("-");
        keyBuilder.append(new String(event.dataPresenceBitMap));
        keyBuilder.append("-");
        keyBuilder.append(new String(event.beforePresenceBitMap));
        keyBuilder.append("-");
        keyBuilder.append(this.nullPattern(event, event.beforePresenceBitMap));
        final String key = keyBuilder.toString();
        final String pkUpdate = (String)event.metadata.get("PK_UPDATE");
//        String sql = (String)event.metadata.get("SQL");
//        if(sql != null) {
//        		return sql;
//        }
        boolean isMySQL = false;
        if(event.metadata.get("TargetType")!=null) {
        		if("MySQL".equalsIgnoreCase(event.metadata.get("TargetType").toString())) {
        			isMySQL = true;
        		}
        }
        String query = this.statementCache.get(key);
        if (query == null) {
        		String table = targetTable;
        		if(!isMySQL) {
	            if(table.indexOf(".")>-1) {
	            		String[] tabs = table.split("\\.");
	            		if(tabs.length == 2) {
	            			table = tabs[0] +".\""+ tabs[1]+"\"";
	            		}
	            }
        		}
            final Boolean isPkUpdate = "TRUE".equalsIgnoreCase(pkUpdate);
            final StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("UPDATE ");
            queryBuilder.append(table);
            queryBuilder.append(" SET ");
            final Table tableDef = (Table)TargetTableMap.getInstance(this.callbackIntf).getTableMeta(targetTable);
            final DatabaseColumn[] cols = tableDef.getColumns();
            final StringBuilder setClause = new StringBuilder();
            final Integer[] afterBitPtrn = BitPatternMap.getPattern(event, event.dataPresenceBitMap);
            for (int itr = 0; itr < afterBitPtrn.length; ++itr) {
	            	if(!isMySQL) {
	                if (!isPkUpdate) {
	                    if (!cols[afterBitPtrn[itr]].isKey()) {
	                        setClause.append(", ");
	                        setClause.append("\"" + cols[afterBitPtrn[itr]].getName() + "\"");
	                        setClause.append(" = ? ");
	                    }
	                }
	                else {
	                    setClause.append(", ");
	                    setClause.append("\""+cols[afterBitPtrn[itr]].getName()+"\"");
	                    setClause.append(" = ? ");
	                }
	            }else {
	            		if (!isPkUpdate) {
	                    if (!cols[afterBitPtrn[itr]].isKey()) {
	                        setClause.append(", ");
	                        setClause.append("" + cols[afterBitPtrn[itr]].getName() + "");
	                        setClause.append(" = ? ");
	                    }
	                }
	                else {
	                    setClause.append(", ");
	                    setClause.append(""+cols[afterBitPtrn[itr]].getName()+"");
	                    setClause.append(" = ? ");
	                }
	            }
            }
            if (setClause.length() > 0) {
                setClause.deleteCharAt(0);
                final StringBuilder whereClause = new StringBuilder();
                final Integer[] beforeBitPtrn = BitPatternMap.getPattern(event, event.beforePresenceBitMap);
                for (int itr2 = 0; itr2 < beforeBitPtrn.length; ++itr2) {
                    if (itr2 != 0) {
                        whereClause.append(" and ");
                    }
                    if(!isMySQL) {
                    		whereClause.append("\""+cols[beforeBitPtrn[itr2]].getName()+"\"");
                    }else {
                    		whereClause.append(""+cols[beforeBitPtrn[itr2]].getName()+"");
                    }
                    if (event.before[beforeBitPtrn[itr2]] != null) {
                    		whereClause.append(" = ? ");
                    }
                    else {
                        whereClause.append(" is null ");
                    }
                }
                queryBuilder.append((CharSequence)setClause);
                queryBuilder.append(" WHERE ");
                queryBuilder.append((CharSequence)whereClause);
                query = queryBuilder.toString();
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug("Query prepared and cahced {" + query + "}");
                }
            }
            else {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug("NO-OP Update, and this event will not be processed");
                }
                query = "";
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
        if (query.isEmpty()) {
            throw new DatabaseWriterException(Error.NO_OP_UPDATE, "Event will not be processed");
        }
        return query;
    }
    
    /**
     * 组装sql
     * @param data
     * @return
     */
    private String buildSql(Object data) {
		//System.out.println("----"+data.getClass().getTypeName());
    		StringBuffer whereClause = new StringBuffer(); 
	    	if(data instanceof DateTime) {
	    		DateTime date = (DateTime)data;
	    		Timestamp timestamp = new Timestamp(date.getMillis());
				whereClause.append(" = TO_TIMESTAMP('"+timestamp+",'yyyy-MM-dd HH24:MI:SS.FF')");
			}else if(data instanceof Date) {
				Date date = (Date)data;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				whereClause.append(" = TO_DATE('"+sdf.format(date)+",'yyyy-MM-dd HH24:MI:SS')");
			}else if(data instanceof Timestamp) {
				Timestamp timestamp = (Timestamp)data;
				whereClause.append(" = TO_Timestamp("+timestamp.getTime()+",'yyyy-MM-dd HH24:MI:SS.FF')");
	    	}else {
	    	   if (data instanceof Integer) {
	            whereClause.append(" = "+(int)data+" ");
	        }else if (data instanceof Double) {
	        		whereClause.append(" = "+(double)data+" ");
	        }else if (data instanceof Float) {
	        		whereClause.append(" = "+(float)data+" ");
	        }else if (data instanceof String) {
	        		whereClause.append(" = '"+data+"' ");
	        }else {
	        		whereClause.append(" = '"+data+"' ");
	        }
	    	}
    		return whereClause.toString();
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}
