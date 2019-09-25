package com.datasphere.OperationHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.BitPattern.BitPatternMap;
import com.datasphere.Table.SourceTableMap;
import com.datasphere.Table.Table;
import com.datasphere.Table.TargetTableMap;
import com.datasphere.TypeHandler.TableToTypeHandlerMap;
import com.datasphere.TypeHandler.TypeHandler;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public abstract class OperationHandler
{
    private Logger logger;
    protected final String SEPARATOR = "-";
    protected final String KEY_SEPARATOR = ";";
    Map<String, String> statementCache;
    DBInterface callbackIntf;
    TableToTypeHandlerMap tableToTypeHandlerMap;
    SourceTableMap srcTblMap;
    TargetTableMap tgtTblMap;
    Map<String, Integer[]> localPatternMap;
    long operationsHandled;
    String handlerName;
    
    public OperationHandler() {
        this.logger = LoggerFactory.getLogger((Class)OperationHandler.class);
        this.statementCache = new HashMap<String, String>();
    }
    
    public OperationHandler(final DBInterface callbackInterface) {
        this.logger = LoggerFactory.getLogger((Class)OperationHandler.class);
        this.statementCache = new HashMap<String, String>();
        this.callbackIntf = callbackInterface;
        this.tableToTypeHandlerMap = this.callbackIntf.getTableToTypeHandlerMap();
        this.srcTblMap = callbackInterface.getSourceTableMap();
        this.tgtTblMap = callbackInterface.getTargetTableMap();
        this.localPatternMap = new HashMap<String, Integer[]>();

    }
    
    public boolean objectBasedOperation() {
        return true;
    }
    
    public void invalidateCacheFor(final String table) {
        final String keys = this.statementCache.get(table);
        if (keys != null) {
            final String[] parts = keys.split(";");
            for (int itr = 0; itr < parts.length; ++itr) {
                if (this.logger.isInfoEnabled()) {
                    this.logger.info("Invalidating cache for {" + table + "} Cached key{" + parts[itr] + "}");
                }
                this.statementCache.remove(parts[itr]);
            }
            this.statementCache.remove(table);
        }
    }
    
    public void validate(final HDEvent event) throws DatabaseWriterException {
    		if(event.metadata.get("Rollback") != null && "1".equalsIgnoreCase(event.metadata.get("Rollback").toString())) {
			return;
		}
        if (event.data == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATA, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
        if (event.dataPresenceBitMap == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATAPRESENCEBITMAP, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
        // MySQL 到 MySQL 全量同步报错  & 文件同步到数据库
        if (event.metadata.get(Constant.TABLE_NAME) == null && event.metadata.get("FileName") == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_METADATA_TABLENAME, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
    }
    
    public abstract void bind(final String p0, final HDEvent p1, final PreparedStatement p2) throws SQLException, DatabaseWriterException;
    
    public void bindNull(final TypeHandler column, final PreparedStatement stmt, final int bindIndex) throws SQLException {
        column.bindNull(stmt, bindIndex);
    }
    
    public abstract String generateDML(final HDEvent p0, final String p1) throws SQLException, DatabaseWriterException;
    
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        return null;
    }
    
    public void onDDLOperation(final String targetTable, final HDEvent event) throws DatabaseWriterException {
    }
    
    protected String nullPattern(final HDEvent event, final byte[] bitMap) {
        final StringBuilder nullPtrn = new StringBuilder();
        final Integer[] bitPtrn = BitPatternMap.getPattern(event, bitMap);
        Object[] data;
        if (event.beforePresenceBitMap == bitMap) {
            data = event.before;
        }
        else {
            data = event.data;
        }
        for (int itr = 0; itr < bitPtrn.length; ++itr) {
            if (data[bitPtrn[itr]] == null) {
                nullPtrn.append(bitPtrn[itr].toString() + ",");
            }
        }
        return nullPtrn.toString();
    }
    
    public String getComponentName(final HDEvent event) {
        final String catalog = (String)event.metadata.get(Constant.CATALOG_NAME);
        final String schema = (String)event.metadata.get(Constant.SCHEMA_NAME);
        final String opType = (String)event.metadata.get(Constant.OPERATION_TYPE);
        String tableName = null;
        if (Constant.DDL_OPERATION.equalsIgnoreCase(opType)) {
            tableName = (String)event.metadata.get(Constant.OBJECT_NAME);
        }
        else {
            tableName = (String)event.metadata.get(Constant.TABLE_NAME);
        }
        String fullyQualifiedName = catalog;
        if (schema != null && !schema.isEmpty()) {
            fullyQualifiedName = ((fullyQualifiedName != null && !fullyQualifiedName.isEmpty()) ? (fullyQualifiedName + "." + schema) : schema);
        }
        fullyQualifiedName = ((fullyQualifiedName != null && !fullyQualifiedName.isEmpty()) ? (fullyQualifiedName + "." + tableName) : tableName);
        return fullyQualifiedName;
    }
    
    public boolean isDDLOperation() {
        return false;
    }
    
    public String generateDebugQuery(final HDEvent event, final String targetTable) throws DatabaseWriterException {
        return "";
    }
    
    public Integer[] getPattern(final HDEvent event, final byte[] bitMap, final String source, final String target) {
        return this.getPattern(event, bitMap, source, target, true);
    }
    
    public Integer[] getPattern(final HDEvent event, final byte[] bitMap, final String source, final String target, final boolean includeUnmangedColumns) {
        try {
            final String key = "" + includeUnmangedColumns + "-" + BitPatternMap.toString(bitMap) + "-" + source + "-" + event.typeUUID.toString() + "-" + target;
            Integer[] bitPattern = this.localPatternMap.get(key);
            if (bitPattern == null) {
                final Table srcTbl = this.srcTblMap.getTableMeta(source);
                final Table tgtTbl = (Table)this.tgtTblMap.getTableMeta(target);
                bitPattern = BitPatternMap.getPattern(event, bitMap, srcTbl, tgtTbl, includeUnmangedColumns);
                this.localPatternMap.put(key, bitPattern);
            }
            return bitPattern;
        }
        catch (DatabaseWriterException e) {
            this.logger.error("Got exception in OperationHandler::getPattern()", (Throwable)e);
            return null;
        }
    }
    
    public void operationsHandled(final long operationCnt) {
        this.operationsHandled += operationCnt;
    }
    
    public long operationHandled() {
        return this.operationsHandled;
    }
    
    public String handlerName() {
        return this.handlerName;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
