package com.datasphere.proc;

import org.apache.log4j.*;

import com.datasphere.source.lib.utils.*;
import com.datasphere.utils.writers.common.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.Connection.*;
import com.datasphere.proc.Table.*;
import com.datasphere.proc.exception.*;
import com.datasphere.Tables.*;
import com.datasphere.Exceptions.*;
import com.datasphere.source.lib.meta.*;
import com.datasphere.TypeHandlers.*;
import com.datasphere.BitPatternMap.*;
import com.datasphere.recovery.*;
import com.datasphere.common.exc.*;

import org.joda.time.*;
import org.apache.kudu.client.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import org.json.*;
import java.io.*;

public class KuduBatchWriter implements BatchableWriter
{
    private static Logger logger;
    private final KuduClient client;
    private int batchSize;
    private KuduWriterConnection connectionObject;
    private TableToTypeHandlerMap tableToTypeHandlerMap;
    private SourceTableMap srcTblMap;
    private TargetTableMap tgtTblMap;
    private WildCardProcessor wildcardProcessor;
    private String defaultUpdateOperation;
    private boolean isbatchFirst;
    private boolean freshStart;
    private IgnoreException ignoreException;
    private List<String> exceptionList;
    private ColumnMap columnMap;
    private boolean isColumnListContainsOnlyPK;
    private long insertCount;
    private long updateCount;
    private long upsertCount;
    private long deleteCount;
    private long ignoredWhileRecoveryCount;
    
    public KuduBatchWriter(final KuduClient client, final int batchSize, final KuduWriterConnection connectionObject, final WildCardProcessor wildcardProcessor, final String defaultUpdateOperation, final boolean freshStart, final List<String> exceptionList, final ColumnMap columnMap) {
        this.isColumnListContainsOnlyPK = false;
        this.client = client;
        this.batchSize = batchSize;
        this.connectionObject = connectionObject;
        this.wildcardProcessor = wildcardProcessor;
        this.defaultUpdateOperation = defaultUpdateOperation;
        this.isbatchFirst = true;
        this.freshStart = freshStart;
        this.tableToTypeHandlerMap = new TableToTypeHandlerMap("kudu");
        this.srcTblMap = new SourceTableMap();
        this.tgtTblMap = new TargetTableMap();
        this.ignoreException = new IgnoreException();
        this.exceptionList = new ArrayList<String>(exceptionList);
        this.columnMap = columnMap;
        this.insertCount = 0L;
        this.updateCount = 0L;
        this.upsertCount = 0L;
        this.deleteCount = 0L;
        this.ignoredWhileRecoveryCount = 0L;
    }
    
    private void applyInsert(final EventDataObject edo, final KuduTable table, final KuduSession session, final String targetTableName, final UUID uuid, final UUID uuid2) throws Exception {
        final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTableName, uuid, uuid2);
        final Table tgtTbl = (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject);
        final DatabaseColumn[] dbColumns = tgtTbl.getColumns();
        TypeHandler[] columnss = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
        final Insert insert = table.newInsert();
        final PartialRow row = insert.getRow();
        int tgtindex = 0;
        int targetIndex = 0;
        while (true) {
            try {
                for (tgtindex = 0; tgtindex < dbColumns.length; ++tgtindex) {
                    final Object value = ((DBWriterColumn)dbColumns[tgtindex]).getFetcher().fetch(edo);
                    targetIndex = ((DBWriterColumn)dbColumns[tgtindex]).getTargetIndex();
                    if (value == null) {
                        columnss[tgtindex].bindNull((Object)row, targetIndex);
                    }
                    else {
                        columnss[tgtindex].bind((Object)row, targetIndex, value);
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (tgtindex = 0; tgtindex < dbColumns.length; ++tgtindex) {
                        final Object value2 = ((DBWriterColumn)dbColumns[tgtindex]).getFetcher().fetch(edo);
                        if (value2 != null) {
                            columnss = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, tgtindex, (Class)value2.getClass());
                        }
                    }
                }
                catch (UnsupportedMappingException mappingExp) {
                    final String srcTable = edo.getSourceTable();
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + table.getName() + "}, Handler Index {" + tgtindex + "}, Column index {" + targetIndex + "}";
                    throw new KuduWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                KuduBatchWriter.logger.error((Object)"Unhandled exception occurred while binding data ", (Throwable)uExp);
                KuduBatchWriter.logger.error((Object)("Event caused this issue = " + edo.toString()));
                throw uExp;
            }
            break;
        }
        session.apply((Operation)insert);
        ++this.insertCount;
    }
    
    private void applyUpdate(final EventDataObject edo, final KuduTable table, final KuduSession session, final String targetTableName, final UUID uuid, final UUID uuid2, final String sourceTable) throws Exception {
        final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTableName, uuid, uuid2);
        final Table tgtTbl = (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject);
        final DatabaseColumn[] dbColumns = tgtTbl.getColumns();
        TypeHandler[] columnss = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
        int itr = 0;
        int columnIndex = 0;
        final Update update = table.newUpdate();
        final PartialRow row = update.getRow();
        int targetIndex = 0;
        this.isColumnListContainsOnlyPK = true;
        final Integer[] dataFiled = BitPatternMap.getPattern(edo, edo.dataPresenceBitMap, this.srcTblMap.getTableMeta(uuid2, sourceTable), (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject), true);
        while (true) {
            try {
                for (itr = 0; itr < dataFiled.length; ++itr) {
                    columnIndex = dataFiled[itr];
                    targetIndex = ((DBWriterColumn)dbColumns[columnIndex]).getTargetIndex();
                    final Object value = ((DBWriterColumn)dbColumns[columnIndex]).getFetcher().fetch(edo);
                    if (value == null) {
                        columnss[columnIndex].bindNull((Object)row, targetIndex);
                    }
                    else {
                        columnss[columnIndex].bind((Object)row, targetIndex, value);
                    }
                    if (this.isColumnListContainsOnlyPK) {
                        this.isColumnListContainsOnlyPK = ((DBWriterColumn)dbColumns[columnIndex]).isKey();
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (int xitr = itr; xitr < dataFiled.length; ++xitr) {
                        columnIndex = dataFiled[xitr];
                        final Object dataObj = ((DBWriterColumn)dbColumns[columnIndex]).getFetcher().fetch(edo);
                        if (dataObj != null) {
                            columnss = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, columnIndex, (Class)dataObj.getClass());
                        }
                    }
                }
                catch (UnsupportedMappingException mappingExp) {
                    final String srcTable = edo.getSourceTable();
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + table.getName() + "},Handler Index {" + columnIndex + "}, Column index {" + targetIndex + "}";
                    throw new KuduWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                KuduBatchWriter.logger.error((Object)"Unhandled exception occurred while binding data ", (Throwable)uExp);
                KuduBatchWriter.logger.error((Object)("Event caused this issue = " + edo.toString()));
                throw uExp;
            }
            break;
        }
        if (this.isColumnListContainsOnlyPK) {
            KuduBatchWriter.logger.warn((Object)("ignoring this event as it has no column mapped columns to be updated " + edo.getDataList()));
        }
        else {
            session.apply((Operation)update);
            ++this.updateCount;
        }
    }
    
    private void applyUpsert(final EventDataObject edo, final KuduTable table, final KuduSession session, final String targetTableName, final UUID uuid, final UUID uuid2, final String sourceTable) throws Exception {
        final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTableName, uuid, uuid2);
        final Table tgtTbl = (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject);
        final DatabaseColumn[] dbColumns = tgtTbl.getColumns();
        TypeHandler[] columnss = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
        int itr = 0;
        int columnIndex = 0;
        final Upsert upsert = table.newUpsert();
        final PartialRow row = upsert.getRow();
        int targetIndex = 0;
        this.isColumnListContainsOnlyPK = true;
        final Integer[] dataFiled = BitPatternMap.getPattern(edo, edo.dataPresenceBitMap, this.srcTblMap.getTableMeta(uuid2, sourceTable), (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject), true);
        while (true) {
            try {
                for (itr = 0; itr < dataFiled.length; ++itr) {
                    columnIndex = dataFiled[itr];
                    targetIndex = ((DBWriterColumn)dbColumns[columnIndex]).getTargetIndex();
                    final Object value = ((DBWriterColumn)dbColumns[columnIndex]).getFetcher().fetch(edo);
                    if (value == null) {
                        columnss[columnIndex].bindNull((Object)row, targetIndex);
                    }
                    else {
                        columnss[columnIndex].bind((Object)row, targetIndex, value);
                    }
                    if (this.isColumnListContainsOnlyPK) {
                        this.isColumnListContainsOnlyPK = ((DBWriterColumn)dbColumns[columnIndex]).isKey();
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (int xitr = itr; xitr < dataFiled.length; ++xitr) {
                        columnIndex = dataFiled[xitr];
                        final Object dataObj = ((DBWriterColumn)dbColumns[columnIndex]).getFetcher().fetch(edo);
                        if (dataObj != null) {
                            columnss = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, columnIndex, (Class)dataObj.getClass());
                        }
                    }
                }
                catch (UnsupportedMappingException mappingExp) {
                    final String srcTable = edo.getSourceTable();
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + table.getName() + "},Handler Index {" + columnIndex + "}, Column index {" + targetIndex + "}";
                    throw new KuduWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                KuduBatchWriter.logger.error((Object)"Unhandled exception occurred while binding data ", (Throwable)uExp);
                KuduBatchWriter.logger.error((Object)("Event caused this issue = " + edo.toString()));
                throw uExp;
            }
            break;
        }
        if (this.isColumnListContainsOnlyPK) {
            KuduBatchWriter.logger.warn((Object)("ignoring this event as it has no column mapped columns to be updated " + edo.getDataList()));
        }
        else {
            session.apply((Operation)upsert);
            ++this.upsertCount;
        }
    }
    
    private void applyDelete(final EventDataObject edo, final KuduTable table, final KuduSession session, final String targetTableName, final UUID uuid, final UUID uuid2) throws Exception {
        final String typeHandlerKey = TableToTypeHandlerMap.formKey(targetTableName, uuid, uuid2);
        final Table tgtTbl = (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject);
        final DatabaseColumn[] dbColumns = tgtTbl.getColumns();
        TypeHandler[] columnss = this.tableToTypeHandlerMap.getColumnMapping(typeHandlerKey);
        final Delete delete = table.newDelete();
        final PartialRow row = delete.getRow();
        int tgtindex = 0;
        while (true) {
            try {
                for (tgtindex = 0; tgtindex < dbColumns.length; ++tgtindex) {
                    if (((DBWriterColumn)dbColumns[tgtindex]).isKey()) {
                        final Object value = ((DBWriterColumn)dbColumns[tgtindex]).getFetcher().fetch(edo);
                        if (value == null) {
                            columnss[tgtindex].bindNull((Object)row, tgtindex);
                        }
                        else {
                            columnss[tgtindex].bind((Object)row, tgtindex, value);
                        }
                    }
                }
            }
            catch (UpdateTypeMapException exp) {
                try {
                    for (tgtindex = 0; tgtindex < dbColumns.length; ++tgtindex) {
                        final Object value2 = ((DBWriterColumn)dbColumns[tgtindex]).getFetcher().fetch(edo);
                        if (value2 != null) {
                            columnss = this.tableToTypeHandlerMap.updateColumnMapping(typeHandlerKey, tgtindex, (Class)value2.getClass());
                        }
                    }
                }
                catch (UnsupportedMappingException mappingExp) {
                    final String srcTable = edo.getSourceTable();
                    final String errMsg = "Mapping of SourceType {" + mappingExp.sourceType + "} to TargetType {" + mappingExp.targetType + "} is not supported. Source Table {" + srcTable + "} Target Table {" + table.getName() + "}, Column index {" + tgtindex + "}";
                    throw new KuduWriterException(errMsg);
                }
                continue;
            }
            catch (Exception uExp) {
                KuduBatchWriter.logger.error((Object)"Unhandled exception occurred while binding data ", (Throwable)uExp);
                KuduBatchWriter.logger.error((Object)("Event caused this issue = " + edo.toString()));
                throw uExp;
            }
            break;
        }
        session.apply((Operation)delete);
        ++this.deleteCount;
    }
    
    public List<Position> write(String targetTableName, final List<EventDataObject> batchedEvents) throws Exception {
        final List<Position> positions = new ArrayList<Position>();
        final ArrayList<String> targetTable = (ArrayList<String>)this.wildcardProcessor.getMapForSourceTable(batchedEvents.get(0).getSourceTable());
        if (targetTable == null) {
            throw new AdapterException("No Mapping found for source table " + batchedEvents.get(0).getSourceTable());
        }
        targetTableName = targetTable.get(0);
        if (!this.client.tableExists(targetTableName)) {
            throw new KuduWriterException(Error.TARGET_TABLE_DOESNOT_EXISTS, targetTableName);
        }
        final String key = TableToTypeHandlerMap.formKey(targetTableName, batchedEvents.get(0).getsourceUUID(), batchedEvents.get(0).gettargetUUID());
        final TypeHandler[] tblColumnMapping = this.tableToTypeHandlerMap.getColumnMapping(key);
        if (tblColumnMapping == null) {
            try {
                final Table srcTblDef = this.srcTblMap.getTableMeta(batchedEvents.get(0).gettargetUUID(), batchedEvents.get(0).getSourceTable());
                final Table targetTableDef = (Table)this.tgtTblMap.getTableMeta(targetTableName, this.connectionObject);
                if (targetTableDef != null) {
                    final Table tgtMappedTable = this.columnMap.getMappedTable(srcTblDef, targetTableDef);
                    tgtMappedTable.setName(targetTableName);
                    this.tableToTypeHandlerMap.initializeForTable(key, tgtMappedTable, this.connectionObject.targetType());
                    this.tgtTblMap.updateTableRef(tgtMappedTable);
                }
            }
            catch (KuduWriterException exp) {
                throw exp;
            }
        }
        final KuduTable table = this.connectionObject.getTableInstance(targetTableName);
        final KuduSession session = this.client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(this.batchSize = batchedEvents.size() + 100);
        session.setMutationBufferLowWatermark(1.0f);
        try {
            for (final EventDataObject event : batchedEvents) {
                final String type = event.getType();
                switch (type) {
                    case "INSERT":
                    case "SELECT": {
                        this.applyInsert(event, table, session, targetTableName, batchedEvents.get(0).getsourceUUID(), batchedEvents.get(0).gettargetUUID());
                        break;
                    }
                    case "UPDATE": {
                        if (this.defaultUpdateOperation.equals("UPSERT")) {
                            this.applyUpsert(event, table, session, targetTableName, batchedEvents.get(0).getsourceUUID(), batchedEvents.get(0).gettargetUUID(), batchedEvents.get(0).getSourceTable());
                            break;
                        }
                        this.applyUpdate(event, table, session, targetTableName, batchedEvents.get(0).getsourceUUID(), batchedEvents.get(0).gettargetUUID(), batchedEvents.get(0).getSourceTable());
                        break;
                    }
                    case "DELETE": {
                        this.applyDelete(event, table, session, targetTableName, batchedEvents.get(0).getsourceUUID(), batchedEvents.get(0).gettargetUUID());
                        break;
                    }
                }
                if (!this.isColumnListContainsOnlyPK) {
                    positions.add(event.getPos());
                }
                else {
                    this.isColumnListContainsOnlyPK = false;
                }
            }
            final long startTime = System.currentTimeMillis();
            final DateTime lastExecutionTime = DateTime.now();
            final List<OperationResponse> responses = (List<OperationResponse>)session.flush();
            final long endTime = System.currentTimeMillis();
            final long executionLatency = endTime - startTime;
            final long totalRows = responses.size();
            for (final OperationResponse response : responses) {
                if (response.hasRowError()) {
                    if (this.isbatchFirst && !this.freshStart) {
                        ++this.ignoredWhileRecoveryCount;
                        KuduBatchWriter.logger.warn((Object)("while starting this row couldnot persisit : " + response.getRowError().getOperation().getRow() + " : " + response.getRowError().toString()));
                    }
                    else {
                        if (!this.ignoreException.checkExceptionList(this.exceptionList, response.getRowError())) {
                            throw new KuduWriterException("Couldnot persist row : " + response.getRowError().getOperation().getRow() + " : " + response.getRowError().toString());
                        }
                        continue;
                    }
                }
            }
            this.connectionObject.setLatency(executionLatency);
            this.connectionObject.setTotalEvent(totalRows);
            this.connectionObject.setOperationMetrics(this.operationMetrics());
            this.connectionObject.setlastIOTime(lastExecutionTime.toString());
            this.isbatchFirst = false;
            session.close();
        }
        catch (Exception ex) {
            KuduBatchWriter.logger.error((Object)("Error while writing batch on table {" + targetTableName + "}"), (Throwable)ex);
            throw new AdapterException("Error while writing batch on table " + targetTableName + " Reason " + ex.getMessage(), (Throwable)ex);
        }
        return positions;
    }
    
    public String operationMetrics() {
        final JSONObject metricsObject = new JSONObject();
        final ObjectMapper mapper = new ObjectMapper();
        try {
            metricsObject.put("INSERT", this.insertCount);
            metricsObject.put("UPDATE", this.updateCount);
            metricsObject.put("DELETE", this.deleteCount);
            metricsObject.put("IGNORED_WHILE_RECOVERY", this.ignoredWhileRecoveryCount);
            metricsObject.put("UPSERT", this.upsertCount);
        }
        catch (JSONException e1) {
            KuduBatchWriter.logger.warn((Object)("Exception while converting metrics object to JSON {" + e1.getMessage() + "}"));
        }
        try {
            final Object json = mapper.readValue(metricsObject.toString(), (Class)Object.class);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        }
        catch (IOException e2) {
            KuduBatchWriter.logger.warn((Object)("Exception while converting metrics object to JSON {" + e2.getMessage() + "}"));
            return "";
        }
    }
    
    static {
        KuduBatchWriter.logger = Logger.getLogger((Class)KuduBatchWriter.class);
    }
    
    public enum PartitionType
    {
        RANGE, 
        HASH;
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}
