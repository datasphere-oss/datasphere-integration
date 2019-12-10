package com.datasphere.proc;

import com.datasphere.anno.*;

import org.apache.log4j.*;
import org.apache.kudu.client.*;

import com.datasphere.source.lib.utils.*;
import com.datasphere.Tables.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.runtime.meta.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.CheckPoint.*;
import com.datasphere.proc.Connection.*;
import com.datasphere.proc.events.*;
import com.datasphere.proc.exception.*;
import com.datasphere.security.*;
import com.datasphere.common.exc.*;
import com.datasphere.utils.writers.common.*;
import java.lang.reflect.*;
import com.datasphere.classloading.*;
import com.datasphere.runtime.*;
import com.datasphere.metaRepository.*;
import java.util.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.monitor.*;
import java.io.*;

@PropertyTemplate(name = "KuduWriter", type = AdapterType.target, properties = { @PropertyTemplateProperty(name = "Tables", type = String.class, required = true, defaultValue = ""), @PropertyTemplateProperty(name = "KuduClientConfig", type = String.class, required = true, defaultValue = ""), @PropertyTemplateProperty(name = "CheckPointTable", type = String.class, required = false, defaultValue = "CHKPOINT"), @PropertyTemplateProperty(name = "UpdateAsUpsert", type = Boolean.class, required = false, defaultValue = "false"), @PropertyTemplateProperty(name = "IgnorableException", type = String.class, required = false, defaultValue = ""), @PropertyTemplateProperty(name = "PKUpdateHandlingMode", type = WriterProperties.PKUpdateHandlingMode.class, required = false, defaultValue = "ERROR"), @PropertyTemplateProperty(name = "BatchPolicy", type = String.class, required = false, defaultValue = "EventCount:1000, Interval:30") }, outputType = HDEvent.class, requiresFormatter = false)
public class KuduWriter extends BaseDataStoreWriter
{
    private static Logger logger;
    private String authenticationPolicy;
    private String authenticationName;
    private String principal;
    private String keytabPath;
    private boolean kerberosEnabled;
    private List<String> kuduMasterAddresses;
    private long socketreadtimeout;
    private long operationtimeout;
    private KuduClient client;
    private Map<String, String> srctarmap;
    private long batchInterval;
    private long batchCount;
    public KuduWriterConnection connectionObject;
    private String tableList;
    private WildCardProcessor wildcardProcessor;
    private CheckpointTableImpl check;
    private List<String> exceptionList;
    private final String KEY_PK_UPDATE_HANDLING = "PKUpdateHandlingMode";
    private WriterProperties.PKUpdateHandlingMode pkupdatemode;
    private ColumnMap columnMap;
    
    public KuduWriter() {
        this.authenticationPolicy = null;
        this.kerberosEnabled = false;
        this.kuduMasterAddresses = new ArrayList<String>();
        this.socketreadtimeout = 10000L;
        this.operationtimeout = 30000L;
        this.batchInterval = 0L;
        this.batchCount = 0L;
        this.tableList = null;
        this.exceptionList = new ArrayList<String>();
        this.pkupdatemode = WriterProperties.PKUpdateHandlingMode.IGNORE;
    }
    
    public WriterProperties initWriter(final Map<String, Object> properties) throws Exception {
        this.srctarmap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        if (properties.containsKey("authenticationPolicy") && properties.get("authenticationPolicy") != null && !properties.get("authenticationPolicy").toString().isEmpty()) {
        		this.authenticationPolicy = properties.get("authenticationPolicy").toString();
            this.validateAuthenticationPolicy(this.authenticationPolicy);
        }
        final String checkPointTableKey = this.getTargetIdentifier((Map)properties);
        properties.put("checkPointTableKey", checkPointTableKey);
        String tables = null;
        final Property prop = new Property((Map)properties);
        if (properties.containsKey("IgnorableException") && properties.get("IgnorableException") != null && !properties.get("IgnorableException").toString().trim().isEmpty()) {
            final String exceptionString = prop.getString("IgnorableException", (String)null);
            final String[] exceptionArray = exceptionString.split(",");
            for (int i = 0; i < exceptionArray.length; ++i) {
                this.exceptionList.add(exceptionArray[i].trim().toUpperCase());
            }
        }
        if (properties.containsKey("Tables") && properties.get("Tables") != null && !properties.get("Tables").toString().isEmpty()) {
            this.tableList = prop.getString("Tables", (String)null);
            if (this.typedEvent) {
                final MetaInfo.Stream typedStream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)properties.get("streamUUID"), WASecurityManager.TOKEN);
                final MetaInfo.Type streamDataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(typedStream.dataType, WASecurityManager.TOKEN);
                final String typeName = streamDataType.name;
                this.tableList = typeName + "," + this.tableList;
                tables = this.tableList;
                properties.put("Tables", tables);
            }
            else {
                tables = properties.get("Tables").toString();
            }
            if (!tables.contains(",")) {
                throw new KuduWriterException(Error.INCORRECT_TABLE_MAP, "Please Specify Tables property in correct format");
            }
            this.srctarmap = (Map<String, String>)ParserUtility.get1PartTables((String)properties.get("Tables"));
        }
        final String chkPointTableName = prop.getString("CheckPointTable", "CHKPOINT");
        if (chkPointTableName.trim().isEmpty()) {
            throw new KuduWriterException(Error.TABLES_NOT_SPECIFIED, "CheckPointTable not specified");
        }
        final String[] includeTablesArray = this.tableList.split(";");
        final String[] excludeTablesArray = null;
        this.wildcardProcessor = new WildCardProcessor();
        try {
            this.wildcardProcessor.initializeWildCardProcessor(includeTablesArray, excludeTablesArray);
        }
        catch (IllegalArgumentException exception) {
            throw new KuduWriterException(Error.INCORRECT_TABLE_MAP, exception.getMessage());
        }
        if (properties.containsKey("BatchPolicy") && properties.get("BatchPolicy") != null && !properties.get("BatchPolicy").toString().isEmpty()) {
            final Long[] values = ParserUtility.getBatchPolicyAndBatchInterval((String)properties.get("BatchPolicy"), 1000L, 60000L);
            this.batchCount = values[0];
            this.batchInterval = values[1];
        }
        this.initializeKuduClient(properties.get("KuduClientConfig").toString());
        if (KuduWriter.logger.isDebugEnabled()) {
            KuduWriter.logger.debug((Object)("Kudu Writer initialized with properties master address" + this.kuduMasterAddresses + " Table Map: " + this.srctarmap + " operationtimeout:" + this.client.getDefaultOperationTimeoutMs()));
        }
        String defaultUpdateOperation = null;
        if (properties.containsKey("UpdateAsUpsert") && properties.get("UpdateAsUpsert") != null && !properties.get("UpdateAsUpsert").toString().trim().isEmpty()) {
            final boolean UpdateAsUpsert = (Boolean)properties.get("UpdateAsUpsert");
            if (UpdateAsUpsert) {
                defaultUpdateOperation = "UPSERT";
            }
            else {
                defaultUpdateOperation = "UPDATE";
            }
        }
        else {
            defaultUpdateOperation = "UPSERT";
        }
        this.check = new CheckpointTableImpl(chkPointTableName, properties.get("checkPointTableKey").toString(), this.connectionObject);
        final Position startStatus = this.check.getAckPosition();
        final boolean freshStart = startStatus == null;
        if (properties.containsKey("PKUpdateHandlingMode") && properties.get("PKUpdateHandlingMode") != null && !properties.get("PKUpdateHandlingMode").toString().isEmpty()) {
            final String pkupdatemode = properties.get("PKUpdateHandlingMode").toString().trim();
            try {
                this.pkupdatemode = WriterProperties.PKUpdateHandlingMode.valueOf(pkupdatemode.toUpperCase());
            }
            catch (IllegalArgumentException ex) {
                throw new AdapterException("PKUpdateHandlingMode not provided properly, please use one of these IGNORE,ERROR,DELETEANDINSERT");
            }
        }
        final Property newProp = new Property((Map)properties);
        this.columnMap = new ColumnMap(newProp, this.wildcardProcessor);
        return new WriterProperties(this.batchInterval, this.batchCount, (Map)this.srctarmap, (Checkpointer)this.check, (BatchableWriter)new KuduBatchWriter(this.client, (int)(Object)new Long(this.batchCount), this.connectionObject, this.wildcardProcessor, defaultUpdateOperation, freshStart, this.exceptionList, this.columnMap), this.pkupdatemode);
    }
    
    private void initializeKuduClient(final String kuduClientConfig) throws KuduWriterException {
        final Map<String, Object> properties = new HashMap<String, Object>();
        final Map<String, String> config = (Map<String, String>)ParserUtility.getNameValuePairs(kuduClientConfig);
        String value = config.get("master.addresses");
        if (value != null && !value.isEmpty()) {
            final String[] split2;
            final String[] splits = split2 = value.split(",");
            for (final String split : split2) {
                this.kuduMasterAddresses.add(split);
            }
            properties.put("kuduMasterAddresses", this.kuduMasterAddresses);
            value = config.get("socketreadtimeout");
            if (value != null && !value.isEmpty()) {
                try {
                    this.socketreadtimeout = Long.parseLong(value);
                }
                catch (IllegalArgumentException ex) {
                    KuduWriter.logger.warn((Object)("Improper value for socketreadtimeout provided " + this.socketreadtimeout + ". Taking default value " + this.socketreadtimeout));
                }
            }
            properties.put("socketreadtimeout", this.socketreadtimeout);
            value = config.get("operationtimeout");
            if (value != null && !value.isEmpty()) {
                try {
                    this.operationtimeout = Long.parseLong(value);
                }
                catch (IllegalArgumentException ex) {
                    KuduWriter.logger.warn((Object)("Improper value for operationtimeout provided " + this.operationtimeout + ". Taking default value " + this.operationtimeout));
                }
            }
            properties.put("operationtimeout", this.operationtimeout);
            final Property prop = new Property((Map)properties);
            this.connectionObject = KuduWriterConnection.getConnection(prop);
            this.client = this.connectionObject.getClient();
            return;
        }
        throw new IllegalArgumentException("master.addresses is an important key for connecting to cluster");
    }
    
    private void validateAuthenticationPolicy(final String authenticationPolicy) throws AdapterException {
        final Map<String, Object> authenticationPropertiesMap = this.extractAuthenticationProperties(authenticationPolicy);
        if (!this.authenticationName.equalsIgnoreCase("kerberos")) {
            throw new AdapterException("Specified authentication " + this.authenticationName + " is not supported");
        }
        this.kerberosEnabled = true;
        this.principal = authenticationPropertiesMap.get("principal") == null? null : authenticationPropertiesMap.get("principal").toString();
        this.keytabPath = authenticationPropertiesMap.get("keytabpath") == null? null : authenticationPropertiesMap.get("keytabpath").toString();
        if ((this.principal == null || this.principal.trim().isEmpty()) && (this.keytabPath == null || this.keytabPath.trim().isEmpty())) {
            throw new AdapterException("Principal or Keytab path required for kerberos authentication cannot be empty or null");
        }
    }
    
    private Map<String, Object> extractAuthenticationProperties(final String authenticationPolicyName) {
        final Map<String, Object> authenticationPropertiesMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        final String[] extractedValues = authenticationPolicyName.split(",");
        boolean isAuthenticationPolicyNameExtracted = false;
        for (final String value : extractedValues) {
            final String[] properties = value.split(":");
            if (properties.length > 1) {
                authenticationPropertiesMap.put(properties[0], properties[1]);
            }
            else if (!isAuthenticationPolicyNameExtracted) {
                this.authenticationName = properties[0];
                isAuthenticationPolicyNameExtracted = true;
            }
        }
        return authenticationPropertiesMap;
    }
    
    public List<String> getKuduMasterAddresses() {
        return this.kuduMasterAddresses;
    }
    
    public long getSocketreadtimeout() {
        return this.socketreadtimeout;
    }
    
    public long getOperationtimeout() {
        return this.operationtimeout;
    }
    
    public void closeWriter() throws Exception {
        this.client.close();
    }
    
    public Position getWaitPosition() throws Exception {
        final Position pos = this.check.getAckPosition();
        return pos;
    }
    
    protected void updateAndExpireBatch(final HDEvent hdevent) throws Exception {
        if (!hdevent.metadata.containsKey("OperationName")) {
            throw new AdapterException("Invalid HDEvent format. " + this.getClass().getSimpleName() + " supports HDEvent originating from CDC and database sources only. For other sources, please send the typed event stream.");
        }
        if (!hdevent.metadata.containsKey("TableName")) {
            KuduWriter.logger.warn((Object)("Ignoring event: " + hdevent + " a field " + "TableName" + " is required in metadata"));
            return;
        }
        final String sourceTableName = hdevent.metadata.get("TableName").toString();
        final String isPKupdate = hdevent.metadata.get("PK_UPDATE") == null ? null : hdevent.metadata.get("PK_UPDATE").toString();
        if (isPKupdate != null && isPKupdate.trim().equalsIgnoreCase("true")) {
            this.handlePKUpdateEvents(hdevent, sourceTableName);
            return;
        }
        final EventDataObject edo = this.getEventDataObject(hdevent, hdevent.data);
        if (!this.getSourceTargetMap().containsKey(sourceTableName)) {
            if (this.wildcardProcessor.getMapForSourceTable(sourceTableName) == null) {
                throw new AdapterException("No Mapping found for " + sourceTableName);
            }
            final String tartable = this.wildcardProcessor.getMapForSourceTable(sourceTableName).get(0);
            this.getSourceTargetMap().put(sourceTableName, tartable);
            this.targetDataMap.put(tartable, new ArrayList());
        }
        if (this.getSourceTargetMap().containsKey(sourceTableName)) {
            final String targetTable = this.getSourceTargetMap().get(sourceTableName);
            if (this.prevTargetTable == null) {
                this.prevTargetTable = targetTable;
                synchronized (this.targetDataMap) {
                    this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo);
                }
            }
            else if (this.prevTargetTable.equals(targetTable)) {
                synchronized (this.targetDataMap) {
                    this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo);
                }
            }
            else {
                this.prevTargetTable = targetTable;
                this.flushAll();
                synchronized (this.targetDataMap) {
                    this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo);
                }
                this.eventCounter = 0L;
            }
        }
        else {
            KuduWriter.logger.warn((Object)("Ignoring event " + hdevent + " No Mapping found, source not mapped with appropriate target"));
        }
        ++this.eventCounter;
        if (this.eventCounter >= this.writerProps.getBatchCount()) {
            this.flushAll();
            this.eventCounter = 0L;
        }
    }
    
    protected void handlePKUpdateEvents(final HDEvent event, final String sourceTableName) throws Exception {
        if (this.writerProps.getPKUpdateHandlingMode().equals((Object)WriterProperties.PKUpdateHandlingMode.IGNORE)) {
            KuduWriter.logger.warn((Object)("PK_UPDATE found on " + sourceTableName + ", ignoring event " + event));
            return;
        }
        if (this.writerProps.getPKUpdateHandlingMode().equals((Object)WriterProperties.PKUpdateHandlingMode.ERROR)) {
            throw new AdapterException("PK_UPDATE found on " + sourceTableName + ", target set to handle PKUpdate with error, event affected " + event);
        }
        final List<EventDataObject> pkedos = new ArrayList<EventDataObject>();
        if (this.writerProps.getPKUpdateHandlingMode().equals((Object)WriterProperties.PKUpdateHandlingMode.DELETEANDINSERT)) {
            event.metadata.put("OperationName", "DELETE");
            final Object[] originalData = event.data;
            event.data = event.before;
            final EventDataObject edo1 = this.getEventDataObject(event, event.data, "PKUPDATE");
            event.metadata.put("OperationName", "INSERT");
            event.data = originalData;
            final EventDataObject edo2 = this.getEventDataObject(event, event.data);
            pkedos.add(edo1);
            pkedos.add(edo2);
        }
        for (final EventDataObject edo3 : pkedos) {
            if (this.getSourceTargetMap().containsKey(sourceTableName)) {
                final String targetTable = this.getSourceTargetMap().get(sourceTableName);
                if (this.prevTargetTable == null) {
                    this.prevTargetTable = targetTable;
                    synchronized (this.targetDataMap) {
                        this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo3);
                    }
                }
                else if (this.prevTargetTable.equals(targetTable)) {
                    synchronized (this.targetDataMap) {
                        this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo3);
                    }
                }
                else {
                    this.prevTargetTable = targetTable;
                    this.flushAll();
                    synchronized (this.targetDataMap) {
                        this.targetDataMap.get(this.getSourceTargetMap().get(sourceTableName)).add(edo3);
                    }
                    this.eventCounter = 0L;
                }
            }
            else {
                KuduWriter.logger.warn((Object)("Ignoring event " + event + " No Mapping found, source not mapped with appropriate target"));
            }
        }
        pkedos.clear();
        ++this.eventCounter;
        if (this.eventCounter >= this.writerProps.getBatchCount()) {
            this.flushAll();
            this.eventCounter = 0L;
        }
    }
    
    protected EventDataObject getEventDataObject(final HDEvent event, final Object[] dataOrBeforeArray) throws AdapterException, MetaDataRepositoryException {
        Field[] fieldsOfThisTable = null;
        List<String> keys = null;
        if (event.typeUUID != null) {
            if (this.typeUUIDCache.containsKey(event.typeUUID) && this.typeUUIDKeyCache.containsKey(event.typeUUID)) {
                fieldsOfThisTable = this.typeUUIDCache.get(event.typeUUID);
                keys = this.typeUUIDKeyCache.get(event.typeUUID);
            }
            else {
                try {
                    final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, WASecurityManager.TOKEN);
                    keys = (List<String>)dataType.keyFields;
                    this.typeUUIDKeyCache.put(event.typeUUID, keys);
                    final Class<?> typeClass = (Class<?>)HDLoader.get().loadClass(dataType.className);
                    fieldsOfThisTable = typeClass.getDeclaredFields();
                    this.typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
                }
                catch (Exception e) {
                    KuduWriter.logger.warn((Object)("Unable to fetch the type for table " + event.metadata.get("TableName") + e));
                    throw new AdapterException("Unable to fetch the type for table " + event.metadata.get("TableName"), (Throwable)e);
                }
            }
        }
        final Map<String, Object> columnsAndDataMap = new LinkedHashMap<String, Object>();
        for (Integer i = 0; i < dataOrBeforeArray.length; ++i) {
            final boolean isPresent = BuiltInFunc.IS_PRESENT(event, dataOrBeforeArray, (int)i);
            if (isPresent) {
                if (fieldsOfThisTable != null) {
                    columnsAndDataMap.put(fieldsOfThisTable[i].getName(), (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
                }
                else {
                    columnsAndDataMap.put(i + "", (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
                }
            }
        }
        return new EventDataObject((Map)columnsAndDataMap, event.metadata.get("OperationName").toString(), (List)keys, this.currentReceivedPosition, fieldsOfThisTable, event.sourceUUID, event.typeUUID, event.metadata.get("TableName").toString(), event.data, (Map)event.metadata, (Map)event.userdata, event.before, event.beforePresenceBitMap, event.dataPresenceBitMap);
    }
    
    protected EventDataObject getEventDataObject(final HDEvent event, final Object[] dataOrBeforeArray, final String updateType) throws AdapterException, MetaDataRepositoryException {
        Field[] fieldsOfThisTable = null;
        List<String> keys = null;
        if (event.typeUUID != null) {
            if (this.typeUUIDCache.containsKey(event.typeUUID) && this.typeUUIDKeyCache.containsKey(event.typeUUID)) {
                fieldsOfThisTable = this.typeUUIDCache.get(event.typeUUID);
                keys = this.typeUUIDKeyCache.get(event.typeUUID);
            }
            else {
                try {
                    final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(event.typeUUID, WASecurityManager.TOKEN);
                    keys = (List<String>)dataType.keyFields;
                    this.typeUUIDKeyCache.put(event.typeUUID, keys);
                    final Class<?> typeClass = (Class<?>)HDLoader.get().loadClass(dataType.className);
                    fieldsOfThisTable = typeClass.getDeclaredFields();
                    this.typeUUIDCache.put(event.typeUUID, fieldsOfThisTable);
                }
                catch (Exception e) {
                    KuduWriter.logger.warn((Object)("Unable to fetch the type for table " + event.metadata.get("TableName") + e));
                    throw new AdapterException("Unable to fetch the type for table " + event.metadata.get("TableName"), (Throwable)e);
                }
            }
        }
        final Map<String, Object> columnsAndDataMap = new LinkedHashMap<String, Object>();
        for (Integer i = 0; i < dataOrBeforeArray.length; ++i) {
            final boolean isPresent = BuiltInFunc.IS_PRESENT(event, dataOrBeforeArray, (int)i);
            if (!isPresent) {
                KuduWriter.logger.warn((Object)"Please provide the full image for insertion");
                throw new AdapterException("Please provide the full image for insertion as PKUpdateHandlingMode is set as DELETE and Insert");
            }
            if (fieldsOfThisTable != null) {
                columnsAndDataMap.put(fieldsOfThisTable[i].getName(), (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
            }
            else {
                columnsAndDataMap.put(i + "", (dataOrBeforeArray[i] != null) ? dataOrBeforeArray[i] : null);
            }
        }
        return new EventDataObject((Map)columnsAndDataMap, event.metadata.get("OperationName").toString(), (List)keys, this.currentReceivedPosition, fieldsOfThisTable, event.sourceUUID, event.typeUUID, event.metadata.get("TableName").toString(), event.data, (Map)event.metadata, (Map)event.userdata, event.before, event.beforePresenceBitMap, event.dataPresenceBitMap);
    }
    
    protected synchronized void flushAll() throws Exception {
        List<Position> positions = new ArrayList<Position>();
        synchronized (this.targetDataMap) {
            for (final String table : this.targetDataMap.keySet()) {
                if (this.targetDataMap.get(table).size() > 0) {
                    positions = (List<Position>)this.writerProps.getWriterStrategy().write(table, (List)new ArrayList(this.targetDataMap.get(table)));
                    this.targetDataMap.get(table).clear();
                    for (final Position pos : positions) {
                        this.writerProps.getCheckpointerStrategy().updatePosition(pos);
                    }
                    ((CheckpointTableImpl)this.writerProps.getCheckpointerStrategy()).updatePositionToDb();
                    if (!KuduWriter.logger.isDebugEnabled()) {
                        continue;
                    }
                    KuduWriter.logger.debug((Object)("Flushed : " + table + "."));
                }
            }
        }
        if (positions.size() > 0) {
            final Position pos2 = this.writerProps.getCheckpointerStrategy().getAckPosition();
            this.receiptCallback.ack((int)(Object)new Long(this.eventCounter), pos2);
            if (this.isRecoveryEnabled) {
                final StringBuilder positionString = new StringBuilder();
                final Collection<Path> paths = (Collection<Path>)pos2.values();
                for (final Path path : paths) {
                    if (positionString.length() != 0) {
                        positionString.append("|");
                    }
                    positionString.append(path.getLowSourcePosition().toString());
                }
                this.connectionObject.setAcknowledgedCheckpoint(positionString.toString());
            }
            else {
                this.connectionObject.setAcknowledgedCheckpoint("N/A");
            }
            if (KuduWriter.logger.isDebugEnabled()) {
                KuduWriter.logger.debug((Object)("Checkpointed position : " + pos2 + "."));
            }
        }
        this.eventCounter = 0L;
    }
    
    public void onDrop(final MetaInfo.MetaObject metaObject) throws Exception {
        final MetaInfo.Target targetMetaObj = (MetaInfo.Target)metaObject;
        final Map<String, Object> tmpmap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        tmpmap.putAll(targetMetaObj.properties);
        final Property tempProp = new Property((Map)tmpmap);
        tmpmap.put("TargetUUID", targetMetaObj.uuid);
        final String targetIdentifier = this.getTargetIdentifier((Map)tmpmap);
        this.initializeKuduClient(tmpmap.get("KuduClientConfig").toString());
        final String chkPointTableName = tempProp.getString("CheckPointTable", "CHKPOINT");
        (this.check = new CheckpointTableImpl(chkPointTableName, targetIdentifier, this.connectionObject)).deletePositionFromDb();
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        if (this.connectionObject != null) {
            events.add(MonitorEvent.Type.EXTERNAL_IO_LATENCY, this.connectionObject.getLatency());
            events.add(MonitorEvent.Type.TOTAL_EVENTS_IN_LAST_IO, this.connectionObject.totalEventCount());
            events.add(MonitorEvent.Type.OPERATION_METRICS, this.connectionObject.getOperationMetrics());
            events.add(MonitorEvent.Type.LAST_IO_TIME, this.connectionObject.getLastIOTime());
            events.add(MonitorEvent.Type.TARGET_COMMIT_POSITION, this.connectionObject.getAcknowledgedCheckpoint());
        }
    }
    
    public void close() throws Exception {
        if (this.writerProps.getBatchInterval() > 0L) {
            this.stopBatchTimer();
        }
        if (KuduWriter.logger.isDebugEnabled()) {
            KuduWriter.logger.debug((Object)"Clearing all data before close...");
        }
        this.targetDataMap.clear();
        this.writerProps.getSourceTargetMap().clear();
        try {
            this.closeWriter();
        }
        catch (Exception e) {
            KuduWriter.logger.warn((Object)("error while closing the adapter " + e));
        }
    }
    
    static {
        KuduWriter.logger = Logger.getLogger((Class)KuduWriter.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
