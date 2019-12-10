package com.datasphere.proc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.datasphere.appmanager.DisapproveQuiesceException;
import com.datasphere.classloading.HDLoader;
import com.datasphere.drop.DropMetaObject;
import com.datasphere.event.Event;
import com.datasphere.intf.SourceMetadataProvider;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.records.Record;
import com.datasphere.proc.monitor.objects.CDCMonitorEvent;
import com.datasphere.proc.monitor.objects.CDCType;
import com.datasphere.proc.monitor.objects.MonView;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.TypeGenerator;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.compiler.stmts.CreateTypeStmt;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.security.HSecurityManager;
import com.datasphere.usagemetrics.MetricsCollector;
import com.datasphere.usagemetrics.SourceMetricsCollector;
import com.datasphere.utility.CDCTablesProperty;
import com.datasphere.utility.ParserUtility;
import com.datasphere.uuid.UUID;

public abstract class SourceProcess extends BaseProcess
{
    private static final Logger logger;
    protected UUID sourceUUID;
    protected String distributionID;
    protected SourceMetricsCollector metricsCollector;
    protected Flow ownerFlow;
    protected Map<String, UUID> typeUUIDForCDCTables;
    private boolean selectFlag;
    private CDCMonitorEvent cdcMonEvent;
    private ObjectMapper mapper;
    private boolean isCDC;
    protected static final String CONNECTION_URL_PROP = "ConnectionURL";
    protected static final String DBNAME_PROP = "DatabaseName";
    CDCREADER readerType;
    protected final String TABLES = "Tables";
    protected final String TABLESEXTENDEDPROPERTY = "TABLESEXTENDEDPROPERTY";
    
    public SourceProcess() {
        this.sourceUUID = null;
        this.metricsCollector = new MetricsCollector();
        this.selectFlag = false;
        this.cdcMonEvent = new CDCMonitorEvent();
        this.mapper = new ObjectMapper();
        this.isCDC = false;
    }
    
    @Override
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow) throws Exception {
        super.init(properties, properties2, sourceUUID, distributionID, restartPosition, sendPositions, flow);
        this.sourceUUID = sourceUUID;
        this.distributionID = distributionID;
        if (properties.containsKey("adapterName")) {
            try {
                this.readerType = CDCREADER.valueOf(properties.get("adapterName").toString());
                this.isCDC = true;
            }
            catch (IllegalArgumentException ex) {
                this.isCDC = false;
            }
        }
        if (this.isCDC) {
            try {
                String connectionURL = properties.get("ConnectionURL").toString();
                if (!connectionURL.startsWith("jdbc:")) {
                    switch (this.readerType) {
                        case DatabaseReader: {}
                        case HPNonStopEnscribeReader: {}
                        case HPNonStopSQLMPReader: {}
                        case MSSqlReader: {
                            connectionURL = "jdbc:sqlserver://" + connectionURL;
                            properties.put("ConnectionURL", connectionURL);
                            break;
                        }
                        case MysqlReader: {
                            URI u;
                            try {
                                u = new URI(connectionURL);
                            }
                            catch (URISyntaxException ex2) {
                                throw new IllegalArgumentException("Cannot parse host and port no expecting connectionURL like mysql://<hostname>:<port> or provide jdbc connection URL");
                            }
                            connectionURL = "jdbc:mysql://" + u.getHost() + ":" + u.getPort();
                            properties.put("ConnectionURL", connectionURL);
                            break;
                        }
                        case OracleReader: {
                            connectionURL = "jdbc:oracle:thin:@" + connectionURL;
                            properties.put("ConnectionURL", connectionURL);
                        }
                    }
                    if (SourceProcess.logger.isInfoEnabled()) {
                        SourceProcess.logger.info((Object)("CDCReader: " + this.readerType.toString() + " connectionURL is transformed to standard JDBC URL: " + connectionURL));
                    }
                }
            }
            catch (NullPointerException ex3) {}
        }
        if (properties.get("Tables") != null) {
            final String tableString = properties.get("Tables").toString();
            if (tableString.matches(".*(?i)keycolumns.*") || tableString.matches(".*(?i)fetchcols.*") || tableString.matches(".*(?i)replacebadcolumns.*")) {
                final CDCTablesProperty tableProperty = ParserUtility.parseCDCTablesProperty(tableString);
                properties.put("TABLESEXTENDEDPROPERTY", tableProperty);
                final StringBuilder builder = new StringBuilder();
                for (final String tablenames : tableProperty.keySet()) {
                    builder.append(tablenames + ";");
                }
                properties.put("Tables", builder.toString());
            }
        }
    }
    
    @Override
    public void init(final Map<String, Object> properties, final Map<String, Object> parserProperties, final UUID uuid, final String distributionID) throws Exception {
        properties.put("UUID", uuid);
        super.init(properties, parserProperties);
        this.sourceUUID = uuid;
        this.distributionID = distributionID;
    }
    
    public void init(final Map<String, Object> properties, final Map<String, Object> properties2, final UUID sourceUUID, final String distributionID, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow, final List<UUID> servers) throws Exception {
    }
    
    public synchronized void initializeTypeSystem(final Map<String, Object> properties, final Map<String, Object> parserProperties, final UUID uuid, final String distributionID) throws Exception {
        final String mode = (String)properties.get("SessionType");
        if ((mode == null || !mode.equalsIgnoreCase("METADATA")) && this instanceof SourceMetadataProvider) {
            final Map<String, TypeDefOrName> metaMap = ((SourceMetadataProvider)this).getMetadata();
            if (SourceProcess.logger.isInfoEnabled()) {
                SourceProcess.logger.info((Object)("Trying to create DSS type(s) from table definition " + ((metaMap != null) ? metaMap.toString() : null)));
            }
            final long startTime = System.currentTimeMillis();
            this.createTypesFromTableDef(metaMap);
            final long endTime = System.currentTimeMillis();
            if (SourceProcess.logger.isInfoEnabled()) {
                SourceProcess.logger.info((Object)("Time taken for creating DSS type(s) from table definiton  is " + (endTime - startTime) / 1000L + " seconds"));
            }
        }
    }
    
    public void onUndeploy() throws Exception {
        if (this.typeUUIDForCDCTables != null) {
            final MetaInfo.Source sourceInfo = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, HSecurityManager.TOKEN);
            final Context ctx = Context.createContext(HSecurityManager.TOKEN);
            for (final Map.Entry<String, UUID> entry : this.typeUUIDForCDCTables.entrySet()) {
                final MetaInfo.Type typeObject = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(entry.getValue(), HSecurityManager.TOKEN);
                final List<String> result = DropMetaObject.DropType.drop(ctx, typeObject, DropMetaObject.DropRule.NONE, HSecurityManager.TOKEN);
                if (SourceProcess.logger.isDebugEnabled()) {
                    SourceProcess.logger.debug((Object)result.toString());
                }
            }
            this.typeUUIDForCDCTables = null;
        }
    }
    
    public void onDeploy() throws Exception {
    }
    
    public void updateMonitor(final Event e, final Position pos) {
        if (!this.isCDC) {
            return;
        }
        Record evt;
        try {
            evt = (Record)e;
        }
        catch (ClassCastException ex) {
            return;
        }
        String opname = "";
        if (evt.metadata.containsKey("OperationName")) {
            opname = evt.metadata.get("OperationName").toString();
            final String upperCase = opname.toUpperCase();
            switch (upperCase) {
                case "INSERT": {
                    this.cdcMonEvent.increment(CDCType.INSERT);
                    break;
                }
                case "UPDATE": {
                    this.cdcMonEvent.increment(CDCType.UPDATE);
                    break;
                }
                case "DELETE": {
                    this.cdcMonEvent.increment(CDCType.DELETE);
                    break;
                }
                case "SELECT": {
                    this.selectFlag = true;
                    this.cdcMonEvent.increment(CDCType.SELECT);
                    break;
                }
                default: {
                    if (evt.metadata.containsKey("OperationType") && evt.metadata.get("OperationType").toString().startsWith("DDL")) {
                        this.cdcMonEvent.increment(CDCType.DDL);
                        break;
                    }
                    break;
                }
            }
            if (evt.metadata.containsKey("TimeStamp")) {
                final Object val = evt.metadata.get("TimeStamp");
                if (val != null && !val.toString().trim().isEmpty()) {
                    long commitTimeStamp = 0L;
                    try {
                        final DateTime time = (DateTime)val;
                        commitTimeStamp = time.getMillis();
                    }
                    catch (ClassCastException exc) {
                        commitTimeStamp = Long.parseLong(val.toString());
                    }
                    this.cdcMonEvent.setReadLag(System.currentTimeMillis() - commitTimeStamp + " ms");
                }
            }
            if (pos != null) {
                this.cdcMonEvent.setLastCheckPoint(pos.toString());
            }
            return;
        }
        SourceProcess.logger.info((Object)"Couldnot monitor since OperationName donot exist in events metadata ");
    }
    
    @Override
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        if (!this.isCDC) {
            return;
        }
        if (SourceProcess.logger.isDebugEnabled()) {
            SourceProcess.logger.debug((Object)("Updating monitor : " + this.cdcMonEvent));
        }
        if (!this.selectFlag) {
            events.add(MonitorEvent.Type.LAST_CHECKPOINT_POSITION, this.cdcMonEvent.getLastCheckPoint());
            try {
                events.add(MonitorEvent.Type.CDC_OPERATION, this.mapper.writerWithDefaultPrettyPrinter().withView((Class)MonView.CDC.class).writeValueAsString((Object)this.cdcMonEvent));
            }
            catch (IOException e) {
                SourceProcess.logger.warn((Object)("Cannot update monitor for DMLOperations because " + e.getMessage()));
            }
            events.add(MonitorEvent.Type.READ_LAG, this.cdcMonEvent.getReadLag());
        }
        else {
            events.add(MonitorEvent.Type.TOTAL_SELECTS, Long.valueOf(this.cdcMonEvent.getNoOfSelects()));
        }
    }
    
    @Override
    public void send(final Event event, final int channel, final Position pos) throws Exception {
        super.send(event, channel, pos);
        this.updateMonitor(event, pos);
    }
    
    public Position getCheckpoint() {
        return null;
    }
    
    public Flow getOwnerFlow() {
        return this.ownerFlow;
    }
    
    public void setOwnerFlow(final Flow ownerFlow) {
        this.ownerFlow = ownerFlow;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        this.metricsCollector.flush();
    }
    
    public void setSourcePosition(final SourcePosition sourcePosition) {
    }
    
    public boolean requiresPartitionedSourcePosition() {
        return false;
    }
    
    public boolean isAdapterShardable() {
        return false;
    }
    
    public void createTypesFromTableDef(final Map<String, TypeDefOrName> metaMap) throws Exception {
        final HDLoader loader = HDLoader.get();
        if (metaMap != null) {
            final MetaInfo.Source sourceInfo = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, HSecurityManager.TOKEN);
            Context ctx = Context.createContext(HSecurityManager.TOKEN);
            this.typeUUIDForCDCTables = new HashMap<String, UUID>();
            for (final Map.Entry<String, TypeDefOrName> entry : metaMap.entrySet()) {
                final String modifiedTableName = TypeGenerator.getValidJavaIdentifierName(entry.getKey());
                final String typeName = TypeGenerator.getTypeName(sourceInfo.nsName, sourceInfo.name, entry.getKey());
                loader.lockClass(typeName);
                try {
                    final TypeDefOrName typeDef = entry.getValue();
                    final String typeNameWithOutNameSpace = typeName.substring(typeName.indexOf(46) + 1);
                    final MetaInfo.Type dataType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.TYPE, sourceInfo.nsName, typeNameWithOutNameSpace, null, HSecurityManager.TOKEN);
                    if (dataType != null) {
                        if (!modifiedTableName.equals(entry.getKey()) && metaMap.containsKey(modifiedTableName)) {
                            SourceProcess.logger.warn((Object)("Type " + typeName + " already exists. Cannot create type for table " + entry.getKey() + "(Modified table name after replacing special character(s) is " + modifiedTableName + ")."));
                        }
                        else {
                            this.typeUUIDForCDCTables.put(entry.getKey(), dataType.uuid);
                            if (!SourceProcess.logger.isDebugEnabled()) {
                                continue;
                            }
                            SourceProcess.logger.debug((Object)("Type " + dataType.name + " already exists. Skipping type creation for table " + entry.getKey()));
                        }
                    }
                    else {
                        final CreateTypeStmt ctStmt = new CreateTypeStmt(typeName, Boolean.valueOf(true), typeDef);
                        final CallBackExecutor cb = new CallBackExecutor();
                        Compiler.compile(ctStmt, ctx, cb);
                        this.typeUUIDForCDCTables.put(entry.getKey(), cb.uuid);
                    }
                }
                catch (Throwable e) {
                    String sourceName = null;
                    if (sourceInfo != null) {
                        sourceName = sourceInfo.getFullName();
                    }
                    if (sourceName == null) {
                        sourceName = this.sourceUUID.getUUIDString();
                    }
                    SourceProcess.logger.warn((Object)("Type creation failed for SourceProcess: " + sourceName + ", reason: " + e.getMessage()), e);
                }
                finally {
                    loader.unlockClass(typeName);
                }
            }
            ctx = null;
        }
    }
    
    public UUID getTypeUUIDForCDCTables(final String table) {
        return this.typeUUIDForCDCTables.get(table);
    }
    
    public static UUID createTypeForCDCTable(final TypeDefOrName typeDef) throws Exception {
        final Context ctx = Context.createContext(HSecurityManager.TOKEN);
        final CreateTypeStmt ctStmt = new CreateTypeStmt(typeDef.typeName, Boolean.valueOf(true), typeDef);
        final CallBackExecutor cb = new CallBackExecutor();
        Compiler.compile(ctStmt, ctx, cb);
        return cb.uuid;
    }
    
    public boolean approveQuiesce() throws DisapproveQuiesceException {
        return true;
    }
    
    static {
        logger = Logger.getLogger((Class)SourceProcess.class);
    }
    
    enum CDCREADER
    {
        OracleReader, 
        MSSqlReader, 
        MysqlReader, 
        DatabaseReader, 
        SalesForceReader, 
        HPNonStopSQLMXReader, 
        HPNonStopSQLMPReader, 
        HPNonStopEnscribeReader;
    }
}
