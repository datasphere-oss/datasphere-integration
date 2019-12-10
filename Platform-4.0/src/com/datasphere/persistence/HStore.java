package com.datasphere.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.persistence.spi.PersistenceUnitTransactionType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.datasphere.cache.CacheConfiguration;
import com.datasphere.cache.CacheManager;
import com.datasphere.cache.CachingProvider;
import com.datasphere.cache.Filter;
import com.datasphere.cache.ICache;
import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.HDLoader;
import com.datasphere.distribution.HIndex;
import com.datasphere.distribution.HQuery;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.event.SimpleEvent;
import com.datasphere.gen.RTMappingGenerator;
import com.datasphere.intf.PersistenceLayer;
import com.datasphere.intf.PersistencePolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.PartitionManager;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.ReportStats;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.channels.SimpleChannel;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.PubSub;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorCollector;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.runtime.utils.FieldToObject;
import com.datasphere.security.Password;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HD;
import com.datasphere.hd.HDContext;
import com.datasphere.hd.HDKey;
import com.datasphere.hd.HDListener;
import com.datasphere.hdstore.CheckpointManager;
import com.datasphere.hdstore.DataType;
import com.datasphere.hdstore.Type;
import com.datasphere.hdstore.Utility;
import com.datasphere.hdstore.HD;
import com.datasphere.hdstore.HDStore;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.HDStores;
import com.datasphere.hdstore.exceptions.HDStoreException;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;

public class HStore extends FlowComponent implements HDListener, PubSub
{
    private CacheManager manager;
    private static Logger logger;
    public static final String MAX_PERSIST_QUEUE_SIZE = "MAX_PERSIST_QUEUE_SIZE";
    public static final String BATCH_WRITING_SIZE = "BATCH_WRITING_SIZE";
    public static final int DEFAULT_BATCH_WRITING_SIZE = 10000;
    public static final String MAX_THREAD_NUM = "MAX_THREAD_NUM";
    public static final int DEFAULT_MAX_THREAD_NUM = 10;
    public static final String DISABLE_MEMORY_CACHE = "DISABLE_MEMORY_CACHE";
    private static final ConcurrentMap<String, HStore> stores;
    private String pu_name;
    private String fullName;
    private String fullTableName;
    private String eventTableName;
    private String hdContextClassName;
    private Class<?> hdContextClass;
    private String hdClassName;
    private Class<?> hdClass;
    ICache<HDKey, HD> hds;
    ICache<Object, HDContext> latestContexts;
    private PersistenceLayer persistenceLayer;
    private PersistencePolicy persistencePolicy;
    private static LRUList<LRUNode> expungeableHDs;
    private static MemoryMonitor memoryMonitor;
    public MetaInfo.Type contextBeanDef;
    private final MetaInfo.HDStore storeMetaInfo;
    private List<Pair<String, FieldFactory>> ctxKeyFacs;
    private List<Pair<String, FieldFactory>> ctxFieldFacs;
    private List<MetaInfo.Type> eventBeanDefs;
    private List<Class<?>> eventBeanClasses;
    private boolean persistenceAvailable;
    private ExecutorService executor;
    private boolean isNoSQL;
    private TARGETDATABASE targetDbType;
    private volatile long created;
    private PathManager waitPosition;
    private final Channel output;
    private boolean evictHDs;
    private boolean isRecoveryEnabled;
    public boolean isCrashed;
    public Exception crashCausingException;
    private boolean relaxSchemaCheck;
    private HDStore persistedHDStore;
    private DataType persistedHDType;
    private final Set<String> contextAttributes;
    private final Map<String, Set<String>> eventAttributes;
    private static final JsonNode hdStoreMetadata;
    private static final Map<String, HDDataType> javaTypeToHDType;
    private static final String MSG_EXC_DATA_TYPE_CREATE_FAIL = "Unable to create data type '%s' in HDStore '%s'";
    private static final String MSG_INF_CONTEXT_TYPE_FOUND = "Have context type '%s' for HDStore '%s'";
    private static final String MSG_WRN_JAVA_TYPE_UNSUPPORTED = "Unsupported Java data type, '%s' for attribute '%s' in type '%s'";
    private static final String MSG_WRN_ES_SCHEMA_UNSUPPORTED = "Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s";
    private static final String MSG_WRN_HDSTORE_DROP_FAILED = "Failed to drop HDStore '%s'";
    private final Object elasticsearchCheckpointLock;
    Long prevCreated;
    Long prevCreatedRate;
    LagMarker lastLagObserved;
    ConcurrentHashMap<String, LagMarker> allPaths;
    Boolean lagReportEnabled;
    static boolean reachedMaxMemory;
    static ReentrantLock expungeablesLock;
    static Condition expungeablesExist;
    static volatile boolean stopEviction;
    
    public static void remove(final UUID uuid) {
        final HStore store = HStore.stores.remove(uuid.getUUIDString());
        if (store != null) {
            store.hds = null;
        }
    }
    
    public static HStore get(final UUID uuid, final BaseServer srv) throws Exception {
        final MDRepository metaDataRepository = MetadataRepository.getINSTANCE();
        HStore store;
        synchronized (HStore.stores) {
            store = HStore.stores.get(uuid.getUUIDString());
            if (store == null) {
                final MetaInfo.HDStore wsMetaInfo = (MetaInfo.HDStore)metaDataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                if (wsMetaInfo == null) {
                    return null;
                }
                store = new HStore(wsMetaInfo, srv);
                if (srv != null) {
                    srv.putOpenObject(store);
                }
                store.init();
                HStore.stores.put(uuid.getUUIDString(), store);
            }
        }
        return store;
    }
    
    public static void drop(final MetaInfo.HDStore hdStore) {
        HStore.logger.info(("Dropping hdstore " + hdStore.getName()));
        final Map<String, Object> properties = hdStore.getProperties();
        if (usesNewHDStore(properties)) {
            final String hdStoreName = hdStore.getFullName();
            try {
                final HDStoreManager manager = HDStores.getInstance(properties);
                if (!manager.removeUsingAlias(hdStoreName)) {
                    HStore.logger.warn(String.format("Failed to drop HDStore '%s'", hdStoreName));
                }
            }
            catch (HDStoreException e) {
                HStore.logger.warn(e.getMessage());
            }
        }
    }
    
    @Override
    public void addSessionToReport(final AuthToken token) {
        final ReportStats.HDstoreReportStats stats = new ReportStats.HDstoreReportStats(this.getMetaInfo());
        this.compStats.put(token, stats);
    }
    
    @Override
    public ReportStats.BaseReportStats removeSessionToReport(final AuthToken token, final boolean computeRetVal) {
        final ReportStats.HDstoreReportStats stats = (ReportStats.HDstoreReportStats)this.compStats.remove(token);
        if (stats == null || !computeRetVal) {
            return null;
        }
        if (stats.getLastHD() != null) {
            stats.setLastHDStr(stats.getLastHD().toJSON());
            stats.setLastHD(null);
        }
        return stats;
    }
    
    public void updateSessionStats(final HD batch) {
        for (final ReportStats.BaseReportStats stats : this.compStats.values()) {
            final ReportStats.HDstoreReportStats wsStats = (ReportStats.HDstoreReportStats)stats;
            final long eventTS = System.currentTimeMillis();
            final long hdsseen = wsStats.getHDsSeen();
            wsStats.setHDsSeen(hdsseen + 1L);
            wsStats.setLasthdTS(eventTS);
            wsStats.setLastHD(batch);
        }
    }
    
    private DataType getPersistentDataType(final Map<String, Object> properties) throws ElasticSearchSchemaException {
        final String hdStoreName = this.fullName;
        final String contextTypeName = this.contextBeanDef.nsName + ':' + this.contextBeanDef.name;
        final JsonNode hdStoreSchema = this.createHDStoreSchema(properties);
        final DataType result = getOrCreateHDStoreType(this.persistedHDStore, contextTypeName, hdStoreSchema, this);
        HStore.logger.info(String.format("Have context type '%s' for HDStore '%s'", contextTypeName, hdStoreName));
        return result;
    }
    
    private static HDStore getOrCreateHDStore(final String hdStoreName, final Map<String, Object> properties) {
        final HDStoreManager manager = HDStores.getInstance(properties);
        return manager.getOrCreate(hdStoreName, properties);
    }
    
    private static DataType getOrCreateHDStoreType(final HDStore hdStore, final String dataTypeName, final JsonNode dataTypeSchema, final HStore ws) {
        final DataType contextType = hdStore.setDataType(dataTypeName, dataTypeSchema, ws);
        if (contextType == null) {
            final String hdStoreName = hdStore.getName();
            throw new HDStoreException(String.format("Unable to create data type '%s' in HDStore '%s'", dataTypeName, hdStoreName));
        }
        return contextType;
    }
    
    private JsonNode createHDStoreSchema(final Map<String, Object> properties) throws ElasticSearchSchemaException {
        final ObjectNode result = Utility.objectMapper.createObjectNode();
        final ArrayNode contextItems = this.createDataTypeSchema(this.contextBeanDef, this.contextAttributes, properties);
        final JsonNode metadataItems = this.getMetadataArray();
        if (contextItems != null) {
            contextItems.add(makeFieldNode("$id", "string"));
            contextItems.add(makeFieldNode("$timestamp", "datetime"));
            contextItems.add(makeFieldNode("$node_name", "java.lang.String"));
            contextItems.add(makeFieldNode("$checkpoint", "java.lang.Long"));
            result.set("context", (JsonNode)contextItems);
        }
        result.set("metadata", metadataItems);
        final ArrayNode eventTypes = this.getEventTypes(this.eventBeanDefs, this.eventAttributes, properties);
        if (eventTypes != null) {
            result.set("events", (JsonNode)eventTypes);
        }
        return (JsonNode)result;
    }
    
    private JsonNode getMetadataArray() {
        return HStore.hdStoreMetadata;
    }
    
    private ArrayNode getEventTypes(final List<MetaInfo.Type> eventTypes, final Map<String, Set<String>> eventAttributes, final Map<String, Object> properties) throws ElasticSearchSchemaException {
        ArrayNode result = null;
        if (eventTypes != null && !eventTypes.isEmpty()) {
            result = Utility.objectMapper.createArrayNode();
            for (final MetaInfo.Type eventType : eventTypes) {
                final ObjectNode eventSchema = Utility.objectMapper.createObjectNode();
                final String eventTypeName = eventType.getFullName();
                final String eventTypeSchemaName = eventType.nsName + ':' + eventType.name;
                final Set<String> attributes = new HashSet<String>();
                final ArrayNode eventTypeSchema = this.createDataTypeSchema(eventType, attributes, properties);
                eventAttributes.put(eventTypeName, attributes);
                eventSchema.put("name", eventTypeSchemaName);
                eventSchema.set("type", (JsonNode)eventTypeSchema);
                result.add((JsonNode)eventSchema);
            }
        }
        return result;
    }
    
    private ArrayNode createDataTypeSchema(final MetaInfo.Type type, final Set<String> attributes, final Map<String, Object> properties) throws ElasticSearchSchemaException {
        final ArrayNode dataTypeSchema = Utility.objectMapper.createArrayNode();
        for (final Map.Entry<String, String> field : type.fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            final JsonNode fieldNode = makeFieldNode(fieldName, fieldType);
            if (fieldNode != null) {
                dataTypeSchema.add(fieldNode);
                attributes.add(fieldName);
            }
            else {
                if (properties == null || !properties.containsKey("elasticsearch.relax_schema")) {
                    final String typeName = type.getFullName();
                    final String message = "You might want to relax the schema by setting the elasticsearch.relax_schema to true while creating hd store. This will insert the data into elastic search, but your query through DSS client might fail";
                    HStore.logger.error(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s", fieldType, fieldName, typeName, message));
                    throw new ElasticSearchSchemaException(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s", fieldType, fieldName, typeName, message));
                }
                final String value = (String)properties.get("elasticsearch.relax_schema");
                if (value.equalsIgnoreCase("true")) {
                    this.relaxSchemaCheck = true;
                }
                else {
                    if (value.equalsIgnoreCase("false")) {
                        final String typeName2 = type.getFullName();
                        HStore.logger.error(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'", fieldType, fieldName, typeName2));
                        throw new ElasticSearchSchemaException(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'", fieldType, fieldName, typeName2));
                    }
                    continue;
                }
            }
        }
        return dataTypeSchema;
    }
    
    private static JsonNode makeFieldNode(final String name, final String javaTypeName) {
        ObjectNode result = null;
        final HDDataType hdDataType = HStore.javaTypeToHDType.get(javaTypeName);
        if (hdDataType != null) {
            result = Utility.objectMapper.createObjectNode();
            result.put("name", name);
            result.put("type", hdDataType.typeName);
            result.put("nullable", hdDataType.nullable);
        }
        return (JsonNode)result;
    }
    
    private HD addToHDStore(final HD hd) {
        if (this.persistedHDType != null) {
            final HDStoreManager manager = this.persistedHDStore.getManager();
            final HD newHD = this.convertOldHDToNew(hd);
            synchronized (this.elasticsearchCheckpointLock) {
                if (this.isRecoveryEnabled) {
                    final CheckpointManager checkpointManager = manager.getCheckpointManager();
                    synchronized (checkpointManager) {
                        checkpointManager.add(this.fullName, newHD, hd.getPosition());
                    }
                }
                this.persistedHDType.queue(newHD);
            }
            return newHD;
        }
        return null;
    }
    
    private HD convertOldHDToNew(final HD hd) {
        final ObjectNode hdJson = (ObjectNode)Utility.objectMapper.valueToTree(hd);
        if (!this.relaxSchemaCheck) {
            hdJson.retain((Collection)this.contextAttributes);
        }
        else {
            final String[] fields = { "_id", "id", "internalHDStoreName", "uuid", "hdTs", "key", "mapKey", "eventsNotAccessedDirectly", "hdStatus", "position", "notificationFlag" };
            final List<String> fieldsToRemove = Arrays.asList(fields);
            hdJson.remove((Collection)fieldsToRemove);
        }
        this.setMissingValuesToNull(this.contextAttributes, hdJson);
        final HD hd = new HD();
        hd.setAll(hdJson);
        hd.put("$id", hd.id);
        hd.put("$timestamp", hd.hdTs);
        this.addNewHDEvents(hd, hd.getEvents());
        return hd;
    }
    
    private void addNewHDEvents(final HD hd, final List<SimpleEvent> events) {
        if (events != null && !events.isEmpty()) {
            for (final SimpleEvent event : events) {
                this.addNewHDEvent(hd, event);
            }
        }
    }
    
    private void addNewHDEvent(final HD hd, final SimpleEvent event) {
        final String eventClassName = event.getClass().getName();
        final MetaInfo.Type eventBeanDef = this.getEventBeanDef(eventClassName);
        if (eventBeanDef != null) {
            final String eventTypeName = eventBeanDef.getFullName();
            final ObjectNode eventJson = (ObjectNode)Utility.objectMapper.valueToTree(event);
            final Set<String> attributes = this.eventAttributes.get(eventTypeName);
            eventJson.retain((Collection)attributes);
            this.setMissingValuesToNull(attributes, eventJson);
            final ArrayNode eventList = getEventList(hd, eventBeanDef);
            eventList.add((JsonNode)eventJson);
        }
    }
    
    private void setMissingValuesToNull(final Set<String> attributes, final ObjectNode eventJson) {
        for (final String attribute : attributes) {
            if (!eventJson.has(attribute)) {
                eventJson.putNull(attribute);
            }
        }
    }
    
    private MetaInfo.Type getEventBeanDef(final String eventClassName) {
        MetaInfo.Type result = null;
        for (final MetaInfo.Type eventBeanDef : this.eventBeanDefs) {
            if (eventClassName.equals(eventBeanDef.className)) {
                result = eventBeanDef;
                break;
            }
        }
        return result;
    }
    
    private static ArrayNode getEventList(final HD hd, final MetaInfo.Type eventBeanDef) {
        final String eventTypeName = eventBeanDef.nsName + ':' + eventBeanDef.name;
        ArrayNode eventList = (ArrayNode)hd.get(eventTypeName);
        if (eventList == null) {
            eventList = Utility.objectMapper.createArrayNode();
            hd.set(eventTypeName, (JsonNode)eventList);
        }
        return eventList;
    }
    
    public static HStore get(final String fullName) throws Exception {
        final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
        final String namespace = fullName.split("\\.")[0];
        final String name = fullName.split("\\.")[1];
        final MetaInfo.HDStore wsInfo = (MetaInfo.HDStore)metadataRepository.getMetaObjectByName(EntityType.HDSTORE, namespace, name, null, HSecurityManager.TOKEN);
        if (wsInfo != null) {
            return get(wsInfo.uuid, null);
        }
        throw new RuntimeException("HD store " + fullName + " does not exist, please ensure the application name is included as appname.storename");
    }
    
    public boolean usesInMemoryHDStore() {
        if (this.storeMetaInfo.properties.containsKey("DISABLE_MEMORY_CACHE")) {
            return "true".equalsIgnoreCase(this.storeMetaInfo.properties.get("DISABLE_MEMORY_CACHE").toString());
        }
        final Type type = this.storeMetaInfo.hdstoretype;
        return type == Type.IN_MEMORY || type == Type.INTERVAL;
    }
    
    public boolean usesOldHDStore() {
        return this.storeMetaInfo.usesIntervalBasedHDStore();
    }
    
    public boolean usesNewHDStore() {
        final Type type = this.storeMetaInfo.hdstoretype;
        return type == Type.STANDARD;
    }
    
    public static boolean usesNewHDStore(final Map<String, Object> properties) {
        final Type type = Type.getType(properties, null);
        return type == Type.STANDARD;
    }
    
    private Map<String, Object> extractDBProps(final Map<String, Object> storeProperties) {
        if (HStore.logger.isInfoEnabled()) {
            HStore.logger.info(("properties for HDStore persistence :\n" + storeProperties));
        }
        final Map<String, Object> storePropCopy = new HashMap<String, Object>();
        for (final String key : storeProperties.keySet()) {
            storePropCopy.put(key.toUpperCase(), storeProperties.get(key));
        }
        final Map<String, Object> props = new HashMap<String, Object>();
        props.putAll(storePropCopy);
        props.put("javax.persistence.transactionType", PersistenceUnitTransactionType.RESOURCE_LOCAL.name());
        props.put("eclipselink.classloader", HDLoader.getDelegate());
        props.put("eclipselink.logging.level", (storePropCopy.get("LOGGING_LEVEL") == null) ? "SEVERE" : storePropCopy.get("LOGGING_LEVEL"));
        props.put("eclipselink.weaving.changetracking", "false");
        props.put("eclipselink.weaving", "false");
        props.put("eclipselink.cache.shared.default", "false");
        props.put("eclipselink.cache.shared.", "false");
        props.put("eclipselink.cache.type.default", "NONE");
        props.put("eclipselink.cache.type.", "NONE");
        props.put("eclipselink.persistence-context.reference-mode", "WEAK");
        final String MONGODB = "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform";
        final String ONDB = "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform";
        if (props.get("CONTEXT_TABLE") == null || ((String)props.get("CONTEXT_TABLE")).isEmpty()) {
            if (HStore.logger.isDebugEnabled()) {
                HStore.logger.debug(("No context tablename provided by user. Using system generated tablename:" + this.fullTableName));
            }
        }
        else {
            this.fullTableName = (String)props.get("CONTEXT_TABLE");
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info(("Using context tablename:" + this.fullTableName));
            }
        }
        if (props.get("EVENT_TABLE") == null || ((String)props.get("EVENT_TABLE")).isEmpty()) {
            if (HStore.logger.isDebugEnabled()) {
                HStore.logger.debug("No event tablename provided by user. Will use system generated name.");
            }
        }
        else {
            this.eventTableName = (String)props.get("EVENT_TABLE");
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info(("Using event tablename:" + this.eventTableName));
            }
        }
        if (storePropCopy.get("NOSQL_PROPERTY") == null) {
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info("RDBMS is used for persisting, setting properties ");
            }
            props.put("javax.persistence.jdbc.driver", storePropCopy.get("JDBC_DRIVER"));
            props.put("javax.persistence.jdbc.url", storePropCopy.get("JDBC_URL"));
            props.put("javax.persistence.jdbc.user", storePropCopy.get("JDBC_USER"));
            props.put("javax.persistence.jdbc.password", Password.getPlainStatic((String)storePropCopy.get("JDBC_PASSWORD")));
            props.put("eclipselink.ddl-generation", storePropCopy.get("DDL_GENERATION"));
            props.put("eclipselink.jdbc.batch-writing", "JDBC");
            if (props.get("BATCH_WRITING_SIZE") != null) {
                try {
                    Utility.extractInt(props.get("BATCH_WRITING_SIZE"));
                    props.put("eclipselink.jdbc.batch-writing.size", props.get("BATCH_WRITING_SIZE").toString());
                }
                catch (NumberFormatException nfe) {
                    props.put("eclipselink.jdbc.batch-writing.size", String.valueOf(10000));
                }
            }
        }
        else {
            this.isNoSQL = true;
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info("NoSQL is used for persisting, setting properties ");
            }
            final String targetDatabase = (String)storePropCopy.get("TARGET_DATABASE");
            if (targetDatabase == null || targetDatabase.isEmpty()) {
                if (HStore.logger.isInfoEnabled()) {
                    HStore.logger.info("no target supplied. assuming this is mongodb");
                }
                props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
                props.put("eclipselink.nosql.property.", storePropCopy.get("NOSQL_PROPERTY"));
                props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
                props.put("eclipselink.nosql.property.mongo.db", storePropCopy.get("DB_NAME"));
                props.put("eclipselink.nosql.property.mongo.host", storePropCopy.get("NOSQL_PROPERTY"));
                this.targetDbType = TARGETDATABASE.MONGODB;
            }
            else if (targetDatabase.equals("org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform") || targetDatabase.equalsIgnoreCase("mongo") || targetDatabase.equalsIgnoreCase("mongodb")) {
                if (HStore.logger.isInfoEnabled()) {
                    HStore.logger.info("this is mongodb");
                }
                props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
                props.put("eclipselink.nosql.property.", storePropCopy.get("NOSQL_PROPERTY"));
                props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
                props.put("eclipselink.nosql.property.mongo.db", storePropCopy.get("DB_NAME"));
                props.put("eclipselink.nosql.property.mongo.host", storePropCopy.get("NOSQL_PROPERTY"));
                this.targetDbType = TARGETDATABASE.MONGODB;
            }
            else if (targetDatabase.equals("org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform") || targetDatabase.equalsIgnoreCase("ondb") || targetDatabase.equalsIgnoreCase("oraclenosql")) {
                if (HStore.logger.isInfoEnabled()) {
                    HStore.logger.info("this is ondb");
                }
                props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform");
                props.put("eclipselink.nosql.property.", storePropCopy.get("NOSQL_PROPERTY"));
                props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLConnectionSpec");
                props.put("eclipselink.nosql.property.nosql.store", storePropCopy.get("DB_NAME"));
                props.put("eclipselink.nosql.property.nosql.host", storePropCopy.get("NOSQL_PROPERTY"));
                this.targetDbType = TARGETDATABASE.ONDB;
            }
            else {
                if (HStore.logger.isInfoEnabled()) {
                    HStore.logger.info("unknown target supplied. assuming this is mongodb");
                }
                props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
                props.put("eclipselink.nosql.property.", storePropCopy.get("NOSQL_PROPERTY"));
                props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
                props.put("eclipselink.nosql.property.mongo.db", storePropCopy.get("DB_NAME"));
                props.put("eclipselink.nosql.property.mongo.host", storePropCopy.get("NOSQL_PROPERTY"));
                this.targetDbType = TARGETDATABASE.MONGODB;
            }
        }
        return props;
    }
    
    private HStore(final MetaInfo.HDStore storeMetaInfo, final BaseServer srv) {
        super(srv, storeMetaInfo);
        this.hds = null;
        this.persistenceAvailable = false;
        this.isNoSQL = false;
        this.targetDbType = TARGETDATABASE.UNKNOWN;
        this.created = 0L;
        this.waitPosition = null;
        this.evictHDs = true;
        this.isRecoveryEnabled = false;
        this.isCrashed = false;
        this.crashCausingException = null;
        this.relaxSchemaCheck = false;
        this.persistedHDStore = null;
        this.persistedHDType = null;
        this.contextAttributes = new HashSet<String>(0);
        this.eventAttributes = new HashMap<String, Set<String>>(0);
        this.elasticsearchCheckpointLock = new Object();
        this.prevCreated = null;
        this.prevCreatedRate = null;
        this.lastLagObserved = null;
        this.allPaths = new ConcurrentHashMap<String, LagMarker>();
        this.lagReportEnabled = Boolean.parseBoolean(System.getProperty("com.dss.lagReportEnabled", "false"));
        this.storeMetaInfo = storeMetaInfo;
        this.output = ((srv == null) ? new SimpleChannel() : srv.createChannel(this));
    }
    
    private void init() throws MetaDataRepositoryException, Exception {
        final MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
        String appName = this.getMetaNsName();
        if (appName == null) {
            final MetaInfo.Namespace app = (MetaInfo.Namespace)metadataRepository.getMetaObjectByUUID(this.storeMetaInfo.namespaceId, HSecurityManager.TOKEN);
            if (app != null) {
                appName = app.name;
            }
        }
        this.fullName = appName + "." + this.getMetaName();
        this.fullTableName = appName + "_" + this.getMetaName();
        this.pu_name = this.fullTableName;
        final Level logLevel = Level.DEBUG;
        if (HStore.logger.isEnabledFor((Priority)logLevel)) {
            HStore.logger.log((Priority)logLevel, String.format("Initializing HDStore '%s'", this.fullName));
            HStore.logger.log((Priority)logLevel, String.format("  Type:       %s", this.storeMetaInfo.type.name()));
            HStore.logger.log((Priority)logLevel, String.format("  Old:        %s", this.usesOldHDStore()));
            HStore.logger.log((Priority)logLevel, String.format("  New:        %s", this.usesNewHDStore()));
            HStore.logger.log((Priority)logLevel, String.format("  In-Memory:  %s", this.usesInMemoryHDStore()));
            HStore.logger.log((Priority)logLevel, "  Properties:");
            int nameLength = 0;
            for (final String propertyName : this.storeMetaInfo.properties.keySet()) {
                nameLength = Math.max(nameLength, propertyName.length());
            }
            final String propertyFormat = "    %-" + nameLength + "s : '%s'";
            final String propertyFormatNull = "    %-" + nameLength + "s : null";
            for (final Map.Entry<String, Object> property : this.storeMetaInfo.properties.entrySet()) {
                final String propertyName2 = property.getKey();
                final Object propertyValue = property.getValue();
                if (propertyValue != null) {
                    HStore.logger.log((Priority)logLevel, String.format(propertyFormat, propertyName2, propertyValue.toString()));
                }
                else {
                    HStore.logger.log((Priority)logLevel, String.format(propertyFormatNull, propertyName2));
                }
            }
        }
        if (this.usesInMemoryHDStore()) {
            final CachingProvider provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
            this.manager = (CacheManager)provider.getCacheManager();
            final int numReplicas = CacheConfiguration.getNumReplicasFromProps(this.storeMetaInfo.getProperties());
            PartitionManager.registerCache(this.fullName, this.storeMetaInfo.uuid);
            final MutableConfiguration<HDKey, HD> hdsConfig = new CacheConfiguration<HDKey, HD>(numReplicas, CacheConfiguration.PartitionManagerType.SIMPLE);
            hdsConfig.setStoreByValue(false);
            hdsConfig.setStatisticsEnabled(false);
            (this.hds = (ICache<HDKey, HD>)(ICache)this.manager.createCache(this.fullName, hdsConfig)).addIndex("key", HIndex.Type.hash, (HIndex.FieldAccessor<Object, HD>)new HIndex.FieldAccessor<Object, HD>() {
                @Override
                public Object getField(final HD parent) {
                    return parent.key;
                }
            });
            this.hds.addIndex("ts", HIndex.Type.tree, (HIndex.FieldAccessor<Long, HD>)new HIndex.FieldAccessor<Long, HD>() {
                @Override
                public Long getField(final HD parent) {
                    return parent.hdTs;
                }
            });
            PartitionManager.registerCache(this.getCacheName(), this.storeMetaInfo.uuid);
            final MutableConfiguration<Object, HDContext> latestContextsConfig = new CacheConfiguration<Object, HDContext>(numReplicas, CacheConfiguration.PartitionManagerType.SIMPLE);
            this.latestContexts = (ICache<Object, HDContext>)(ICache)this.manager.createCache(this.getCacheName(), latestContextsConfig);
        }
        if (HazelcastSingleton.isClientMember()) {
            this.persistenceLayer = null;
        }
        else {
            this.createPersistingPolicy();
        }
        this.contextBeanDef = (MetaInfo.Type)metadataRepository.getMetaObjectByUUID(this.storeMetaInfo.contextType, HSecurityManager.TOKEN);
        this.hdContextClassName = this.contextBeanDef.className + "_hdContext";
        this.hdClassName = "wa.HD_" + this.pu_name;
        try {
            this.hdContextClass = ClassLoader.getSystemClassLoader().loadClass(this.hdContextClassName);
        }
        catch (Exception e) {
            HStore.logger.error(("Problem loading hd context class for " + this.fullName), (Throwable)e);
        }
        try {
            this.hdClass = ClassLoader.getSystemClassLoader().loadClass(this.hdClassName);
            if (HStore.logger.isDebugEnabled()) {
                HStore.logger.debug(("hd run time class loaded :" + this.hdClassName));
            }
        }
        catch (Exception e) {
            HStore.logger.error(("Problem loading hd class for " + this.fullName), (Throwable)e);
        }
        if (this.usesOldHDStore() && this.storeMetaInfo.frequency != null) {
            this.createORMXmlDoc();
        }
        final HDLoader wal = HDLoader.get();
        final String bundleUri = wal.createIfNotExistsBundleDefinition(appName, BundleDefinition.Type.fieldFactory, this.getMetaName());
        final BundleDefinition bundleDefinition = HDLoader.get().getBundleDefinition(bundleUri);
        this.ctxKeyFacs = new ArrayList<Pair<String, FieldFactory>>();
        this.ctxFieldFacs = new ArrayList<Pair<String, FieldFactory>>();
        final List<String> classNames = bundleDefinition.getClassNames();
        for (final String cName : classNames) {
            try {
                final Class cc = ClassLoader.getSystemClassLoader().loadClass(cName);
                final FieldFactory waf = (FieldFactory)cc.newInstance();
                final String fName = waf.getFieldName();
                if (this.contextBeanDef.keyFields.contains(fName)) {
                    this.ctxKeyFacs.add(Pair.make(fName, waf));
                }
                this.ctxFieldFacs.add(Pair.make(fName, waf));
            }
            catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex2) {
                HStore.logger.error(("Problem creating field factory for " + this.contextBeanDef.getFullName() + " with error : " + ex2.getMessage()));
            }
        }
        if (this.storeMetaInfo.eventTypes != null && !this.storeMetaInfo.eventTypes.isEmpty()) {
            this.eventBeanDefs = new ArrayList<MetaInfo.Type>();
            this.eventBeanClasses = new ArrayList<Class<?>>();
            for (final UUID typeUUID : this.storeMetaInfo.eventTypes) {
                final MetaInfo.Type eventBeanDef = (MetaInfo.Type)metadataRepository.getMetaObjectByUUID(typeUUID, HSecurityManager.TOKEN);
                this.eventBeanDefs.add(eventBeanDef);
                try {
                    final Class<?> eventBeanClass = ClassLoader.getSystemClassLoader().loadClass(eventBeanDef.className);
                    this.eventBeanClasses.add(eventBeanClass);
                }
                catch (ClassNotFoundException e3) {
                    HStore.logger.error(("Problem loading event class " + eventBeanDef.className), (Throwable)e3);
                }
            }
        }
        if (this.usesOldHDStore() && this.persistenceLayer != null) {
            this.persistenceLayer.init(this.pu_name);
        }
        if (this.usesNewHDStore()) {
            this.persistedHDStore = getOrCreateHDStore(this.fullName, this.storeMetaInfo.properties);
            this.persistedHDType = this.getPersistentDataType(this.storeMetaInfo.properties);
        }
        this.isRecoveryEnabled = (this.getCurrentApp(this.storeMetaInfo).getRecoveryType() != 0);
        HStore.logger.info(("Recovery is " + (this.isRecoveryEnabled ? "enabled" : "disabled") + " for HDStore '" + this.fullName + "'"));
    }
    
    private void createPersistingPolicy() {
        final Object evictionPolicy = this.storeMetaInfo.properties.get("evict");
        if (evictionPolicy != null) {
            this.evictHDs = !"FALSE".equalsIgnoreCase(evictionPolicy.toString());
        }
        if (HStore.logger.isDebugEnabled()) {
            HStore.logger.debug((this.fullName + ": evict hds=" + this.evictHDs));
        }
        if (this.storeMetaInfo.frequency != null && this.usesOldHDStore()) {
            this.executor = ((this.srv() == null) ? null : this.srv().getThreadPool());
            this.persistenceAvailable = true;
            final Map<String, Object> props = this.extractDBProps(this.storeMetaInfo.properties);
            props.put("wsname", this.getMetaName());
            String target_database = (String)this.storeMetaInfo.properties.get("eclipselink.target-database");
            if (target_database == null) {
                target_database = "rdbms";
            }
            if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
                target_database = "ONDB";
            }
            (this.persistenceLayer = PersistenceFactory.createPersistenceLayer(target_database, PersistenceFactory.PersistingPurpose.HDSTORE, this.pu_name, props)).setStoreName(this.pu_name, this.fullTableName);
            this.persistencePolicy = PersistenceFactory.createPersistencePolicy(this.storeMetaInfo.frequency.value, this.persistenceLayer, this.srv(), props, this);
        }
        else {
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info("persistence policy is none. so hds will NOT be persisted. ");
            }
            this.persistenceLayer = null;
        }
    }
    
    private void createORMXmlDoc() throws MetaDataRepositoryException {
        String driver = (String)this.storeMetaInfo.properties.get("JDBC_URL");
        if (driver != null) {
            driver = driver.toLowerCase();
            if (driver.contains("sqlmxdriver")) {
                final String xml = RTMappingGenerator.createHibernateMappings(this.hdClassName, this.contextBeanDef);
                RTMappingGenerator.addMappings(this.pu_name, xml);
                return;
            }
        }
        String xml;
        if (this.isNoSQL) {
            if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
                xml = RTMappingGenerator.createOrmMappingforRTHDNoSQL(this.hdClassName, this.fullTableName, this.contextBeanDef, TARGETDATABASE.ONDB);
            }
            else {
                xml = RTMappingGenerator.createOrmMappingforRTHDNoSQL(this.hdClassName, this.fullTableName, this.contextBeanDef, TARGETDATABASE.MONGODB);
            }
        }
        else if (this.storeMetaInfo.eventTypes != null && this.storeMetaInfo.eventTypes.size() == 1) {
            final MDRepository MDRepository = MetadataRepository.getINSTANCE();
            final MetaInfo.Type eventType = (MetaInfo.Type)MDRepository.getMetaObjectByUUID(this.storeMetaInfo.eventTypes.get(0), HSecurityManager.TOKEN);
            xml = RTMappingGenerator.createOrmMappingforRTHDRDBMSNew(this.hdClassName, this.fullTableName, this.contextBeanDef.fields, eventType, this.eventTableName, this.storeMetaInfo.properties);
        }
        else {
            xml = RTMappingGenerator.createOrmMappingforRTHDRDBMS(this.hdClassName, this.fullTableName, this.contextBeanDef, this.eventTableName, this.storeMetaInfo.properties);
        }
        RTMappingGenerator.addMappings(this.pu_name, xml);
    }
    
    public Iterable<HD> DBSearch(final Object key, final HQuery query) {
        Iterable<HD> hdsFromDB = null;
        final Set<HDKey> keys = new HashSet<HDKey>();
        if (this.persistenceLayer != null) {
            final long T1 = System.nanoTime();
            if (this.persistenceLayer instanceof HibernatePersistenceLayerImpl) {
                if (HStore.logger.isInfoEnabled()) {
                    HStore.logger.info("this is hibernate query.");
                }
                this.persistenceLayer.init(this.fullTableName);
                final Class wclazz = this.hdClass;
                if (key instanceof HDKey) {
                    hdsFromDB = ((HibernatePersistenceLayerImpl)this.persistenceLayer).getResults((Class<HD>)wclazz, ((HDKey)key).getHDKeyStr(), query.getFilterMap());
                }
                else {
                    hdsFromDB = ((HibernatePersistenceLayerImpl)this.persistenceLayer).getResults((Class<HD>)wclazz, query.getFilterMap(), keys);
                }
            }
            else {
                this.persistenceLayer.init(this.fullTableName);
                final Class wclazz = this.hdClass;
                if (key instanceof HDKey) {
                    if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
                        hdsFromDB = this.persistenceLayer.getResults((Class<HD>)wclazz, ((HDKey)key).getHDKeyStr(), query.getFilterMap());
                    }
                    else {
                        hdsFromDB = ((HStorePersistenceLayerImpl)this.persistenceLayer).getResults((Class<HD>)wclazz, ((HDKey)key).getHDKeyStr(), query);
                    }
                }
                else if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
                    hdsFromDB = ((ONDBPersistenceLayerImpl)this.persistenceLayer).getResults((Class<HD>)wclazz, query.getFilterMap(), keys);
                }
                else {
                    hdsFromDB = ((HStorePersistenceLayerImpl)this.persistenceLayer).getResults((Class<HD>)wclazz, query);
                }
            }
            final long T2 = System.nanoTime();
            final long diff = T2 - T1;
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info(("Executed DB query " + query + " in " + diff / 1000000.0 + "ms"));
            }
        }
        return hdsFromDB;
    }
    
    public static List<MetaInfo.Type>[] getHDStoreDef(final String storename) throws Exception {
        final HStore ws = get(storename);
        final List<MetaInfo.Type>[] defs = (List<MetaInfo.Type>[])new List[] { Collections.singletonList(ws.contextBeanDef), ws.eventBeanDefs };
        return defs;
    }
    
    public Collection<HD> getHDs(final long startTime, final long endTime) {
        try {
            final HQuery q = new HQuery(this.fullName, startTime, endTime);
            final Map<UUID, HD> res = new LinkedHashMap<UUID, HD>();
            Future<Iterable<HD>> dbFuture = null;
            if (this.persistenceAvailable) {
                dbFuture = this.executor.submit((Callable<Iterable<HD>>)new Callable<Iterable<HD>>() {
                    @Override
                    public Iterable<HD> call() throws Exception {
                        return HStore.this.DBSearch(null, q);
                    }
                });
            }
            final Iterator<Cache.Entry<HDKey, HD>> it = this.hds.iterator(new HDFilter(q));
            while (it.hasNext()) {
                final HD wa = (HD)it.next().getValue();
                res.put(wa.uuid, wa);
            }
            if (this.persistenceAvailable) {
                try {
                    final Iterable<HD> dbHDs = dbFuture.get();
                    for (final HD wa2 : dbHDs) {
                        if (q.matches(wa2) && !res.containsKey(wa2.uuid)) {
                            res.put(wa2.uuid, wa2);
                        }
                    }
                    if (dbHDs instanceof JPAIterable) {
                        ((JPAIterable)dbHDs).close();
                    }
                }
                catch (InterruptedException | ExecutionException ex2) {
                    HStore.logger.error("Problem obtaining hds from db", (Throwable)ex2);
                }
            }
            return res.values();
        }
        catch (Throwable t) {
            HStore.logger.error("Problem executing hd query using", t);
            throw t;
        }
    }
    
    public Map<HDKey, Map<String, Object>> getHDs(final HDKey key, final String[] fields, final Map<String, Object> filter) {
        try {
            final long stime = System.nanoTime();
            final HQuery q = new HQuery(this.fullName, this.contextBeanDef, fields, filter);
            q.setSingleKey(key);
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info(("Starting execution of query " + q));
            }
            if (!this.persistenceAvailable) {
                if (q.requiresResultStats()) {
                    final HQuery.ResultStats stats = this.hds.queryStats(q);
                    q.setGlobalStats(stats);
                }
                if (this.usesInMemoryHDStore() && q.getLatestPerKey && q.endTime == null && !q.getEvents) {
                    final long T1 = System.nanoTime();
                    int count = 0;
                    for (final Object akey : this.latestContexts.getAllKeys()) {
                        final HD w = new HD(akey);
                        final HDContext wc = this.latestContexts.get(akey);
                        w.setContext(wc);
                        q.runOne(w);
                        ++count;
                    }
                    final long T2 = System.nanoTime();
                    final long diff = T2 - T1;
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Executed latest context query " + q + " for " + count + " hds in " + diff / 1000000.0 + "ms"));
                    }
                }
                else {
                    final long T1 = System.nanoTime();
                    this.hds.query(q);
                    final long T3 = System.nanoTime();
                    final long diff2 = T3 - T1;
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Executed in-memory query " + q + " for " + q.processed + " hds in " + diff2 / 1000000.0 + "ms"));
                    }
                }
            }
            else {
                Iterable<HD> dbHDs = null;
                boolean triedToGetDbHDs = false;
                final Callable<Iterable<HD>> dbTask = new Callable<Iterable<HD>>() {
                    @Override
                    public Iterable<HD> call() throws Exception {
                        return HStore.this.DBSearch(key, q);
                    }
                };
                final Future<Iterable<HD>> dbFuture = this.executor.submit(dbTask);
                if (q.requiresResultStats()) {
                    final HQuery.ResultStats stats2 = this.hds.queryStats(q);
                    try {
                        dbHDs = dbFuture.get();
                    }
                    catch (InterruptedException | ExecutionException ex3) {
                        HStore.logger.error("Problem obtaining hds from db", (Throwable)ex3);
                    }
                    triedToGetDbHDs = true;
                    if (dbHDs != null) {
                        final HQuery.ResultStats dbStats = q.getResultStats(dbHDs);
                        if (stats2.startTime == 0L || dbStats.startTime < stats2.startTime) {
                            stats2.startTime = dbStats.startTime;
                        }
                        if (stats2.endTime == 0L || dbStats.endTime > stats2.endTime) {
                            stats2.endTime = dbStats.endTime;
                        }
                        final HQuery.ResultStats resultStats = stats2;
                        resultStats.count += dbStats.count;
                    }
                    q.setGlobalStats(stats2);
                }
                if (this.usesInMemoryHDStore() && q.getLatestPerKey && q.endTime == null && !q.getEvents) {
                    if (HStore.logger.isDebugEnabled()) {
                        HStore.logger.debug((this.fullName + ": Using only latest hd context"));
                    }
                    final long T4 = System.nanoTime();
                    int count2 = 0;
                    for (final Object akey2 : this.latestContexts.getAllKeys()) {
                        final HD w2 = new HD(akey2);
                        final HDContext wc2 = this.latestContexts.get(akey2);
                        w2.setContext(wc2);
                        q.runOne(w2);
                        ++count2;
                    }
                    final long T5 = System.nanoTime();
                    final long diff3 = T5 - T4;
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Executed latest context query " + q + " for " + count2 + " hds in " + diff3 / 1000000.0 + "ms"));
                    }
                }
                else {
                    final long T4 = System.nanoTime();
                    this.hds.query(q);
                    final long T6 = System.nanoTime();
                    final long diff4 = T6 - T4;
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Executed in-memory query " + q + " for " + q.processed + " hds in " + diff4 / 1000000.0 + "ms"));
                    }
                }
                if (!triedToGetDbHDs) {
                    try {
                        dbHDs = dbFuture.get();
                    }
                    catch (InterruptedException | ExecutionException ex4) {
                        HStore.logger.error("Problem obtaining hds from db", (Throwable)ex4);
                    }
                }
                if (dbHDs != null) {
                    final long T4 = System.nanoTime();
                    int count2 = 0;
                    for (final HD hd : dbHDs) {
                        q.runOne(hd);
                        ++count2;
                    }
                    final long T5 = System.nanoTime();
                    final long diff3 = T5 - T4;
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Executed analysis of DB results for " + q + " for " + count2 + " hds in " + diff3 / 1000000.0 + "ms"));
                    }
                }
                if (dbHDs instanceof JPAIterable) {
                    ((JPAIterable)dbHDs).close();
                }
            }
            final long etime = System.nanoTime();
            final long diff5 = etime - stime;
            final Map<HDKey, Map<String, Object>> res = q.getResults();
            if (HStore.logger.isInfoEnabled()) {
                HStore.logger.info(("Executed query " + q + " with " + res.size() + " results in " + diff5 / 1000000.0 + "ms"));
            }
            return res;
        }
        catch (Throwable t) {
            HStore.logger.error(("Problem executing hd query using key=" + key + " fields=" + ((fields == null) ? "NULL" : cern.colt.Arrays.toString((Object[])fields)) + " filter=" + filter), t);
            throw t;
        }
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        if (this.lastLagObserved != null) {
            final StringBuilder stringBuilder = new StringBuilder();
            for (final LagMarker lagMarker : this.allPaths.values()) {
                stringBuilder.append(lagMarker.toString() + "\n");
            }
            stringBuilder.append("\n");
            monEvs.add(MonitorEvent.Type.LAG_REPORT, stringBuilder.toString());
            monEvs.add(MonitorEvent.Type.LAG_RATE, this.lastLagObserved.calculateTotalLag());
            final ObjectMapper objectMapper = ObjectMapperFactory.newInstance();
            final ArrayNode fieldArray = objectMapper.createArrayNode();
            for (final LagMarker lagMarker2 : this.allPaths.values()) {
                fieldArray.add(lagMarker2.toJSON());
            }
            monEvs.add(MonitorEvent.Type.LAG_REPORT_JSON, fieldArray.toString());
        }
        final Long c = this.created;
        final long timeStamp = monEvs.getTimeStamp();
        if (!c.equals(this.prevCreated)) {
            monEvs.add(MonitorEvent.Type.HDS_CREATED, c);
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
        }
        final long delta = (this.prevCreated == null) ? 0L : (c - this.prevCreated);
        final Long rate = (delta == 0L) ? 0L : ((long)Math.ceil(1000.0 * delta / (timeStamp - this.prevTimeStamp)));
        if (!rate.equals(this.prevCreatedRate)) {
            monEvs.add(MonitorEvent.Type.INPUT_RATE, rate);
            monEvs.add(MonitorEvent.Type.RATE, rate);
            monEvs.add(MonitorEvent.Type.HDS_CREATED_RATE, rate);
        }
        this.prevCreated = c;
        this.prevCreatedRate = rate;
    }
    
    @Override
    public void close() throws Exception {
        HStore.logger.info(("HDStore " + this.fullName + " is being closed"));
        this.resetProcessThread();
        this.output.close();
        if (HStore.expungeableHDs != null && HStore.memoryMonitor != null) {
            for (final LRUNode w : HStore.expungeableHDs.toList()) {
                if (w.hStore == this) {
                    HStore.expungeableHDs.remove(w);
                }
            }
            if (HStore.expungeableHDs.size() == 0) {
                HStore.memoryMonitor.stopRunning();
                HStore.memoryMonitor = null;
            }
        }
        if (this.persistedHDType != null) {
            this.persistedHDType.fsync();
        }
        if (this.persistencePolicy != null) {
            this.persistencePolicy.close();
        }
        if (this.persistenceLayer != null) {
            this.persistenceLayer.close();
        }
        this.persistencePolicy = null;
        this.closeCache();
        if (this.isRecoveryEnabled && this.persistedHDStore != null) {
            final HDStoreManager manager = this.persistedHDStore.getManager();
            final CheckpointManager checkpointManager = manager.getCheckpointManager();
            checkpointManager.closeHDStore(this.fullName);
        }
        remove(this.getMetaID());
        HStore.logger.info(("HDStore '" + this.fullName + "' is closed."));
    }
    
    @Override
    public void flush() {
        if (this.persistencePolicy != null && !this.isCrashed) {
            this.persistencePolicy.flush();
        }
        if (this.persistedHDType != null) {
            this.persistedHDType.flush();
        }
        if (this.usesNewHDStore()) {
            final HDStoreManager manager = this.persistedHDStore.getManager();
            final CheckpointManager checkpointManager = manager.getCheckpointManager();
            checkpointManager.flush(this.persistedHDStore);
        }
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    @Override
    public void stop() throws Exception {
        this.flush();
        this.stopCache();
    }
    
    public void stopCache() {
        try {
            if (this.usesInMemoryHDStore()) {
                this.manager.stopCache(this.getCacheName());
                if (this.hds != null) {
                    this.manager.stopCache(this.fullName);
                }
            }
        }
        catch (Exception e) {
            HStore.logger.error(e.getMessage(), (Throwable)e);
        }
        final long time = System.currentTimeMillis();
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.INPUT_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.HDS_CREATED_RATE, Long.valueOf(0L), Long.valueOf(time)));
        MonitorCollector.reportMonitorEvent(new MonitorEvent(BaseServer.getBaseServer().getServerID(), this.getMetaID(), MonitorEvent.Type.RATE, Long.valueOf(0L), Long.valueOf(time)));
    }
    
    public void closeCache() {
        try {
            if (this.usesInMemoryHDStore()) {
                this.manager.close(this.getCacheName());
                if (this.hds != null) {
                    this.manager.close(this.fullName);
                }
            }
        }
        catch (Exception e) {
            HStore.logger.error(e);
        }
    }
    
    public void startCache(final List<UUID> servers) {
        try {
            if (this.usesInMemoryHDStore()) {
                this.manager.startCache(this.getCacheName(), servers);
                if (this.hds != null) {
                    this.manager.startCache(this.fullName, servers);
                }
            }
        }
        catch (Exception e) {
            HStore.logger.error(e);
        }
    }
    
    public String getCacheName() {
        return getCacheName(this.fullName);
    }
    
    public static String getCacheName(final String hdStoreName) {
        return hdStoreName + "-Contexts";
    }
    
    public HD createHD(final Object key) {
        HD hd = null;
        try {
            final Object wobj = this.hdClass.newInstance();
            this.hdClass.cast(wobj);
            hd = (HD)wobj;
        }
        catch (Exception e1) {
            HStore.logger.error(("failed to create HD runtime class : " + this.hdClassName + e1));
        }
        hd.init(key);
        hd.setInternalHDStoreName(this.fullTableName);
        try {
            final Object obj = this.hdContextClass.newInstance();
            this.hdContextClass.cast(obj);
            hd.setContext(obj);
        }
        catch (Exception e2) {
            HStore.logger.error(("Failed creating HDContext runtime class : " + e2));
        }
        ++this.created;
        return hd;
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (this.lagReportEnabled && this.shouldMarkerBePassedAlong(event)) {
            this.recordLagMarker(((TaskEvent)event).getLagMarker());
            this.lastLagObserved = ((TaskEvent)event).getLagMarker().copy();
            this.lagRecordInfo(this.getMetaInfo(), this.lastLagObserved);
            this.allPaths.put(this.lastLagObserved.key(), this.lastLagObserved);
        }
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        try {
            if (this.isFlowInError()) {
                return;
            }
            this.setProcessThread();
            final List<DARecord> jsonBatch = new ArrayList<DARecord>();
            for (final DARecord data : event.batch()) {
                Position dataPosition = null;
                if (data.position != null) {
                    dataPosition = data.position.createAugmentedPosition(this.getMetaID(), (String)null);
                    if (this.isBeforeWaitPosition(dataPosition)) {
                        if (Logger.getLogger("Recovery").isDebugEnabled()) {
                            Logger.getLogger("Recovery").debug(("Dropping duplicate HD: " + data.position));
                            continue;
                        }
                        continue;
                    }
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug(("Accepting new HD: " + data.position));
                }
                final SimpleEvent se = (SimpleEvent)data.data;
                final Object[] keys = new Object[this.ctxKeyFacs.size()];
                for (int i = 0; i < this.ctxKeyFacs.size(); ++i) {
                    keys[i] = this.ctxKeyFacs.get(i).second.getField(se);
                }
                Object key;
                if (keys.length == 1) {
                    key = keys[0];
                }
                else {
                    key = RecordKey.createKeyFromObjArray(keys);
                }
                if (key == null) {
                    if (!HStore.logger.isInfoEnabled()) {
                        continue;
                    }
                    HStore.logger.info(("Creating no hd because SimpleEvent has null key: " + se + " with key : " + ((se == null) ? "NULL!" : se.getKey())));
                }
                else {
                    final HD newHD = this.createHD(key);
                    final Map<String, Object> wContextMap = new HashMap<String, Object>();
                    if (this.usesInMemoryHDStore() && !se.allFieldsSet) {
                        final HDContext latestContext = this.latestContexts.get(key);
                        if (latestContext != null) {
                            wContextMap.putAll(latestContext);
                        }
                    }
                    int j = 0;
                    for (final Pair<String, FieldFactory> p : this.ctxFieldFacs) {
                        if (se.isFieldSet(j++)) {
                            final Object val = p.second.getField(data.data);
                            wContextMap.put(p.first, val);
                        }
                    }
                    newHD.setContext(wContextMap);
                    newHD.context.hdID = newHD.uuid;
                    if (this.usesInMemoryHDStore()) {
                        this.latestContexts.put(key, newHD.context);
                    }
                    if (se.linkedSourceEvents != null && this.eventBeanClasses != null) {
                        for (final Object[] array : se.linkedSourceEvents) {
                            final Object[] lses = array;
                            for (final Object lse : array) {
                                if (lse instanceof SimpleEvent) {
                                    if (this.eventBeanClasses.contains(lse.getClass())) {
                                        newHD.addEvent((SimpleEvent)lse);
                                    }
                                }
                            }
                        }
                    }
                    newHD.setPosition(dataPosition);
                    if (this.usesInMemoryHDStore()) {
                        newHD.addHDListener(this);
                        newHD.designateCommitted();
                        this.hds.put(newHD.getMapKey(), newHD);
                        jsonBatch.add(new DARecord(newHD));
                    }
                    if (this.usesOldHDStore() && this.persistenceAvailable) {
                        this.persistencePolicy.addHD(newHD);
                    }
                    if (this.usesNewHDStore()) {
                        final HD jsonWA = this.addToHDStore(newHD);
                        final JsonNodeEvent e = new JsonNodeEvent();
                        e.setData((JsonNode)jsonWA);
                        jsonBatch.add(new DARecord(e));
                    }
                    this.updateSessionStats(newHD);
                }
            }
            if (!jsonBatch.isEmpty()) {
                final TaskEvent te = TaskEvent.createStreamEvent(jsonBatch);
                this.output.publish((ITaskEvent)te);
            }
        }
        catch (Exception ex) {
            HStore.logger.error(("exception receiving hds by hdstore:" + this.storeMetaInfo.getFullName()));
            this.notifyAppMgr(EntityType.HDSTORE, this.getMetaName(), this.getMetaID(), ex, "hdstore receive", event);
            throw new Exception(ex);
        }
    }
    
    private synchronized boolean isBeforeWaitPosition(final Position position) {
        if (this.waitPosition == null || position == null) {
            return false;
        }
        for (final Path hdPath : position.values()) {
            if (!this.waitPosition.containsKey(hdPath.getPathHash())) {
                continue;
            }
            final SourcePosition sp = this.waitPosition.get(hdPath.getPathHash()).getLowSourcePosition();
            if (hdPath.getLowSourcePosition().compareTo(sp) <= 0) {
                return true;
            }
            this.waitPosition.removePath(hdPath.getPathHash());
        }
        if (this.waitPosition.isEmpty()) {
            this.waitPosition = null;
        }
        return false;
    }
    
    private static double memoryFree() {
        final long totalMemory = Runtime.getRuntime().totalMemory();
        final long maxMemory = Runtime.getRuntime().maxMemory();
        final long freeMemory = Runtime.getRuntime().freeMemory();
        final boolean hadReachedMaxMemory = HStore.reachedMaxMemory;
        final long usedMemory = totalMemory - freeMemory;
        final long actualFree = maxMemory - usedMemory;
        final double actualFreePercent = actualFree * 100.0 / maxMemory * 1.0;
        if (HStore.logger.isInfoEnabled()) {
            if (HStore.reachedMaxMemory && !hadReachedMaxMemory) {
                HStore.logger.info(("Actual Free = " + actualFree + " [" + actualFreePercent + "%]: Reached max memory with " + freeMemory * 100.0 / (maxMemory * 1.0) + "% free"));
            }
            if (!HStore.reachedMaxMemory && hadReachedMaxMemory) {
                HStore.logger.info(("Actual Free = " + actualFree + " [" + actualFreePercent + "%]: Downsized from max memory to " + totalMemory + " with " + freeMemory * 100.0 / (maxMemory * 1.0) + "% free"));
            }
        }
        return actualFreePercent;
    }
    
    private static void evict() {
        LRUNode node = null;
        while (node == null) {
            node = HStore.expungeableHDs.removeLeastRecentlyUsed();
            if (node != null) {
                node.hStore.hds.remove(node.hdKey);
                final HDContext c = node.hStore.latestContexts.get(node.hdKey.key);
                if (c == null || c.hdID == null || !c.hdID.equals(node.hdKey.id)) {
                    continue;
                }
                node.hStore.latestContexts.remove(node.hdKey.key);
            }
            else {
                try {
                    HStore.expungeablesLock.lock();
                    HStore.expungeablesExist.await();
                }
                catch (InterruptedException e) {
                    if (HStore.stopEviction) {
                        break;
                    }
                    continue;
                }
                finally {
                    HStore.expungeablesLock.unlock();
                }
            }
        }
    }
    
    @Override
    public void hdAccessed(final HD hd) {
        if (this.usesInMemoryHDStore() && this.evictHDs && (!this.persistenceAvailable || hd.isPersisted())) {
            if (HStore.expungeableHDs == null) {
                HStore.expungeableHDs = PersistenceFactory.createExpungeList((String)this.storeMetaInfo.properties.get("hdDef.persist.expungePolicy"));
            }
            if (HStore.memoryMonitor == null) {
                synchronized (MemoryMonitor.class) {
                    if (HStore.memoryMonitor == null) {
                        (HStore.memoryMonitor = new MemoryMonitor()).start();
                    }
                }
            }
            final HDKey hdKey = hd.getMapKey();
            HStore.expungeableHDs.add(new LRUNode(hdKey, this));
            HStore.expungeablesLock.lock();
            HStore.expungeablesExist.signal();
            HStore.expungeablesLock.unlock();
        }
    }
    
    public static FieldFactory genFieldFactory(final String bundleUri, final HDLoader wal, final String eventTypeClassName, final String fieldName) throws Exception {
        final ClassPool pool = wal.getBundlePool(bundleUri);
        final String className = "FieldFactory" + System.nanoTime();
        final CtClass cc = pool.makeClass(className);
        final CtClass sup = pool.get(FieldFactory.class.getName());
        cc.setSuperclass(sup);
        final String code = "public Object getField(Object obj)\n{\n\t" + eventTypeClassName + " tmp = (" + eventTypeClassName + ")obj;\n\treturn " + FieldToObject.genConvert("tmp." + fieldName) + ";\n}\n";
        final CtMethod m = CtNewMethod.make(code, cc);
        cc.addMethod(m);
        cc.setModifiers(cc.getModifiers() & 0xFFFFFBFF);
        cc.setModifiers(1);
        wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        final Class<?> klass = wal.loadClass(className);
        final FieldFactory waf = (FieldFactory)klass.newInstance();
        return waf;
    }
    
    @Override
    public String toString() {
        return this.getMetaUri() + " - " + this.getMetaID() + " RUNTIME";
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    public void start() throws MetaDataRepositoryException {
        this.isCrashed = false;
        this.crashCausingException = null;
        if (this.isRecoveryEnabled) {
            this.waitPosition = new PathManager(this.getPersistedCheckpointPosition());
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug(("HDStore '" + this.fullName + "' is starting at position:"));
                if (this.waitPosition != null) {
                    com.datasphere.utility.Utility.prettyPrint(this.waitPosition);
                }
            }
            if (this.persistedHDStore != null) {
                final HDStoreManager manager = this.persistedHDStore.getManager();
                final CheckpointManager checkpointManager = manager.getCheckpointManager();
                checkpointManager.removeInvalidHDs(this.fullName);
                checkpointManager.start(this.fullName);
            }
        }
    }
    
    public Position getPersistedCheckpointPosition() {
        return com.datasphere.utility.Utility.updateUuids(this.usesOldHDStore() ? this.getPersistedCheckpointPositionOld() : this.getPersistedCheckpointPositionNew());
    }
    
    public Position getPersistedCheckpointPositionOld() {
        Position result = null;
        if (this.persistenceLayer != null) {
            try {
                result = this.persistenceLayer.getWSPosition(this.getMetaNsName(), this.getMetaName());
            }
            catch (Exception e) {
                HStore.logger.error((this.fullName + ": Problem getting checkpointPosition for " + this.fullTableName), (Throwable)e);
                this.notifyAppMgr(EntityType.HDSTORE, this.getMetaName(), this.getMetaID(), e, "get checkpoint position", new Object[0]);
            }
        }
        if (HStore.logger.isDebugEnabled()) {
            HStore.logger.debug(("HDStore " + this.getMetaName() + " returning HDStore Checkpoint from disk: " + result));
        }
        return result;
    }
    
    public Position getPersistedCheckpointPositionNew() {
        Position result = null;
        if (this.isRecoveryEnabled && this.persistedHDStore != null) {
            final HDStoreManager manager = this.persistedHDStore.getManager();
            final CheckpointManager checkpointManager = manager.getCheckpointManager();
            result = checkpointManager.get(this.fullName);
        }
        return result;
    }
    
    private MetaInfo.Flow getCurrentApp(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        if (obj == null) {
            return null;
        }
        if (obj.type.equals(EntityType.APPLICATION)) {
            return (MetaInfo.Flow)obj;
        }
        final Set<UUID> parents = obj.getReverseIndexObjectDependencies();
        if (parents != null) {
            final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
            for (final UUID aUUID : parents) {
                final MetaInfo.MetaObject parent = metadataRepository.getMetaObjectByUUID(aUUID, HSecurityManager.TOKEN);
                if (parent != null) {
                    if (parent.getType() == EntityType.APPLICATION) {
                        return (MetaInfo.Flow)parent;
                    }
                    if (parent.getType() != EntityType.WASTOREVIEW && parent.getReverseIndexObjectDependencies() != null) {
                        return this.getCurrentApp(parent);
                    }
                    continue;
                }
                else {
                    if (!HStore.logger.isDebugEnabled()) {
                        continue;
                    }
                    HStore.logger.debug(("Found uuid in " + obj.getFullName() + " dependencies that has no matching object: " + aUUID));
                }
            }
        }
        return null;
    }
    
    @Override
    public Position getCheckpoint() {
        final long startTime = System.currentTimeMillis();
        final Position result = this.usesOldHDStore() ? this.getCheckpointOld() : this.getCheckpointNew();
        final long elapsed = System.currentTimeMillis() - startTime;
        Utility.reportExecutionTime(HStore.logger, elapsed);
        return result;
    }
    
    public Position getCheckpointOld() {
        PathManager result = null;
        if (this.persistencePolicy != null) {
            final Set<HD> toBePersisted = this.persistencePolicy.getUnpersistedHDs();
            if (toBePersisted != null) {
                for (final HD w : toBePersisted) {
                    if (result == null) {
                        result = new PathManager();
                    }
                    result.mergeLowerPositions(w.getPosition());
                }
            }
        }
        return (result != null) ? result.toPosition() : null;
    }
    
    public Position getCheckpointNew() {
        synchronized (this.elasticsearchCheckpointLock) {
            if (this.isRecoveryEnabled) {
                this.persistedHDType.flush();
                final HDStoreManager manager = this.persistedHDStore.getManager();
                final CheckpointManager checkpointManager = manager.getCheckpointManager();
                checkpointManager.flush(this.persistedHDStore);
            }
            return null;
        }
    }
    
    public boolean clearHDStoreCheckpoint() {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            Logger.getLogger("Recovery").debug(("Clearing HDStore checkpoint for " + this.fullName));
        }
        final Boolean result = this.usesOldHDStore() ? this.clearHDStoreCheckpointOld() : clearHDStoreCheckpointNew((MetaInfo.HDStore)this.getMetaInfo());
        return result;
    }
    
    public boolean clearHDStoreCheckpointOld() {
        if (this.persistenceLayer != null) {
            try {
                return this.persistenceLayer.clearWSPosition(this.getMetaNsName(), this.getMetaName());
            }
            catch (Exception e) {
                Logger.getLogger("Recovery").error((this.fullName + ": Problem clearing Checkpoint for " + this.fullTableName), (Throwable)e);
                return false;
            }
        }
        return true;
    }
    
    public static boolean clearHDStoreCheckpointNew(final MetaInfo.HDStore hdStore) {
        final Map<String, Object> properties = hdStore.getProperties();
        if (usesNewHDStore(properties)) {
            final String hdStoreName = hdStore.getFullName();
            final HDStoreManager manager = HDStores.getInstance(properties);
            if (manager == null) {
                Logger.getLogger("Recovery").warn(("HDStore not found for resetting recovery: " + hdStore.getName()));
                return false;
            }
            final CheckpointManager checkpointManager = manager.getCheckpointManager();
            if (checkpointManager == null) {
                Logger.getLogger("Recovery").warn(("HDStore Checkpoint Manager not found for resetting recovery: " + hdStore.getName()));
                return false;
            }
            checkpointManager.writeBlankCheckpoint(hdStoreName);
        }
        return true;
    }
    
    static {
        HStore.logger = Logger.getLogger((Class)HStore.class);
        stores = new ConcurrentHashMap<String, HStore>();
        hdStoreMetadata = (JsonNode)Utility.objectMapper.createArrayNode().add("any").add("checkpoint");
        (javaTypeToHDType = new HashMap<String, HDDataType>()).put("byte", new HDDataType("integer", false));
        HStore.javaTypeToHDType.put("short", new HDDataType("integer", false));
        HStore.javaTypeToHDType.put("int", new HDDataType("integer", false));
        HStore.javaTypeToHDType.put("java.lang.Byte", new HDDataType("integer", true));
        HStore.javaTypeToHDType.put("java.lang.Short", new HDDataType("integer", true));
        HStore.javaTypeToHDType.put("java.lang.Integer", new HDDataType("integer", true));
        HStore.javaTypeToHDType.put("long", new HDDataType("long", false));
        HStore.javaTypeToHDType.put("java.lang.Long", new HDDataType("long", true));
        HStore.javaTypeToHDType.put("float", new HDDataType("double", false));
        HStore.javaTypeToHDType.put("double", new HDDataType("double", false));
        HStore.javaTypeToHDType.put("java.lang.Float", new HDDataType("double", true));
        HStore.javaTypeToHDType.put("java.lang.Double", new HDDataType("double", true));
        HStore.javaTypeToHDType.put("java.lang.Number", new HDDataType("double", true));
        HStore.javaTypeToHDType.put("boolean", new HDDataType("boolean", false));
        HStore.javaTypeToHDType.put("java.lang.Boolean", new HDDataType("boolean", true));
        HStore.javaTypeToHDType.put("string", new HDDataType("string", false));
        HStore.javaTypeToHDType.put("java.lang.String", new HDDataType("string", true));
        HStore.javaTypeToHDType.put("datetime", new HDDataType("datetime", false));
        HStore.javaTypeToHDType.put("org.joda.time.DateTime", new HDDataType("datetime", true));
        HStore.reachedMaxMemory = false;
        HStore.expungeablesLock = new ReentrantLock();
        HStore.expungeablesExist = HStore.expungeablesLock.newCondition();
        HStore.stopEviction = false;
    }
    
    class LRUNode
    {
        HDKey hdKey;
        HStore hStore;
        
        public LRUNode(final HDKey hdKey, final HStore hStore) {
            this.hdKey = hdKey;
            this.hStore = hStore;
        }
        
        @Override
        public boolean equals(final Object obj) {
            return obj instanceof LRUNode && this.hdKey.equals(((LRUNode)obj).hdKey);
        }
        
        @Override
        public int hashCode() {
            return this.hdKey.hashCode();
        }
    }
    
    public enum TARGETDATABASE
    {
        UNKNOWN, 
        MONGODB, 
        ONDB;
    }
    
    private static class HDDataType
    {
        public final String typeName;
        public final boolean nullable;
        
        HDDataType(final String typeName, final boolean nullable) {
            this.typeName = typeName;
            this.nullable = nullable;
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof HDDataType) {
                final HDDataType instance = HDDataType.class.cast(obj);
                return instance.typeName.equals(this.typeName) && instance.nullable == this.nullable;
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return this.typeName.hashCode() ^ (this.nullable ? 1 : 0);
        }
    }
    
    public static class HDFilter implements Filter<HDKey, HD>, Serializable
    {
        private static final long serialVersionUID = 1368964098470210574L;
        public HQuery q;
        
        public HDFilter(final HQuery q) {
            this.q = q;
        }
        
        @Override
        public boolean matches(final HDKey key, final HD value) {
            return this.q.matches(value);
        }
        
        @Override
        public String toString() {
            return this.q.toString();
        }
    }
    
    static class MemoryMonitor extends Thread
    {
        static double evictionthreshold;
        volatile boolean running;
        
        public MemoryMonitor() {
            this.running = true;
            MemoryMonitor.evictionthreshold = Server.evictionThresholdPercent();
            this.setName("HDStore-MemoryMonitor");
        }
        
        public void stopRunning() {
            this.running = false;
            HStore.stopEviction = true;
            this.interrupt();
        }
        
        @Override
        public void run() {
            while (this.running) {
                double freeMemory = memoryFree();
                if (freeMemory < 100.0 - MemoryMonitor.evictionthreshold) {
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Hit " + (100.0 - freeMemory) + "% memory level - evicting hds"));
                    }
                    int count = 0;
                    while (this.running && freeMemory < 105.0 - MemoryMonitor.evictionthreshold) {
                        evict();
                        ++count;
                        freeMemory = memoryFree();
                        if (count % 1000 == 0) {
                            LockSupport.parkNanos(1000000L);
                        }
                    }
                    if (HStore.logger.isInfoEnabled()) {
                        HStore.logger.info(("Evicted - " + count + " hds - now at " + (100.0 - freeMemory) + "% memory level"));
                    }
                }
                LockSupport.parkNanos(10000000L);
            }
        }
        
        static {
            MemoryMonitor.evictionthreshold = 0.0;
        }
    }
    
    public abstract static class FieldFactory
    {
        public abstract Object getField(final Object p0);
        
        public abstract String getFieldName();
    }
}
