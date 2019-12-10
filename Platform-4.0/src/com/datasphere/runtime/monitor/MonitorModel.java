package com.datasphere.runtime.monitor;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.PersistenceUnitLoadingException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.classloading.WALoader;
import com.datasphere.common.errors.CommonError;
import com.datasphere.common.errors.IError;
import com.datasphere.errorhandling.DatallException;
import com.datasphere.event.Event;
import com.datasphere.health.AppHealth;
import com.datasphere.health.CacheHealth;
import com.datasphere.health.HealthMonitor;
import com.datasphere.health.HealthMonitorImpl;
import com.datasphere.health.HealthRecord;
import com.datasphere.health.HealthRecordBuilder;
import com.datasphere.health.HealthRecordCollection;
import com.datasphere.health.Issue;
import com.datasphere.health.ServerHealth;
import com.datasphere.health.SourceHealth;
import com.datasphere.health.StateChange;
import com.datasphere.health.TargetHealth;
import com.datasphere.health.HStoreHealth;
import com.datasphere.intf.PersistenceLayer;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDLocalCache;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataDBDetails;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.persistence.PersistenceFactory;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.Version;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HDKey;
import com.datasphere.hdstore.DataType;
import com.datasphere.hdstore.InternalType;
import com.datasphere.hdstore.HD;
import com.datasphere.hdstore.HDQuery;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.HDStores;
import com.datasphere.hdstore.exceptions.HDStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

@PropertyTemplate(name = "MonitorModel", type = AdapterType.internal, properties = { @PropertyTemplateProperty(name = "derbyDbDir", type = String.class, required = false, defaultValue = "derby/monitorDb") }, outputType = MonitorBatchEvent.class)
public class MonitorModel extends BaseProcess implements Subscriber, Runnable
{
    private static Logger logger;
    private static final UUID MonitorModelID;
    public static final String KAFKA_NAME = "Kafka Cluster";
    public static final UUID KAFKA_SERVER_UUID;
    public static final UUID KAFKA_ENTITY_UUID;
    public static final UUID ES_SERVER_UUID;
    public static final UUID ES_ENTITY_UUID;
    private static final Map<UUID, String> uuidMap;
    public static final int NANOS = 1000000000;
    public static final int PERCENT = 100;
    private static final int DEFAULT_MAX_EVENTS = 1000;
    private static final int DEFAULT_MAX_EVENTS_FOR_LOAD = 10000;
    private static final int DEFAULT_DB_EXPIRE_TIME = 86400000;
    private static final long HEALTH_WRITE_PERIOD = 30000L;
    private static final long KAFKA_COLLECTION_PERIOD = 10000L;
    private static final long ES_COLLECTION_PERIOD = 10000L;
    private static int persistPeriod;
    private static volatile MonitorModel instance;
    private static Deque<List<MonitorEvent>> insertQueue;
    private final ScheduledExecutorService executor;
    private static final KafkaMonitor kafkaMonitor;
    private static final ElasticsearchMonitor elasticsearchMonitor;
    static boolean running;
    static boolean persistMonitor;
    static boolean persistHealth;
    static final Lock persistChangeLock;
    private static HDStoreManager monitorModelPersistedHDStore;
    private static DataType monitorEventDataType;
    private static HDStoreManager healthRecordHDStore;
    private static DataType healthRecordDataType;
    private MDRepository mdRepository;
    private static MDLocalCache mdLocalCache;
    private static final MBeanServer mbs;
    private static final Map<String, Object> beanMap;
    private static UUID monitorModelID;
    private static boolean isFailedToInsertFlag;
    static DecimalFormat decimalFormat;
    static List<String> rateDatum;
    static Map<MonitorData.Key, MonitorData> monitorDataMap;
    static Map<UUID, Long> serverTxMap;
    static Map<UUID, Long> serverRxMap;
    private long currentHealthCreateTime;
    private HealthRecordBuilder currentHealthReportBuilder;
    private MetaDataDBDetails metaDataDBDetails;
    private static volatile PersistenceLayer theDb;
    
    public static MonitorModel getOrCreateInstance() {
        if (MonitorModel.instance == null) {
            synchronized (MonitorModel.class) {
                if (MonitorModel.instance == null) {
                    MonitorModel.instance = new MonitorModel();
                }
            }
        }
        return MonitorModel.instance;
    }
    
    public static MonitorModel getInstance() {
        return MonitorModel.instance;
    }
    
    public static void shutdown() throws Exception {
        if (MonitorModel.instance != null) {
            MonitorModel.instance.close();
            MonitorModel.instance = null;
        }
    }
    
    public static String getWhereClause(final Map<String, Object> params) {
        if (params.isEmpty()) {
            return "";
        }
        final StringBuilder result = new StringBuilder();
        result.append("WHERE ");
        final Iterator<String> entries = params.keySet().iterator();
        while (entries.hasNext()) {
            final String entry = entries.next();
            if (entry.equalsIgnoreCase("serverID")) {
                result.append("se.serverID = :serverID ");
            }
            if (entry.equalsIgnoreCase("entityID")) {
                result.append("se.entityID = :entityID ");
            }
            if (entry.equalsIgnoreCase("startTime")) {
                result.append("se.timeStamp >= :startTime ");
            }
            if (entry.equalsIgnoreCase("datumNames")) {
                result.append("se.type in :datumNames ");
            }
            if (entry.equalsIgnoreCase("endTime")) {
                result.append("se.timeStamp <= :endTime ");
            }
            if (entries.hasNext()) {
                result.append("AND ");
            }
        }
        return result.toString();
    }
    
    public static UUID getMonitorModelID() {
        return MonitorModel.monitorModelID;
    }
    
    public void setPersistence(final boolean turnOn) {
        synchronized (MonitorModel.persistChangeLock) {
            MonitorModel.persistMonitor = (turnOn && Server.persistenceIsEnabled());
            this.initPersistence();
        }
    }
    
    private static Map<MonitorData.Key, Map<String, Object>> getCurrentValues(final Set<MonitorData.Key> keysToGet) {
        final Map<MonitorData.Key, Map<String, Object>> result = new HashMap<MonitorData.Key, Map<String, Object>>();
        for (final MonitorData.Key key : keysToGet) {
            final MonitorData md = MonitorModel.monitorDataMap.get(key);
            if (md != null) {
                result.put(key, md.getValues());
            }
        }
        return result;
    }
    
    private static void addCurrentMonitorValues(final Map<MonitorData.Key, MonitorData> values) {
        for (final Map.Entry<MonitorData.Key, MonitorData> entry : values.entrySet()) {
            MonitorData currData = MonitorModel.monitorDataMap.get(entry.getKey());
            if (currData == null) {
                currData = new MonitorData(entry.getKey().entityID, entry.getKey().serverID);
            }
            currData.addLatest(entry.getValue());
            MonitorModel.monitorDataMap.put(entry.getKey(), currData);
        }
    }
    
    private MonitorModel() {
        this.executor = Server.getServer().getScheduler();
        this.mdRepository = MetadataRepository.getINSTANCE();
        this.currentHealthCreateTime = 0L;
        this.currentHealthReportBuilder = new HealthRecordBuilder();
        this.metaDataDBDetails = new MetaDataDBDetails();
        this.setPersistence(monitorPersistenceIsEnabled());
        final WALoader loader = WALoader.get();
        try {
            loader.loadClass("com.datasphere.runtime.monitor.MonitorBatchEvent");
        }
        catch (ClassNotFoundException e) {
            MonitorModel.logger.error((Object)"Could not load class", (Throwable)e);
        }
        this.loadCacheFromDb();
        HazelcastSingleton.get().getMap("MonitorModelToServerMap").put((Object)"MonitorModel", (Object)Server.getServer().getServerID());
        HazelcastSingleton.get().getCluster().addMembershipListener((MembershipListener)new MembershipListener() {
            public void memberRemoved(final MembershipEvent arg0) {
                final UUID objectID = new UUID(arg0.getMember().getUuid());
                MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToHumanReadableString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName(), "SERVER", "PRESENT", "ABSENT", System.currentTimeMillis()));
            }
            
            public void memberAttributeChanged(final MemberAttributeEvent arg0) {
                final UUID objectID = new UUID(arg0.getMember().getUuid());
                MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToHumanReadableString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName(), "SERVER", "NOT KNOWN", arg0.getValue().toString(), System.currentTimeMillis()));
            }
            
            public void memberAdded(final MembershipEvent arg0) {
                final UUID objectID = new UUID(arg0.getMember().getUuid());
                MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToHumanReadableString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName(), "SERVER", "ABSENT", "PRESENT", System.currentTimeMillis()));
            }
        });
        HazelcastSingleton.get().getClientService().addClientListener((ClientListener)new ClientListener() {
            public void clientConnected(final Client arg0) {
                try {
                    final UUID objectID = new UUID(arg0.getUuid());
                    final MetaInfo.Server removedAgent = (MetaInfo.Server)MonitorModel.this.mdRepository.getMetaObjectByUUID(objectID, HSecurityManager.TOKEN);
                    if (removedAgent != null && removedAgent.isAgent) {
                        MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToHumanReadableString(arg0.getUuid(), arg0.getSocketAddress().toString(), "AGENT", "ABSENT", "PRESENT", System.currentTimeMillis()));
                    }
                }
                catch (MetaDataRepositoryException e) {
                    MonitorModel.logger.warn((Object)e.getMessage(), (Throwable)e);
                }
            }
            
            public void clientDisconnected(final Client arg0) {
                try {
                    final UUID objectID = new UUID(arg0.getUuid());
                    final MetaInfo.Server removedAgent = (MetaInfo.Server)MonitorModel.this.mdRepository.getMetaObjectByUUID(objectID, HSecurityManager.TOKEN);
                    if (removedAgent != null && removedAgent.isAgent) {
                        MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToHumanReadableString(arg0.getUuid(), arg0.getSocketAddress().toString(), "AGENT", "PRESENT", "ABSENT", System.currentTimeMillis()));
                    }
                }
                catch (MetaDataRepositoryException e) {
                    MonitorModel.logger.warn((Object)e.getMessage(), (Throwable)e);
                }
            }
        });
        this.executor.scheduleWithFixedDelay(MonitorModel.kafkaMonitor, 10000L, 10000L, TimeUnit.MILLISECONDS);
        this.executor.scheduleWithFixedDelay(MonitorModel.elasticsearchMonitor, 10000L, 10000L, TimeUnit.MILLISECONDS);
        MonitorModel.uuidMap.put(MonitorModel.ES_ENTITY_UUID, "ES_ENTITY");
        MonitorModel.uuidMap.put(MonitorModel.ES_SERVER_UUID, "ES_SERVER");
        MonitorModel.uuidMap.put(MonitorModel.KAFKA_ENTITY_UUID, "KAFKA_ENTITY");
        MonitorModel.uuidMap.put(MonitorModel.KAFKA_SERVER_UUID, "KAFKA_SERVER");
        MonitorModel.uuidMap.put(MonitorModel.MonitorModelID, "MONITOR_MODEL");
        MonitorModel.uuidMap.put(MonitorEvent.RollupUUID, "ROLLUP");
    }
    
    private void initPersistence() {
        synchronized (MonitorModel.persistChangeLock) {
            try {
                if (MonitorModel.persistMonitor) {
                    MonitorModel.logger.info((Object)"PersistMonitor is true");
                    final Map<String, Object> properties = new HashMap<String, Object>();
                    properties.put("elasticsearch.time_to_live", Integer.toString(getDbExpireTime()));
                    properties.put("elasticsearch.refresh_interval", "30s");
                    properties.put("elasticsearch.flush_size", "1gb");
                    properties.put("elasticsearch.merge_num_threads", "1");
                    if (MonitorModel.monitorModelPersistedHDStore == null) {
                        MonitorModel.monitorModelPersistedHDStore = HDStores.getInstance(properties);
                        MonitorModel.monitorEventDataType = InternalType.MONITORING.getDataType(MonitorModel.monitorModelPersistedHDStore, properties);
                    }
                }
                if (MonitorModel.persistHealth) {
                    MonitorModel.logger.info((Object)"Persist health is true");
                    final Map<String, Object> properties = createPropertiesForHealthES();
                    initHealthStore(properties);
                }
            }
            catch (Throwable e) {
                MonitorModel.logger.error((Object)("Failed to initialize persistence with exception " + e));
            }
        }
    }
    
    public static Map<String, Object> createPropertiesForHealthES() {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("elasticsearch.time_to_live", Integer.toString(getDbExpireTime()));
        properties.put("elasticsearch.refresh_interval", "30s");
        properties.put("elasticsearch.flush_size", "1gb");
        properties.put("elasticsearch.merge_num_threads", "1");
        return properties;
    }
    
    public static void initHealthStore(final Map<String, Object> properties) {
        if (MonitorModel.healthRecordHDStore == null || MonitorModel.healthRecordDataType == null) {
            MonitorModel.healthRecordHDStore = HDStores.getInstance(properties);
            MonitorModel.healthRecordDataType = InternalType.HEALTH.getDataType(MonitorModel.healthRecordHDStore, properties);
        }
    }
    
    public static DataType getHealthRecordDataType() {
        if (MonitorModel.healthRecordDataType == null) {
            final Map<String, Object> properties = createPropertiesForHealthES();
            initHealthStore(properties);
        }
        return MonitorModel.healthRecordDataType;
    }
    
    public static HDStoreManager getHealthRecordDataStore() {
        if (MonitorModel.healthRecordHDStore == null) {
            final Map<String, Object> properties = createPropertiesForHealthES();
            initHealthStore(properties);
        }
        return MonitorModel.healthRecordHDStore;
    }
    
    private void loadCacheFromDb() {
        final long startTime = System.currentTimeMillis() - MonitorData.HISTORY_EXPIRE_TIME;
        final List<MonitorEvent> olderEvents = this.getDbMonitorEvents(null, null, startTime, System.currentTimeMillis(), null, "ASC", 10000);
        MonitorModel.logger.info((Object)("loading " + olderEvents.size() + " number of events from cache"));
        writeToCache(olderEvents);
    }
    
    @Override
    public void run() {
    }
    
    private static Map<Pair<String, UUID>, Map<UUID, String>> getAllAppDetails() throws MetaDataRepositoryException {
        final Map<Pair<String, UUID>, Map<UUID, String>> allAppUUIDs = new HashMap<Pair<String, UUID>, Map<UUID, String>>();
        final Set<MetaInfo.Flow> allApps = (Set<MetaInfo.Flow>)MonitorModel.mdLocalCache.getByEntityType(EntityType.APPLICATION);
        for (final MetaInfo.Flow app : allApps) {
            final Map<EntityType, LinkedHashSet<UUID>> appE = app.getObjects();
            if (!appE.isEmpty()) {
                final Pair<String, UUID> key = Pair.make(app.nsName + "." + app.name, app.uuid);
                final Map<UUID, String> appUUIDs = new LinkedHashMap<UUID, String>();
                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> appList : appE.entrySet()) {
                    for (final UUID uuid : appList.getValue()) {
                        final MetaInfo.MetaObject subObj = MonitorModel.mdLocalCache.getMetaObjectByUUID(uuid);
                        if (subObj != null) {
                            appUUIDs.put(uuid, subObj.nsName + "." + subObj.name);
                        }
                    }
                    if (appList.getKey().equals(EntityType.FLOW)) {
                        for (final UUID flowUUID : appList.getValue()) {
                            final MetaInfo.Flow flow = (MetaInfo.Flow)MonitorModel.mdLocalCache.getMetaObjectByUUID(flowUUID);
                            if (flow != null) {
                                final Map<EntityType, LinkedHashSet<UUID>> flowE = flow.getObjects();
                                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> flowList : flowE.entrySet()) {
                                    for (final UUID uuid2 : flowList.getValue()) {
                                        final MetaInfo.MetaObject subObj2 = MonitorModel.mdLocalCache.getMetaObjectByUUID(uuid2);
                                        if (subObj2 != null) {
                                            appUUIDs.put(uuid2, subObj2.nsName + "." + subObj2.name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                allAppUUIDs.put(key, appUUIDs);
            }
        }
        return allAppUUIDs;
    }
    
    private static UUID getContainingAppID(final Map<Pair<String, UUID>, Map<UUID, String>> allApps, final UUID entityID) {
        for (final Map.Entry<Pair<String, UUID>, Map<UUID, String>> entry : allApps.entrySet()) {
            if (entry.getValue().containsKey(entityID)) {
                return entry.getKey().second;
            }
        }
        return null;
    }
    
    private static String getEntityName(final Map<Pair<String, UUID>, Map<UUID, String>> allApps, final UUID appID, final UUID entityID) {
        for (final Map.Entry<Pair<String, UUID>, Map<UUID, String>> entry : allApps.entrySet()) {
            if (entry.getKey().second.equals((Object)appID)) {
                return entry.getValue().get(entityID);
            }
        }
        return null;
    }
    
    private static List<MonitorEvent> rollupComponentData(final Map<UUID, MetaInfo.Server> allServers, final Map<Pair<String, UUID>, Map<UUID, String>> allApps, final List<MonitorEvent> rawEvents) {
        final List<MonitorEvent> componentRollupEvents = new ArrayList<MonitorEvent>();
        if (rawEvents.isEmpty()) {
            return componentRollupEvents;
        }
        final Set<MonitorData.Key> keysToGet = new HashSet<MonitorData.Key>();
        for (final MonitorEvent monEvent : rawEvents) {
            for (final UUID serverID : allServers.keySet()) {
                if (monEvent.serverID.equals((Object)serverID)) {
                    continue;
                }
                keysToGet.add(new MonitorData.Key(monEvent.entityID, serverID));
            }
        }
        Map<MonitorData.Key, Map<String, Object>> currentData = null;
        Map<UUID, Set<UUID>> entityServerMap = null;
        if (!keysToGet.isEmpty()) {
            currentData = getCurrentValues(keysToGet);
            if (!currentData.isEmpty()) {
                entityServerMap = new HashMap<UUID, Set<UUID>>();
                for (final MonitorData.Key key : currentData.keySet()) {
                    Set<UUID> serverSet = entityServerMap.get(key.entityID);
                    if (serverSet == null) {
                        serverSet = new HashSet<UUID>();
                        entityServerMap.put(key.entityID, serverSet);
                    }
                    serverSet.add(key.serverID);
                }
            }
        }
        Long ts = null;
        for (final MonitorEvent monEvent2 : rawEvents) {
            if (monEvent2.type == MonitorEvent.Type.STATUS_CHANGE) {
                continue;
            }
            if (monEvent2.type == MonitorEvent.Type.KAFKA_BROKERS) {
                continue;
            }
            if (monEvent2.entityID.equals((Object)monEvent2.serverID)) {
                continue;
            }
            if (ts == null) {
                ts = monEvent2.timeStamp;
            }
            final MonitorEvent rollupEvent = new MonitorEvent(MonitorEvent.RollupUUID, monEvent2.entityID, monEvent2.type, Long.valueOf(0L), Long.valueOf(monEvent2.timeStamp));
            rollupEvent.valueLong = null;
            List<Pair<String, String>> stringRollups = null;
            Long longRollups = null;
            final MetaInfo.Server monEventServer = allServers.get(monEvent2.serverID);
            if (monEventServer == null) {
                continue;
            }
            if (monEvent2.valueString != null) {
                stringRollups = new ArrayList<Pair<String, String>>();
                stringRollups.add(Pair.make(monEventServer.name, monEvent2.valueString));
            }
            else {
                longRollups = monEvent2.valueLong;
            }
            if (entityServerMap != null) {
                final Set<UUID> serversForEntity = entityServerMap.get(monEvent2.entityID);
                if (serversForEntity != null) {
                    for (final UUID serverID2 : serversForEntity) {
                        if (MonitorEvent.RollupUUID.equals((Object)serverID2)) {
                            continue;
                        }
                        if (monEvent2.serverID.equals((Object)serverID2)) {
                            continue;
                        }
                        final MonitorData.Key key2 = new MonitorData.Key(monEvent2.entityID, serverID2);
                        final Map<String, Object> serverCurrentMap = currentData.get(key2);
                        if (serverCurrentMap == null) {
                            continue;
                        }
                        final MetaInfo.Server otherEventServer = allServers.get(serverID2);
                        final Object value = serverCurrentMap.get(monEvent2.type.name());
                        if (value == null || otherEventServer == null) {
                            continue;
                        }
                        if (value instanceof String) {
                            stringRollups.add(Pair.make(otherEventServer.name, (String)value));
                        }
                        else {
                            if (!(value instanceof Long)) {
                                continue;
                            }
                            if (monEvent2.operation.equals(MonitorEvent.Operation.MAX)) {
                                longRollups = Math.max(longRollups, (long)value);
                            }
                            if (isTimeType(monEvent2.type)) {
                                longRollups = Math.max(longRollups, (long)value);
                            }
                            else {
                                longRollups += (long)value;
                            }
                        }
                    }
                }
            }
            if (stringRollups != null) {
                
                Collections.sort(stringRollups, (o1, o2) -> {
                		final int c1 = ((String)o1.first).compareTo((String)o2.first);
                    if (c1 != 0) {
                        return c1;
                    }
                    else {
                        return ((String)o1.second).compareTo((String)o2.second);
                    }
                });
                String stringVal = null;
                Double d = 0.0;
                for (final Pair<String, String> pair : stringRollups) {
                    if (monEvent2.operation.equals(MonitorEvent.Operation.SUM)) {
                        d += Double.parseDouble(pair.second);
                    }
                    else if (monEvent2.operation.equals(MonitorEvent.Operation.AVG)) {
                        d += Double.parseDouble(pair.second);
                    }
                    else {
                        String pairVal = pair.first + ":" + pair.second;
                        if (Utility.isValidJson(pair.second)) {
                            pairVal = "{\"" + pair.first + "\":" + pair.second + "}";
                            stringVal = ((stringVal == null) ? pairVal : Utility.addJsonStrings(stringVal, pairVal));
                        }
                        else {
                            stringVal = ((stringVal == null) ? pairVal : (stringVal + "," + pairVal));
                        }
                    }
                }
                if (monEvent2.operation.equals(MonitorEvent.Operation.SUM)) {
                    rollupEvent.valueString = MonitorModel.decimalFormat.format(d);
                }
                else if (monEvent2.operation.equals(MonitorEvent.Operation.AVG)) {
                    rollupEvent.valueString = MonitorModel.decimalFormat.format(d / stringRollups.size());
                }
                else {
                    rollupEvent.valueString = stringVal;
                }
            }
            else if (longRollups != null) {
                rollupEvent.valueLong = longRollups;
            }
            componentRollupEvents.add(rollupEvent);
        }
        return componentRollupEvents;
    }
    
    private static boolean isTimeType(final MonitorEvent.Type type) {
        return type == MonitorEvent.Type.LATEST_ACTIVITY || type == MonitorEvent.Type.CACHE_REFRESH || type == MonitorEvent.Type.LAST_COMMIT_TIME || type == MonitorEvent.Type.ORA_READER_LAST_OBSERVED_TIMESTAMP || type == MonitorEvent.Type.LAST_IO_TIME;
    }
    
    private static List<MonitorEvent> createAppRollupEvents(final Map<UUID, MetaInfo.Server> allServers, final Map<Pair<String, UUID>, Map<UUID, String>> allApps, final List<MonitorEvent> rawEvents) {
        final BuiltInFunc.LagRateAverageCalculator avgLong = new BuiltInFunc.LagRateAverageCalculator();
        final List<MonitorEvent> appRollupEvents = new ArrayList<MonitorEvent>();
        if (rawEvents.isEmpty()) {
            return appRollupEvents;
        }
        final Map<UUID, String> entityThreads = new HashMap<UUID, String>();
        UUID serverID = null;
        Long ts = null;
        for (final MonitorEvent monEvent : rawEvents) {
            if (serverID == null) {
                serverID = monEvent.serverID;
            }
            if (ts == null) {
                ts = monEvent.timeStamp;
            }
            if (monEvent.type.equals(MonitorEvent.Type.CPU_THREAD)) {
                if (!MonitorEvent.RollupUUID.equals((Object)serverID)) {
                    final MetaInfo.Server s = allServers.get(serverID);
                    if (s == null) {
                        continue;
                    }
                    entityThreads.put(monEvent.entityID, s.name + ":" + monEvent.valueString);
                }
                else {
                    entityThreads.put(monEvent.entityID, monEvent.valueString);
                }
            }
        }
        final Set<String> seenThreads = new HashSet<String>();
        final Map<UUID, Map<MonitorEvent.Type, Object>> currentAppValues = new HashMap<UUID, Map<MonitorEvent.Type, Object>>();
        for (final Pair<String, UUID> pair : allApps.keySet()) {
            currentAppValues.put(pair.second, new HashMap<MonitorEvent.Type, Object>());
        }
        for (final MonitorEvent monEvent2 : rawEvents) {
            final UUID containingAppID = getContainingAppID(allApps, monEvent2.entityID);
            if (containingAppID == null) {
                continue;
            }
            final String entityName = getEntityName(allApps, containingAppID, monEvent2.entityID);
            if (entityName == null) {
                continue;
            }
            if (monEvent2.type.equals(MonitorEvent.Type.CPU_RATE)) {
                final String thread = entityThreads.get(monEvent2.entityID);
                if (thread != null) {
                    String[] threads;
                    if (MonitorEvent.RollupUUID.equals((Object)serverID)) {
                        threads = thread.split("[,]");
                    }
                    else {
                        threads = new String[] { thread };
                    }
                    boolean seen = false;
                    for (final String aThread : threads) {
                        if (seenThreads.contains(aThread)) {
                            seen = true;
                            break;
                        }
                    }
                    if (seen) {
                        continue;
                    }
                    seenThreads.add(thread);
                }
            }
            Map<MonitorEvent.Type, Object> currentAppData = currentAppValues.get(containingAppID);
            if (currentAppData == null) {
                currentAppData = new HashMap<MonitorEvent.Type, Object>();
                currentAppValues.put(containingAppID, currentAppData);
            }
            Object currValue = currentAppData.get(monEvent2.type);
            try {
                if (monEvent2.valueString != null) {
                    if (currValue == null) {
                        currValue = monEvent2.valueString;
                    }
                    else if (!((String)currValue).contains(monEvent2.valueString)) {
                        currValue = currValue + "," + monEvent2.valueString;
                    }
                }
                else if (isTimeType(monEvent2.type)) {
                    currValue = ((currValue == null) ? monEvent2.valueLong : Math.max((long)currValue, monEvent2.valueLong));
                }
                else if (monEvent2.type == MonitorEvent.Type.LAG_RATE) {
                    avgLong.incAggValue(monEvent2.valueLong);
                    currValue = avgLong.getAggValue();
                }
                else {
                    currValue = ((currValue == null) ? monEvent2.valueLong : ((long)currValue + monEvent2.valueLong));
                }
            }
            catch (Throwable t) {
                MonitorModel.logger.error((Object)("Problem obtaining current value from " + monEvent2), t);
            }
            currentAppData.put(monEvent2.type, currValue);
        }
        for (final Map.Entry<UUID, Map<MonitorEvent.Type, Object>> appEntry : currentAppValues.entrySet()) {
            final UUID appId = appEntry.getKey();
            for (final Map.Entry<MonitorEvent.Type, Object> eventEntry : appEntry.getValue().entrySet()) {
                final MonitorEvent appEvent = new MonitorEvent(serverID, appId, eventEntry.getKey(), Long.valueOf(0L), ts);
                final Object value = eventEntry.getValue();
                appEvent.valueLong = ((value instanceof Long) ? ((Long)value) : null);
                appEvent.valueString = ((value instanceof String) ? ((String)value) : null);
                appRollupEvents.add(appEvent);
            }
        }
        return appRollupEvents;
    }
    
    private static List<MonitorEvent> createElasticsearchRollupEvents(final List<MonitorEvent> rawEvents) {
        final List<MonitorEvent> serverRollupEvents = new ArrayList<MonitorEvent>();
        if (rawEvents.isEmpty()) {
            return serverRollupEvents;
        }
        Long txTimeStamp = null;
        Long rxTimeStamp = null;
        final StringBuilder hotThreads = new StringBuilder();
        for (final MonitorEvent e : rawEvents) {
            if (e.type == MonitorEvent.Type.ES_TX_BYTES) {
                MonitorModel.serverTxMap.put(e.serverID, e.valueLong);
                txTimeStamp = e.timeStamp;
            }
            else if (e.type == MonitorEvent.Type.ES_RX_BYTES) {
                MonitorModel.serverRxMap.put(e.serverID, e.valueLong);
                rxTimeStamp = e.timeStamp;
            }
            else {
                if (e.type != MonitorEvent.Type.ES_HOT_THREADS) {
                    continue;
                }
                hotThreads.append(e.valueString);
                txTimeStamp = e.getTimeStamp();
            }
        }
        if (txTimeStamp != null) {
            long value = 0L;
            for (final Long a : MonitorModel.serverTxMap.values()) {
                value += a;
            }
            serverRollupEvents.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_TX_BYTES, Long.valueOf(value), txTimeStamp));
        }
        if (rxTimeStamp != null) {
            long value = 0L;
            for (final Long a : MonitorModel.serverRxMap.values()) {
                value += a;
            }
            serverRollupEvents.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_RX_BYTES, Long.valueOf(value), txTimeStamp));
        }
        if (!hotThreads.isEmpty()) {
            final String value2 = hotThreads.toString();
            serverRollupEvents.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_HOT_THREADS, value2, txTimeStamp));
        }
        return serverRollupEvents;
    }
    
    private static List<MonitorEvent> createServerRollupEvents(final List<MonitorEvent> rawEvents) {
        final List<MonitorEvent> serverRollupEvents = new ArrayList<MonitorEvent>();
        if (rawEvents.isEmpty()) {
            return serverRollupEvents;
        }
        UUID serverID = null;
        Long ts = null;
        final Map<MonitorEvent.Type, Object> currentServerData = new HashMap<MonitorEvent.Type, Object>();
        for (final MonitorEvent monEvent : rawEvents) {
            if (serverID == null) {
                serverID = monEvent.serverID;
            }
            if (ts == null) {
                ts = monEvent.timeStamp;
            }
            if (monEvent.type == MonitorEvent.Type.STATUS_CHANGE) {
                continue;
            }
            if (monEvent.type == MonitorEvent.Type.CPU_RATE) {
                continue;
            }
            if (monEvent.type == MonitorEvent.Type.CORES) {
                try {
                    final Set<MetaInfo.Server> servers = MonitorModel.mdLocalCache.getServerSet();
                    final long totalCpus = Server.server.getClusterPhysicalCoreCount(servers);
                    currentServerData.put(monEvent.type, totalCpus);
                    continue;
                }
                catch (Exception e) {
                    MonitorModel.logger.warn((Object)("Could not do smart rollup of CORES, resorting to simple rollup: " + e.getMessage()));
                }
            }
            Object currValue = currentServerData.get(monEvent.type);
            try {
                if (monEvent.valueString != null) {
                    if (currValue == null) {
                        currValue = monEvent.valueString;
                    }
                    else if (!((String)currValue).contains(monEvent.valueString)) {
                        currValue = currValue + "," + monEvent.valueString;
                    }
                }
                else if (isTimeType(monEvent.type)) {
                    currValue = ((currValue == null) ? monEvent.valueLong : Math.max((long)currValue, monEvent.valueLong));
                }
                else {
                    currValue = ((currValue == null) ? monEvent.valueLong : ((long)currValue + monEvent.valueLong));
                }
            }
            catch (Throwable t) {
                MonitorModel.logger.error((Object)("Problem obtaining current value from " + monEvent), t);
            }
            currentServerData.put(monEvent.type, currValue);
        }
        for (final Map.Entry<MonitorEvent.Type, Object> eventEntry : currentServerData.entrySet()) {
            final MonitorEvent appEvent = new MonitorEvent(serverID, serverID, eventEntry.getKey(), Long.valueOf(0L), ts);
            final Object value = eventEntry.getValue();
            appEvent.valueLong = ((value instanceof Long) ? ((Long)value) : null);
            appEvent.valueString = ((value instanceof String) ? ((String)value) : null);
            serverRollupEvents.add(appEvent);
        }
        return serverRollupEvents;
    }
    
    private static List<MonitorEvent> augmentWithRollupEvents(final List<MonitorEvent> events) throws MetaDataRepositoryException {
        final Map<Pair<String, UUID>, Map<UUID, String>> allApps = getAllAppDetails();
        final Map<UUID, MetaInfo.Server> allServers = MonitorModel.mdLocalCache.getServerMap();
        final List<MonitorEvent> allRollupEvents = new ArrayList<MonitorEvent>();
        final List<MonitorEvent> componentRollupEvents = rollupComponentData(allServers, allApps, events);
        final List<MonitorEvent> appByServerRollupEvents = createAppRollupEvents(allServers, allApps, events);
        final List<MonitorEvent> appRollupEvents = createAppRollupEvents(allServers, allApps, componentRollupEvents);
        final List<MonitorEvent> serverRollupEvents = createServerRollupEvents(events);
        final List<MonitorEvent> elasticsearchRollupEvents = createElasticsearchRollupEvents(events);
        allRollupEvents.addAll(events);
        allRollupEvents.addAll(componentRollupEvents);
        allRollupEvents.addAll(appByServerRollupEvents);
        allRollupEvents.addAll(appRollupEvents);
        allRollupEvents.addAll(serverRollupEvents);
        allRollupEvents.addAll(elasticsearchRollupEvents);
        return allRollupEvents;
    }
    
    public static MonitorBatchEvent processBatch(final MonitorBatchEvent monEventBatch) throws Exception {
        try {
            if (getOrCreateInstance() == null) {
                return null;
            }
            if (monEventBatch.events == null) {
                return null;
            }
            final List<MonitorEvent> removethese = new ArrayList<MonitorEvent>();
            final List<MonitorEvent> removeAdapterDetailEvents = new ArrayList<MonitorEvent>();
            for (final MonitorEvent event : monEventBatch.events) {
                if (event.type == MonitorEvent.Type.KAFKA_BROKERS) {
                    MonitorModel.kafkaMonitor.addKafkaBrokers(event.valueString);
                    removethese.add(event);
                }
                else {
                    if (event.type != MonitorEvent.Type.ADAPTER_DETAIL) {
                        continue;
                    }
                    removeAdapterDetailEvents.add(event);
                }
            }
            monEventBatch.events.removeAll(removethese);
            List<MonitorEvent> events = monEventBatch.events;
            events = augmentWithRollupEvents(events);
            final String jmxConfig = System.getProperty("com.dss.jmx.enabled", "false");
            final Boolean isJmxEnabled = Boolean.valueOf(jmxConfig);
            if (isJmxEnabled) {
                updateDSSMBeansMetric(events);
            }
            monEventBatch.events.remove(removeAdapterDetailEvents);
            final MonitorBatchEvent batchEvent = new MonitorBatchEvent(System.currentTimeMillis(), events);
            writeToCache(events);
            synchronized (MonitorModel.persistChangeLock) {
                if (MonitorModel.persistMonitor) {
                    try {
                        persistInES(events);
                        if (MonitorModel.isFailedToInsertFlag) {
                            MonitorModel.logger.error((Object)"Successfully inserted monitoring events in elasticsearch after the error");
                            MonitorModel.isFailedToInsertFlag = false;
                        }
                    }
                    catch (Throwable e) {
                        if (!MonitorModel.isFailedToInsertFlag) {
                            MonitorModel.logger.error((Object)("Failed to insert monitoring events in elasticsearch with error " + e.getMessage()));
                            MonitorModel.isFailedToInsertFlag = true;
                        }
                    }
                }
            }
            final HealthRecord healthRecord = MonitorModel.instance.updateHealthReport(events);
            if (healthRecord != null && isJmxEnabled) {
                updateHealthReportMBean(healthRecord);
            }
            return batchEvent;
        }
        catch (Throwable e2) {
            MonitorModel.logger.error((Object)("Failed to process monitoring batch event with exception " + e2.getMessage()));
            return monEventBatch;
        }
    }
    
    private static void updateDSSMBeansMetric(final List<MonitorEvent> events) throws MetaDataRepositoryException, DatallException {
        final MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
        final AuthToken sessionID = HSecurityManager.TOKEN;
        final Map<UUID, MetaInfo.Server> serverMap = MonitorModel.mdLocalCache.getServerMap();
        final Map<UUID, String> serverIdNameMap = new HashMap<UUID, String>();
        serverIdNameMap.putAll(MonitorModel.uuidMap);
        for (final UUID serverUUID : serverMap.keySet()) {
            serverIdNameMap.put(serverUUID, serverMap.get(serverUUID).getName());
        }
        for (final MonitorEvent event : events) {
            final String serverName = serverIdNameMap.get(event.getServerID());
            String entityName = event.entityID.toString();
            final MetaInfo.MetaObject metaObject = metadataRepository.getMetaObjectByUUID(event.entityID, sessionID);
            if (metaObject != null) {
                entityName = metaObject.getName();
            }
            final String metricName = serverName + "." + entityName;
            final MainMBean bean = getDSSMBeanForMetricName(metricName);
            bean.updateMetric(event.type.toString(), (event.valueLong == null) ? event.valueString : event.valueLong);
        }
    }
    
    private static void updateHealthReportMBean(final HealthRecord healthRecord) throws DatallException {
        final ClusterInfo clusterInfo = getOrCreateClusterInfoMBean();
        clusterInfo.updateBean(healthRecord.getId(), healthRecord.getStartTime(), healthRecord.getEndTime(), healthRecord.getClusterSize(), healthRecord.isDerbyAlive(), healthRecord.getElasticSearch(), healthRecord.getAgentCount());
        final Map<String, AppHealth> appHealthMap = healthRecord.getAppHealthMap();
        for (final Map.Entry<String, AppHealth> entry : appHealthMap.entrySet()) {
            final AppHealth appHealth = getOrCreateAppHealthMBean(entry.getKey());
            appHealth.update(entry.getValue().getFqAppName(), entry.getValue().getStatus(), entry.getValue().getLastModifiedTime());
        }
        final Map<String, CacheHealth> cacheHealthMap = healthRecord.getCacheHealthMap();
        for (final Map.Entry<String, CacheHealth> entry2 : cacheHealthMap.entrySet()) {
            final CacheHealth cacheHealth = getOrCreateCacheHealthMBean(entry2.getKey());
            cacheHealth.update(entry2.getValue().getSize(), entry2.getValue().getLastRefresh(), entry2.getValue().getFqCacheName());
        }
        final Map<String, ServerHealth> serverHealthMap = healthRecord.getServerHealthMap();
        for (final Map.Entry<String, ServerHealth> entry3 : serverHealthMap.entrySet()) {
            final ServerHealth serverHealth = getOrCreateServerHealthMBean(entry3.getKey());
            serverHealth.update(entry3.getValue().getCpu(), entry3.getValue().getFqServerName(), entry3.getValue().getMemory(), entry3.getValue().getDiskFree(), entry3.getValue().getElasticsearchFree());
        }
        final Map<String, SourceHealth> sourceHealthMap = healthRecord.getSourceHealthMap();
        for (final Map.Entry<String, SourceHealth> entry4 : sourceHealthMap.entrySet()) {
            final SourceHealth sourceHealth = getOrCreateSourceHealthMBean(entry4.getKey());
            sourceHealth.update(entry4.getValue().getFqSourceName(), entry4.getValue().getLastEventTime(), entry4.getValue().getEventRate());
        }
        final Map<String, TargetHealth> targetHealthMap = healthRecord.getTargetHealthMap();
        for (final Map.Entry<String, TargetHealth> entry5 : targetHealthMap.entrySet()) {
            final TargetHealth targetHealth = getOrCreateTargetHealthMBean(entry5.getKey());
            targetHealth.update(entry5.getValue().getFqTargetName(), entry5.getValue().getLastWriteTime(), entry5.getValue().getEventRate());
        }
        final Map<String, HStoreHealth> waStoreHealthMap = healthRecord.getWaStoreHealthMap();
        for (final Map.Entry<String, HStoreHealth> entry6 : waStoreHealthMap.entrySet()) {
            final HStoreHealth waStoreHealth = getOrCreateWAStoreHealthMBean(entry6.getKey());
            waStoreHealth.update(entry6.getValue().getFqWAStoreName(), entry6.getValue().getLastWriteTime(), entry6.getValue().getWriteRate());
        }
        final List<StateChange> stateChangeList = healthRecord.getStateChangeList();
        for (final StateChange entry7 : stateChangeList) {
            final StateChange stateChange = getorCreateStateChangeBean(entry7.getFqName() + "." + entry7.getTimestamp());
            stateChange.update(entry7.getFqName(), entry7.getType(), entry7.getPreviousStatus(), entry7.getCurrentStatus(), entry7.getTimestamp());
        }
        final List<Issue> issuesList = healthRecord.getIssuesList();
        for (final Issue entry8 : issuesList) {
            final Issue issueBean = getIssueBean(entry8.getFqName());
            issueBean.update(entry8.getFqName(), entry8.getComponentType(), entry8.getIssue());
        }
    }
    
    private static Issue getIssueBean(final String appName) throws DatallException {
        final String beanName = "Issue." + appName;
        Issue bean = (Issue)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new Issue("", "", "");
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "IssueReport", appName);
        }
        return bean;
    }
    
    private static StateChange getorCreateStateChangeBean(final String appName) throws DatallException {
        StateChange bean = (StateChange)MonitorModel.beanMap.get(appName);
        if (bean == null) {
            bean = new StateChange("", "", "", "", 0L);
            MonitorModel.beanMap.put(appName, bean);
            registerMBean(bean, "StateChangeReport", appName);
        }
        return bean;
    }
    
    private static HStoreHealth getOrCreateWAStoreHealthMBean(final String appName) throws DatallException {
        final String beanName = "WAStore." + appName;
        HStoreHealth bean = (HStoreHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new HStoreHealth("", 0L, 0L);
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static TargetHealth getOrCreateTargetHealthMBean(final String appName) throws DatallException {
        final String beanName = "Target." + appName;
        TargetHealth bean = (TargetHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new TargetHealth("", 0L, 0L);
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static SourceHealth getOrCreateSourceHealthMBean(final String appName) throws DatallException {
        final String beanName = "Source." + appName;
        SourceHealth bean = (SourceHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new SourceHealth("", 0L, 0L);
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static ServerHealth getOrCreateServerHealthMBean(final String appName) throws DatallException {
        final String beanName = "Server." + appName;
        ServerHealth bean = (ServerHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new ServerHealth("", "", 0L, "", "");
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static CacheHealth getOrCreateCacheHealthMBean(final String appName) throws DatallException {
        final String beanName = "Cache." + appName;
        CacheHealth bean = (CacheHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new CacheHealth(0L, 0L, "");
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static AppHealth getOrCreateAppHealthMBean(final String appName) throws DatallException {
        final String beanName = "App." + appName;
        AppHealth bean = (AppHealth)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new AppHealth("", "", -1L);
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static ClusterInfo getOrCreateClusterInfoMBean() throws DatallException {
        final String beanName = "ClusterInfoMBean";
        ClusterInfo bean = (ClusterInfo)MonitorModel.beanMap.get(beanName);
        if (bean == null) {
            bean = new ClusterInfo();
            MonitorModel.beanMap.put(beanName, bean);
            registerMBean(bean, "HealthReport", beanName);
        }
        return bean;
    }
    
    private static void registerMBean(final Object bean, final String type, final String beanName) throws DatallException {
        try {
            MonitorModel.mbs.registerMBean(bean, new ObjectName("com.dss.metrics:type=" + type + ",name=" + beanName));
        }
        catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException ex2) {
            throw new DatallException((IError)CommonError.JMX_ERROR, (Throwable)ex2, "JMX", new String[0]);
        }
    }
    
    private static MainMBean getDSSMBeanForMetricName(final String metricName) throws DatallException {
        MainMBean bean = (MainMBean)MonitorModel.beanMap.get(metricName);
        if (bean == null) {
            bean = new MainMBean();
            MonitorModel.beanMap.put(metricName, bean);
            registerMBean(bean, "DSSMBean", metricName);
        }
        return bean;
    }
    
    private HealthRecord updateHealthReport(final List<MonitorEvent> monEvents) throws Exception {
        final long now = System.currentTimeMillis();
        if (this.currentHealthCreateTime == 0L) {
            this.currentHealthCreateTime = now;
            final List<MonitorEvent> gapEvents = this.getMonitorEventsSinceLastHealthReport(this.currentHealthCreateTime);
            if (gapEvents != null) {
                this.updateHealthReport(gapEvents);
            }
        }
        HealthRecord healthRecord = null;
        if (MonitorModel.persistMonitor && now > this.currentHealthCreateTime + 30000L) {
            healthRecord = this.rollHealthReport(now);
        }
        for (final MonitorEvent monEvent : monEvents) {
            try {
                MetaInfo.MetaObject mo = MonitorModel.mdLocalCache.getMetaObjectByUUID(monEvent.entityID);
                if (monEvent.type == MonitorEvent.Type.LOG_ERROR) {
                    final String fullName = (mo == null) ? monEvent.getEntityID().getUUIDString() : mo.getFullName();
                    final String typeName = (mo == null) ? "" : mo.type.name();
                    this.currentHealthReportBuilder.addIssue(fullName, typeName, monEvent.valueString);
                }
                else if (monEvent.type == MonitorEvent.Type.STATUS_CHANGE) {
                    if (mo == null) {
                        mo = MonitorModel.mdLocalCache.getServerByUUID(monEvent.serverID);
                    }
                    final String fullName = (mo == null) ? monEvent.getEntityID().getUUIDString() : mo.getFullName();
                    final String typeName = (mo == null) ? "" : mo.type.name();
                    this.currentHealthReportBuilder.addStateChange(fullName, typeName, monEvent.timeStamp, monEvent.valueString);
                }
                else if (mo != null) {
                    this.currentHealthReportBuilder.addValue(monEvent.serverID.toString(), mo.getFullName(), mo.type, monEvent.type, monEvent.timeStamp, monEvent.valueString, monEvent.valueLong);
                }
                else if (monEvent.getEntityID() == MonitorModel.KAFKA_ENTITY_UUID) {
                    final HealthRecordBuilder currentHealthReportBuilder = this.currentHealthReportBuilder;
                    final String string = monEvent.serverID.toString();
                    final String fqName = "Kafka Cluster";
                    final EntityType type = EntityType.TYPE;
                    currentHealthReportBuilder.addValue(string, fqName, EntityType.UNKNOWN, monEvent.type, monEvent.timeStamp, monEvent.valueString, monEvent.valueLong);
                }
                else {
                    if (!MonitorModel.logger.isDebugEnabled()) {
                        continue;
                    }
                    MonitorModel.logger.debug((Object)("Could not find meta object for " + monEvent.serverID + " " + monEvent.entityID));
                }
            }
            catch (MetaDataRepositoryException e) {
                MonitorModel.logger.warn((Object)"Error trying to look up meta object", (Throwable)e);
            }
            catch (Exception e2) {
                MonitorModel.logger.warn((Object)("Error processing event " + monEvent), (Throwable)e2);
            }
        }
        return healthRecord;
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        final MonitorBatchEvent monEventBatch = (MonitorBatchEvent)((DARecord)event.batch().first()).data;
        processBatch(monEventBatch);
    }
    
    @Override
    public void receiveImpl(final int channel, final Event event) throws Exception {
        final MonitorBatchEvent monEventBatch = (MonitorBatchEvent)event;
        processBatch(monEventBatch);
    }
    
    private Map<MonitorEvent.Type, List<Pair<Long, Object>>> getMonitorEventsAsMap(final UUID compId, final UUID serverId, final MonitorData historyData, final List<String> datumNames, final Long startTime, final Long endTime) {
        final List<MonitorEvent> monEvList = this.getMonitorEvents(compId, serverId, historyData, datumNames, startTime, endTime);
        final Map<MonitorEvent.Type, List<Pair<Long, Object>>> typedEvents = new HashMap<MonitorEvent.Type, List<Pair<Long, Object>>>();
        if (monEvList == null || monEvList.isEmpty()) {
            return typedEvents;
        }
        for (final MonitorEvent monEvent : monEvList) {
            List<Pair<Long, Object>> typedEventList = typedEvents.get(monEvent.type);
            if (typedEventList == null) {
                typedEventList = new ArrayList<Pair<Long, Object>>();
                typedEvents.put(monEvent.type, typedEventList);
            }
            typedEventList.add(Pair.make(monEvent.timeStamp, (monEvent.valueString != null) ? monEvent.valueString : monEvent.valueLong));
        }
        for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : typedEvents.entrySet()) {
            final List<Pair<Long, Object>> timeseries = entry.getValue();
            Collections.sort(timeseries, new Comparator<Pair<Long, Object>>() {
                @Override
                public int compare(final Pair<Long, Object> o1, final Pair<Long, Object> o2) {
                    return o1.first.compareTo(o2.first);
                }
            });
        }
        return typedEvents;
    }
    
    private List<MonitorEvent> getMonitorEvents(final UUID compId, final UUID serverId, final MonitorData historyData, final List<String> datumNames, final Long startTime, final Long endTime) {
        final List<MonitorEvent> orderedEvents = new ArrayList<MonitorEvent>();
        List<MonitorEvent> recentEvents = new ArrayList<MonitorEvent>();
        if (historyData != null) {
            recentEvents = historyData.getHistory();
        }
        if (startTime != null && (recentEvents.isEmpty() || recentEvents.get(0).timeStamp > startTime)) {
            final List<MonitorEvent> olderEvents = this.getDbMonitorEvents(serverId, compId, startTime, endTime, datumNames, "DESC", 1000);
            orderedEvents.addAll(olderEvents);
        }
        long newestTimestamp = 0L;
        for (final MonitorEvent med : orderedEvents) {
            if (med.timeStamp > newestTimestamp) {
                newestTimestamp = med.timeStamp;
            }
        }
        final Iterator<MonitorEvent> it = recentEvents.iterator();
        while (it.hasNext()) {
            final MonitorEvent med = it.next();
            if ((newestTimestamp != 0L && med.timeStamp <= newestTimestamp) || (startTime != null && med.timeStamp < startTime) || (endTime != null && med.timeStamp > endTime) || (datumNames != null && !datumNames.isEmpty() && !datumNames.contains(med.type.name()))) {
                it.remove();
            }
        }
        for (final MonitorEvent med2 : recentEvents) {
            orderedEvents.add(med2);
        }
        return orderedEvents;
    }
    
    private List<MonitorEvent> getDbMonitorEvents(final UUID serverID, final UUID entityID, final Long startTime, final Long endTime, final List<String> datumNames, final String desc, final int default_max_events) {
        synchronized (MonitorModel.persistChangeLock) {
            if (MonitorModel.persistMonitor) {
                return getDbMonitorEventsFromES(serverID, entityID, startTime, endTime, datumNames, desc, default_max_events);
            }
        }
        return new ArrayList<MonitorEvent>();
    }
    
    private static String listToString(final List<?> list) {
        if (list == null) {
            return "";
        }
        String ret = null;
        for (final Object obj : list) {
            ret = ((ret == null) ? obj.toString() : (ret + "," + obj));
        }
        return ret;
    }
    
    private static List<String> stringToList(final String string) {
        if (string == null) {
            return null;
        }
        if (string.trim().length() == 0) {
            return Collections.emptyList();
        }
        final String[] parts = string.split("[,]");
        return Arrays.asList(parts);
    }
    
    private static String getCompStatus(final UUID uuid) throws MetaDataRepositoryException {
        final MetaInfo.StatusInfo si = MetadataRepository.getINSTANCE().getStatusInfo(uuid, HSecurityManager.TOKEN);
        if (si != null) {
            return si.status.name();
        }
        return "UNKNOWN";
    }
    
    private static MonitorStats calculateStats(final String field, final List<Pair<Long, Object>> values) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long ave = 0L;
        long sum = 0L;
        long count = 0L;
        long first = -1L;
        long last = -1L;
        long diff = 0L;
        long resets = 0L;
        String min_s = null;
        String max_s = null;
        long trues_s = 0L;
        final StringBuilder sum_s = new StringBuilder();
        final boolean isInput = field.contains("input");
        for (final Pair<Long, Object> data : values) {
            if (data.second instanceof Long) {
                final long value = (long)data.second;
                if (first == -1L) {
                    first = value;
                }
                if (isInput && last != -1L && value < last) {
                    diff += last - first;
                    first = value;
                    ++resets;
                }
                last = value;
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
                sum += value;
                ++count;
                ave = sum / count;
            }
            else {
                final String value2 = (String)data.second;
                if (sum_s.size() > 0) {
                    sum_s.append(",");
                }
                sum_s.append(value2);
                if (value2.contains("true")) {
                    ++trues_s;
                }
                if (min_s == null) {
                    min_s = value2;
                }
                max_s = value2;
                ++count;
            }
        }
        if (last != -1L && first != -1L) {
            diff += last - first;
        }
        final MonitorStats stats = new MonitorStats();
        if (min_s != null) {
            stats.min = min_s;
            stats.max = max_s;
            stats.ave = String.valueOf(trues_s / count);
            stats.count = count;
            stats.sum = sum_s.toString();
        }
        else {
            if (min < Long.MAX_VALUE) {
                stats.min = min;
            }
            if (max > Long.MIN_VALUE) {
                stats.max = max;
            }
            stats.ave = ave;
            stats.sum = sum;
            stats.count = count;
            stats.diff = diff;
            stats.resets = resets;
        }
        return stats;
    }
    
    private void addKafkaResults(final Map<HDKey, Map<String, Object>> result) {
        final ResultKey key = new ResultKey(MonitorModel.KAFKA_ENTITY_UUID, MonitorModel.KAFKA_SERVER_UUID);
        final MonitorData historyData = MonitorModel.monitorDataMap.get(new MonitorData.Key(MonitorModel.KAFKA_ENTITY_UUID, MonitorModel.KAFKA_SERVER_UUID));
        if (historyData == null) {
            return;
        }
        final Map<String, Object> newEntResults = new HashMap<String, Object>();
        for (final MonitorEvent.Type type : MonitorEvent.KAFKA_TYPES) {
            final String typeString = type.toString();
            if (historyData.getValues().containsKey(typeString)) {
                final String s = typeString.toLowerCase().replace("_", "-");
                newEntResults.put(s, historyData.getValues().get(typeString));
            }
        }
        newEntResults.put("name", "kafka");
        newEntResults.put("id", MonitorModel.KAFKA_ENTITY_UUID);
        result.put(key, newEntResults);
    }
    
    private void addElasticsearchResults(final Map<HDKey, Map<String, Object>> result, final UUID serverId) {
        final ResultKey key = new ResultKey(MonitorModel.ES_ENTITY_UUID, serverId);
        final MonitorData historyData = MonitorModel.monitorDataMap.get(new MonitorData.Key(MonitorModel.ES_ENTITY_UUID, serverId));
        if (historyData == null) {
            return;
        }
        final Map<String, Object> newEntResults = new HashMap<String, Object>();
        for (final MonitorEvent.Type type : MonitorEvent.ES_TYPES) {
            final String typeString = type.toString();
            if (historyData.getValues().containsKey(typeString)) {
                final String s = typeString.toLowerCase().replace("_", "-");
                newEntResults.put(s, historyData.getValues().get(typeString));
            }
        }
        newEntResults.put("name", "es");
        newEntResults.put("id", MonitorModel.ES_ENTITY_UUID);
        result.put(key, newEntResults);
    }
    
    private void addExternalComponentResults(final Map<HDKey, Map<String, Object>> result, final UUID compId, final UUID serverId, final String name, final List<String> timeSeriesFields, final List<String> statsFields, final List<String> mostRecentDataFields, final List<String> datumNames, final Long startTime, final Long endTime) throws Exception {
        final MonitorData historyData = MonitorModel.monitorDataMap.get(new MonitorData.Key(compId, serverId));
        final ResultKey key = new ResultKey(compId, serverId);
        final Map<String, Object> serverCurrentMap = new HashMap<String, Object>();
        Map<MonitorEvent.Type, List<Pair<Long, Object>>> timeSeriesData = null;
        if (datumNames != null || endTime != null) {
            timeSeriesData = this.getMonitorEventsAsMap(compId, serverId, historyData, (endTime != null || (statsFields != null && !statsFields.isEmpty()) || (timeSeriesFields != null && !timeSeriesFields.isEmpty())) ? null : datumNames, startTime, endTime);
        }
        Map<String, Object> cachedServerCurrentMap;
        if (historyData != null) {
            cachedServerCurrentMap = historyData.getValues();
        }
        else {
            cachedServerCurrentMap = new HashMap<String, Object>();
        }
        for (final Map.Entry<String, Object> entry : cachedServerCurrentMap.entrySet()) {
            final String newKey = entry.getKey().toLowerCase().replaceAll("[_]", "-");
            serverCurrentMap.put(newKey, entry.getValue());
        }
        if (endTime != null) {
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry2 : timeSeriesData.entrySet()) {
                final String newKey = entry2.getKey().name().toLowerCase().replaceAll("[_]", "-");
                final List<Pair<Long, Object>> list = entry2.getValue();
                if (list != null && !list.isEmpty()) {
                    serverCurrentMap.put("TIMESTAMP", list.get(list.size() - 1).first);
                    serverCurrentMap.put(newKey, list.get(list.size() - 1).second);
                }
            }
        }
        final Map<String, Object> newEntResults = new HashMap<String, Object>();
        newEntResults.put("most-recent-data", serverCurrentMap);
        newEntResults.put("name", name);
        newEntResults.put("type", name);
        newEntResults.put("id", compId);
        if (datumNames != null && timeSeriesFields != null) {
            final Map<String, List<Pair<Long, Object>>> renamedTimeSeriesData = new HashMap<String, List<Pair<Long, Object>>>();
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry3 : timeSeriesData.entrySet()) {
                final String newKey2 = entry3.getKey().name().toLowerCase().replaceAll("[_]", "-");
                if (timeSeriesFields.isEmpty() || timeSeriesFields.contains(newKey2)) {
                    renamedTimeSeriesData.put(newKey2, entry3.getValue());
                }
            }
            newEntResults.put("time-series-data", renamedTimeSeriesData);
        }
        if (datumNames != null && statsFields != null) {
            final Map<String, MonitorStats> statsData = new HashMap<String, MonitorStats>();
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry3 : timeSeriesData.entrySet()) {
                final String newKey2 = entry3.getKey().name().toLowerCase().replaceAll("[_]", "-");
                if (statsFields.isEmpty() || statsFields.contains(newKey2)) {
                    final MonitorStats stats = calculateStats(newKey2, entry3.getValue());
                    if (stats == null) {
                        continue;
                    }
                    statsData.put(newKey2, stats);
                }
            }
            newEntResults.put("stats", statsData);
        }
        result.put(key, newEntResults);
    }
    
    private void addComponentResults(final Map<HDKey, Map<String, Object>> result, final Set<MetaInfo.Server> servers, final MetaInfo.MetaObject comp, final UUID serverId, final List<String> timeSeriesFields, final List<String> statsFields, final List<String> mostRecentDataFields, final List<String> datumNames, final Long startTime, final Long endTime) throws Exception {
        final MonitorData historyData = MonitorModel.monitorDataMap.get(new MonitorData.Key(comp.uuid, serverId));
        final ResultKey key = new ResultKey(comp.uuid, serverId);
        final Map<String, Object> mostRecentData = new HashMap<String, Object>();
        Map<MonitorEvent.Type, List<Pair<Long, Object>>> timeSeriesData = null;
        if (datumNames != null || endTime != null) {
            timeSeriesData = this.getMonitorEventsAsMap(comp.uuid, serverId, historyData, (endTime != null || (statsFields != null && !statsFields.isEmpty()) || (timeSeriesFields != null && !timeSeriesFields.isEmpty())) ? null : datumNames, startTime, endTime);
        }
        Map<String, Object> cachedServerCurrentMap;
        if (historyData != null) {
            cachedServerCurrentMap = historyData.getValues();
        }
        else {
            cachedServerCurrentMap = new HashMap<String, Object>();
        }
        if (this.tsIsWithin(cachedServerCurrentMap.get("TIMESTAMP"), startTime, endTime)) {
            for (final Map.Entry<String, Object> entry : cachedServerCurrentMap.entrySet()) {
                if (mostRecentDataFields != null && !mostRecentDataFields.contains(entry.getKey())) {
                    continue;
                }
                final String newKey = entry.getKey().toLowerCase().replaceAll("[_]", "-");
                mostRecentData.put(newKey, entry.getValue());
                if (!entry.getKey().equals("CPU_RATE")) {
                    continue;
                }
                final long rawRate = (Long)entry.getValue();
                mostRecentData.put("cpu", renderCpuPercent(rawRate));
                mostRecentData.put("cpu-per-node", renderCpuPerNodePercent(rawRate, Runtime.getRuntime().availableProcessors()));
            }
        }
        if (timeSeriesData != null) {
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry2 : timeSeriesData.entrySet()) {
                if (mostRecentDataFields != null && !mostRecentDataFields.contains(entry2.getKey().name())) {
                    continue;
                }
                final String newKey = entry2.getKey().name().toLowerCase().replaceAll("[_]", "-");
                final List<Pair<Long, Object>> list = entry2.getValue();
                if (list == null || list.isEmpty()) {
                    continue;
                }
                final Long itemTs = list.get(list.size() - 1).first;
                if (!this.tsIsWithin(itemTs, startTime, endTime)) {
                    continue;
                }
                mostRecentData.put("timestamp", itemTs);
                mostRecentData.put(newKey, list.get(list.size() - 1).second);
            }
        }
        final Map<String, Object> newEntResults = new HashMap<String, Object>();
        newEntResults.put("most-recent-data", mostRecentData);
        newEntResults.put("name", comp.name);
        newEntResults.put("id", comp.uuid);
        newEntResults.put("ns-name", comp.nsName);
        newEntResults.put("full-name", comp.nsName + "." + comp.name);
        newEntResults.put("ns-id", comp.namespaceId);
        if (comp instanceof MetaInfo.Server) {
            final MetaInfo.Server server = (MetaInfo.Server)comp;
            String apps = null;
            int numComps = 0;
            for (final Map.Entry<String, Set<UUID>> entry3 : server.currentUUIDs.entrySet()) {
                apps = ((apps == null) ? entry3.getKey() : (apps + "," + entry3.getKey()));
                numComps += entry3.getValue().size();
            }
            newEntResults.put("type", server.isAgent ? EntityType.AGENT : comp.type);
            newEntResults.put("status", "RUNNING");
            newEntResults.put("servers", comp.name);
            newEntResults.put("num-servers", 1);
            newEntResults.put("applications", apps);
            newEntResults.put("num-applications", server.currentUUIDs.entrySet().size());
            newEntResults.put("num-components", numComps);
        }
        else {
            final List<String> onServerList = new ArrayList<String>();
            for (final MetaInfo.Server server2 : servers) {
                if (!MonitorEvent.RollupUUID.equals((Object)serverId) && !server2.uuid.equals((Object)serverId)) {
                    continue;
                }
                final Map<String, Set<UUID>> appUUIDs = server2.getCurrentUUIDs();
                for (final Map.Entry<String, Set<UUID>> entry4 : appUUIDs.entrySet()) {
                    if (entry4.getValue().contains(comp.uuid)) {
                        onServerList.add(server2.name);
                        break;
                    }
                }
            }
            newEntResults.put("type", comp.type);
            if (comp.type == EntityType.APPLICATION) {
                newEntResults.put("status", getCompStatus(comp.uuid));
            }
            else {
                newEntResults.put("status", "");
            }
            newEntResults.put("servers", listToString(onServerList));
            newEntResults.put("num-servers", onServerList.size());
            MetaInfo.Flow app = null;
            try {
                app = comp.getCurrentApp();
            }
            catch (MetaDataRepositoryException e) {
                MonitorModel.logger.error((Object)"Error getting current application", (Throwable)e);
            }
            newEntResults.put("applications", (app != null) ? app.getFullName() : "<NONE>");
            newEntResults.put("num-applications", (int)((app != null) ? 1 : 0));
            int numComps = 1;
            if (comp instanceof MetaInfo.Flow) {
                final MetaInfo.Flow flow = (MetaInfo.Flow)comp;
                numComps = 0;
                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> entry5 : flow.objects.entrySet()) {
                    if (entry5.getKey().equals(EntityType.FLOW)) {
                        for (final UUID subUUID : entry5.getValue()) {
                            final MetaInfo.Flow subFlow = (MetaInfo.Flow)MonitorModel.mdLocalCache.getMetaObjectByUUID(subUUID);
                            if (subFlow != null) {
                                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> subEntry : subFlow.objects.entrySet()) {
                                    numComps += subEntry.getValue().size();
                                }
                            }
                        }
                    }
                    else {
                        numComps += entry5.getValue().size();
                    }
                }
            }
            newEntResults.put("num-components", numComps);
            if (app != null && app.getMetaInfoStatus() != null) {
                newEntResults.put("is-valid", app.getMetaInfoStatus().isValid());
            }
        }
        final String dssVersion = Version.getVersionString();
        newEntResults.put("dss-version", dssVersion);
        final String mdcVersion = Version.getMetaDataVersion();
        if (mdcVersion != null) {
            newEntResults.put("mdc-version", mdcVersion);
        }
        if (datumNames != null && timeSeriesFields != null) {
            final Map<String, List<Pair<Long, Object>>> renamedTimeSeriesData = new HashMap<String, List<Pair<Long, Object>>>();
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry6 : timeSeriesData.entrySet()) {
                final String newKey2 = entry6.getKey().name().toLowerCase().replaceAll("[_]", "-");
                if (timeSeriesFields.isEmpty() || timeSeriesFields.contains(newKey2)) {
                    renamedTimeSeriesData.put(newKey2, entry6.getValue());
                }
                if (MonitorEvent.Type.CPU_RATE.equals(entry6.getKey()) && (timeSeriesFields.isEmpty() || timeSeriesFields.contains("cpu-per-node"))) {
                    final List<Pair<Long, Object>> cpuRates = new ArrayList<Pair<Long, Object>>();
                    final List<Pair<Long, Object>> cpuRatesPerNode = new ArrayList<Pair<Long, Object>>();
                    for (final Pair<Long, Object> rawCpu : entry6.getValue()) {
                        final String cpuPercentString = renderCpuPercent((long)rawCpu.second);
                        final String cpuPerNodePercentString = renderCpuPerNodePercent((long)rawCpu.second, Runtime.getRuntime().availableProcessors());
                        cpuRates.add(Pair.make(rawCpu.first, cpuPercentString));
                        cpuRatesPerNode.add(Pair.make(rawCpu.first, cpuPerNodePercentString));
                    }
                    renamedTimeSeriesData.put("cpu", cpuRates);
                    renamedTimeSeriesData.put("cpu-per-node", cpuRatesPerNode);
                }
            }
            newEntResults.put("time-series-data", renamedTimeSeriesData);
        }
        if (datumNames != null && statsFields != null) {
            final Map<String, MonitorStats> statsData = new HashMap<String, MonitorStats>();
            for (final Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry6 : timeSeriesData.entrySet()) {
                final String newKey2 = entry6.getKey().name().toLowerCase().replaceAll("[_]", "-");
                if (statsFields.isEmpty() || statsFields.contains(newKey2)) {
                    final MonitorStats stats = calculateStats(newKey2, entry6.getValue());
                    if (stats != null) {
                        statsData.put(newKey2, stats);
                    }
                }
                if (MonitorEvent.Type.CPU_RATE.equals(entry6.getKey()) && (statsFields.isEmpty() || statsFields.contains("cpu-per-node"))) {
                    final MonitorStats stats = calculateStats("cpu", entry6.getValue());
                    if (stats == null) {
                        continue;
                    }
                    stats.min = renderCpuPercent((long)stats.min);
                    stats.max = renderCpuPercent((long)stats.max);
                    stats.ave = renderCpuPercent((long)stats.ave);
                    statsData.put("cpu", stats);
                }
            }
            newEntResults.put("stats", statsData);
        }
        if (cachedServerCurrentMap.get(MonitorEvent.Type.LAG_REPORT.toString()) != null) {
            newEntResults.put("lag-report", cachedServerCurrentMap.get(MonitorEvent.Type.LAG_REPORT.toString()));
        }
        if (cachedServerCurrentMap.get(MonitorEvent.Type.LAG_RATE.toString()) != null) {
            newEntResults.put("lag-rate", cachedServerCurrentMap.get(MonitorEvent.Type.LAG_RATE.toString()));
        }
        if (cachedServerCurrentMap.get(MonitorEvent.Type.LAG_REPORT_JSON.toString()) != null) {
            newEntResults.put("lag-report-json", cachedServerCurrentMap.get(MonitorEvent.Type.LAG_REPORT_JSON.toString()));
        }
        result.put(key, newEntResults);
    }
    
    private boolean tsIsWithin(final Object timeStamp, final Long startTime, final Long endTime) {
        if (timeStamp == null) {
            return true;
        }
        if (startTime == null && endTime == null) {
            return true;
        }
        try {
            final Long tsl = Long.valueOf(timeStamp.toString());
            if (startTime != null && startTime > tsl) {
                return false;
            }
            if (endTime != null && endTime < tsl) {
                return false;
            }
        }
        catch (NumberFormatException e) {
            MonitorModel.logger.debug((Object)("Could not parse timestamp value as number: " + timeStamp));
        }
        return true;
    }
    
    private void addApplicationResults(final Map<HDKey, Map<String, Object>> result, final Set<MetaInfo.Server> servers, final MetaInfo.Flow app, final UUID serverId, final List<String> timeSeriesFields, final List<String> statsFields, final List<String> mostRecentDataFields, final List<String> datumNames, final Long startTime, final Long endTime) throws Exception {
        final Map<EntityType, LinkedHashSet<UUID>> appE = app.getObjects();
        if (!appE.isEmpty()) {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> appList : appE.entrySet()) {
                for (final UUID uuid : appList.getValue()) {
                    final MetaInfo.MetaObject subObj = MonitorModel.mdLocalCache.getMetaObjectByUUID(uuid);
                    if (subObj != null) {
                        this.addComponentResults(result, servers, subObj, serverId, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
                    }
                }
                if (appList.getKey().equals(EntityType.FLOW)) {
                    for (final UUID flowUUID : appList.getValue()) {
                        final MetaInfo.Flow flow = (MetaInfo.Flow)MonitorModel.mdLocalCache.getMetaObjectByUUID(flowUUID);
                        if (flow != null) {
                            final Map<EntityType, LinkedHashSet<UUID>> flowE = flow.getObjects();
                            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> flowList : flowE.entrySet()) {
                                for (final UUID uuid2 : flowList.getValue()) {
                                    final MetaInfo.MetaObject subObj2 = MonitorModel.mdLocalCache.getMetaObjectByUUID(uuid2);
                                    if (subObj2 != null) {
                                        this.addComponentResults(result, servers, subObj2, serverId, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    private static List<String> getTimeSeriesFields(final List<String> flist, final Map<String, Object> filter) {
        final boolean getTimeSeries = flist.contains("time-series");
        if (!getTimeSeries) {
            return null;
        }
        final String timeSeriesFields = (String)filter.get("time-series-fields");
        List<String> timeSeriesFieldList = stringToList(timeSeriesFields);
        if (getTimeSeries && timeSeriesFieldList == null) {
            timeSeriesFieldList = Collections.emptyList();
        }
        return timeSeriesFieldList;
    }
    
    private static List<String> getStatsFields(final List<String> flist, final Map<String, Object> filter) {
        final boolean getStats = flist.contains("stats");
        if (!getStats) {
            return null;
        }
        final String statsFields = (String)filter.get("stats-fields");
        List<String> statsFieldList = stringToList(statsFields);
        if (getStats && statsFieldList == null) {
            statsFieldList = Collections.emptyList();
        }
        return statsFieldList;
    }
    
    private List<String> getFieldsFromFilter(final Map<String, Object> filter, final String fromField) {
        final Object o = filter.get(fromField);
        List<String> result;
        if (o == null) {
            result = null;
        }
        else {
            result = new ArrayList<String>();
            final String[] split;
            final String[] ss = split = o.toString().split(",");
            for (String s : split) {
                s = s.trim();
                String val = s.toUpperCase().replaceAll("[-]", "_");
                val = val.toUpperCase().replaceAll(" ", "_");
                try {
                    MonitorEvent.Type.valueOf(val);
                    result.add(val);
                }
                catch (Exception e) {
                    MonitorModel.logger.error((Object)("Could not find field: " + s));
                }
            }
        }
        return result;
    }
    
    private static List<String> getDatumNamesFromFilter(final List<String> flist, final Map<String, Object> filter) {
        final List<String> timeSeriesFieldList = getTimeSeriesFields(flist, filter);
        final List<String> statsFieldList = getStatsFields(flist, filter);
        List<String> allFields = null;
        if (timeSeriesFieldList != null || statsFieldList != null) {
            allFields = new ArrayList<String>();
        }
        if (timeSeriesFieldList != null) {
            allFields.addAll(timeSeriesFieldList);
        }
        if (statsFieldList != null) {
            allFields.addAll(statsFieldList);
        }
        if (allFields == null) {
            return null;
        }
        final List<String> datumNames = new ArrayList<String>();
        if (allFields.contains("cpu-per-node")) {
            allFields.add("cpu-rate");
        }
        for (final String string : allFields) {
            String val = string.toUpperCase().replaceAll("[-]", "_");
            val = val.toUpperCase().replaceAll(" ", "_");
            try {
                MonitorEvent.Type.valueOf(val);
                datumNames.add(val);
            }
            catch (Exception e) {
                MonitorModel.logger.warn((Object)("Could not parse field " + string));
            }
        }
        return datumNames;
    }
    
    private static Long getStartTimeFromFilter(final Map<String, Object> filter) {
        Object startTime = filter.get("startTime");
        if (startTime == null) {
            startTime = filter.get("start-time");
        }
        return (startTime != null) ? Long.parseLong(startTime.toString()) : null;
    }
    
    private static Long getEndTimeFromFilter(final Map<String, Object> filter) {
        Object endTime = filter.get("endTime");
        if (endTime == null) {
            endTime = filter.get("end-time");
        }
        return (endTime != null) ? Long.parseLong(endTime.toString()) : null;
    }
    
    private static UUID getAppIDFromFilter(final Map<String, Object> filter, final AuthToken authToken) throws MetaDataRepositoryException {
        final String appName = (String)filter.get("app-name");
        final String appUUID = (String)filter.get("app-uuid");
        UUID appId = null;
        if (appUUID != null) {
            final MetaInfo.Flow app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(new UUID(appUUID), authToken);
            if (app == null) {
                throw new RuntimeException("App with UUID " + appUUID + " cannot be found");
            }
            appId = app.uuid;
        }
        else if (appName != null) {
            MetaInfo.Flow app;
            if (appName.contains(".")) {
                final String[] namespaceAndName = appName.split("\\.");
                assert namespaceAndName[0] != null && namespaceAndName[1] != null;
                app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, namespaceAndName[0], namespaceAndName[1], null, authToken);
            }
            else {
                app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(null, null, appName, null, authToken);
            }
            if (app == null) {
                throw new RuntimeException("App with name " + appName + " cannot be found");
            }
            appId = app.uuid;
        }
        return appId;
    }
    
    private static UUID getServerIDFromFilter(final Set<MetaInfo.Server> servers, final Map<String, Object> filter) {
        final UUID serverId = MonitorEvent.RollupUUID;
        final String serverName = (String)filter.get("server-name");
        String serverUUID = (String)filter.get("server-uuid");
        if (serverUUID == null && filter.get("serverInfo") instanceof UUID) {
            serverUUID = filter.get("serverInfo").toString();
        }
        if (MonitorModel.KAFKA_SERVER_UUID.toString().equals(serverUUID)) {
            return MonitorModel.KAFKA_SERVER_UUID;
        }
        if (MonitorModel.ES_SERVER_UUID.toString().equals(serverUUID)) {
            return MonitorModel.ES_SERVER_UUID;
        }
        if (serverName != null || serverUUID != null) {
            for (final MetaInfo.Server server : servers) {
                if (server.name.equals(serverName) || server.uuid.toString().equals(serverUUID)) {
                    return server.uuid;
                }
            }
        }
        return serverId;
    }
    
    private static void syncTimeSeriesData(final Map<HDKey, Map<String, Object>> results) {
        for (final Map.Entry<HDKey, Map<String, Object>> entry : results.entrySet()) {
            final Map<String, Object> data = entry.getValue();
            final Map<String, List<Pair<Long, Object>>> timeSeriesData = (Map<String, List<Pair<Long, Object>>>)data.get("time-series-data");
            if (timeSeriesData != null) {
                final List<Long> allTimePeriods = new ArrayList<Long>();
                for (final Map.Entry<String, List<Pair<Long, Object>>> timeSeriesEntry : timeSeriesData.entrySet()) {
                    final List<Pair<Long, Object>> timeSeriesList = timeSeriesEntry.getValue();
                    for (final Pair<Long, Object> timeSeriesValue : timeSeriesList) {
                        if (!allTimePeriods.contains(timeSeriesValue.first)) {
                            allTimePeriods.add(timeSeriesValue.first);
                        }
                    }
                }
                if (allTimePeriods.isEmpty()) {
                    return;
                }
                Collections.sort(allTimePeriods);
                for (final String timeSeriesEntry2 : timeSeriesData.keySet()) {
                    final Map<Long, Object> timeSeriesMap = new HashMap<Long, Object>();
                    final List<Pair<Long, Object>> timeSeriesList2 = timeSeriesData.get(timeSeriesEntry2);
                    Object firstValue = null;
                    for (final Pair<Long, Object> timeSeriesValue2 : timeSeriesList2) {
                        timeSeriesMap.put(timeSeriesValue2.first, timeSeriesValue2.second);
                        if (firstValue == null) {
                            firstValue = timeSeriesValue2.second;
                        }
                    }
                    Object prevValue = (firstValue instanceof String) ? "" : 0L;
                    final List<Pair<Long, Object>> updatedTimeSeriesList = new ArrayList<Pair<Long, Object>>();
                    for (final Long timePeriod : allTimePeriods) {
                        Object value = timeSeriesMap.get(timePeriod);
                        if (value == null) {
                            value = prevValue;
                        }
                        updatedTimeSeriesList.add(Pair.make(timePeriod, value));
                        prevValue = value;
                    }
                    timeSeriesData.put(timeSeriesEntry2, updatedTimeSeriesList);
                }
            }
        }
    }
    
    public Map<HDKey, Map<String, Object>> getAllMonitorHDs(final String[] fields, final Map<String, Object> filter, final AuthToken authToken) throws Exception {
        final Map<HDKey, Map<String, Object>> result = new LinkedHashMap<HDKey, Map<String, Object>>();
        if (filter == null) {
            return result;
        }
        final List<String> flist = (fields == null) ? Collections.EMPTY_LIST : Arrays.asList(fields);
        final boolean rollupApps = flist.contains("rollupApps");
        final Long startTime = getStartTimeFromFilter(filter);
        final Long endTime = getEndTimeFromFilter(filter);
        final List<String> timeSeriesFields = getTimeSeriesFields(flist, filter);
        final List<String> statsFields = getStatsFields(flist, filter);
        final List<String> mostRecentDataFields = this.getFieldsFromFilter(filter, "most-recent-data-fields");
        final List<String> datumNames = getDatumNamesFromFilter(flist, filter);
        final UUID appId = getAppIDFromFilter(filter, authToken);
        final Set<MetaInfo.MetaObject> allAppsMo = (Set<MetaInfo.MetaObject>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.APPLICATION, authToken);
        final List<MetaInfo.Flow> allApps = new ArrayList<MetaInfo.Flow>();
        for (final MetaInfo.MetaObject obj : allAppsMo) {
            allApps.add((MetaInfo.Flow)obj);
        }
        final Set<MetaInfo.Server> servers = MonitorModel.mdLocalCache.getServerSet();
        final UUID serverId = getServerIDFromFilter(servers, filter);
        if (rollupApps) {
            for (final MetaInfo.Flow app : allApps) {
                if (appId != null && !app.uuid.equals((Object)appId)) {
                    continue;
                }
                this.addComponentResults(result, servers, app, serverId, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
            }
            for (final MetaInfo.Server server : servers) {
                if (!serverId.equals((Object)MonitorEvent.RollupUUID) && !server.uuid.equals((Object)serverId)) {
                    continue;
                }
                this.addComponentResults(result, servers, server, server.uuid, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
            }
            if (flist.contains("es")) {
                this.addExternalComponentResults(result, MonitorModel.ES_ENTITY_UUID, MonitorModel.ES_SERVER_UUID, "es", timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
            }
            if (flist.contains("kafka")) {
                this.addExternalComponentResults(result, MonitorModel.KAFKA_ENTITY_UUID, MonitorModel.KAFKA_SERVER_UUID, "kafka", timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
            }
        }
        else {
            for (final MetaInfo.Flow app : allApps) {
                if (appId != null && !app.uuid.equals((Object)appId)) {
                    continue;
                }
                this.addApplicationResults(result, servers, app, serverId, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
            }
        }
        syncTimeSeriesData(result);
        return result;
    }
    
    public Map<HDKey, Map<String, Object>> getMonitorHD(final HDKey wk, final String[] fields, final Map<String, Object> filter) throws Exception {
        final Map<HDKey, Map<String, Object>> result = new LinkedHashMap<HDKey, Map<String, Object>>();
        if (wk == null) {
            return result;
        }
        final UUID entityId = wk.id;
        final MetaInfo.MetaObject obj = MonitorModel.mdLocalCache.getMetaObjectByUUID(entityId);
        final Set<MetaInfo.Server> servers = MonitorModel.mdLocalCache.getServerSet();
        final UUID serverId = getServerIDFromFilter(servers, filter);
        if (obj != null) {
            final Long startTime = getStartTimeFromFilter(filter);
            final Long endTime = getEndTimeFromFilter(filter);
            final List<String> flist = (fields == null) ? Collections.EMPTY_LIST : Arrays.asList(fields);
            final List<String> timeSeriesFields = getTimeSeriesFields(flist, filter);
            final List<String> statsFields = getStatsFields(flist, filter);
            final List<String> datumNames = getDatumNamesFromFilter(flist, filter);
            final List<String> mostRecentDataFields = this.getFieldsFromFilter(filter, "most-recent-data-fields");
            final UUID u = (obj instanceof MetaInfo.Server) ? obj.uuid : serverId;
            this.addComponentResults(result, servers, obj, u, timeSeriesFields, statsFields, mostRecentDataFields, datumNames, startTime, endTime);
        }
        else if (MonitorModel.KAFKA_ENTITY_UUID.equals((Object)entityId)) {
            this.addKafkaResults(result);
        }
        else if (MonitorModel.ES_ENTITY_UUID.equals((Object)entityId)) {
            this.addElasticsearchResults(result, serverId);
        }
        else if (MonitorModel.logger.isInfoEnabled()) {
            MonitorModel.logger.info((Object)("Component not found for Monitor HD request Entity UUID=" + entityId + " Server UUID=" + serverId));
        }
        syncTimeSeriesData(result);
        return result;
    }
    
    public static boolean monitorIsEnabled() {
        if (HazelcastSingleton.isAvailable()) {
            final Object enableMonitor = HazelcastSingleton.get().getMap("#ClusterSettings").get((Object)"com.datasphere.config.enable-monitor");
            if (enableMonitor != null) {
                return enableMonitor.toString().equalsIgnoreCase("TRUE");
            }
        }
        final String localProp = System.getProperty("com.datasphere.config.enable-monitor");
        final boolean b = localProp != null && localProp.equalsIgnoreCase("TRUE");
        if (HazelcastSingleton.isAvailable()) {
            HazelcastSingleton.get().getMap("#ClusterSettings").put((Object)"com.datasphere.config.enable-monitor", (Object)b);
        }
        return b;
    }
    
    public static boolean monitorPersistenceIsEnabled() {
        if (HazelcastSingleton.isAvailable()) {
            final Object enableMonitor = HazelcastSingleton.get().getMap("#ClusterSettings").get((Object)"com.datasphere.config.enable-monitor-persistence");
            if (enableMonitor != null) {
                return !enableMonitor.toString().equalsIgnoreCase("FALSE");
            }
        }
        final String localProp = System.getProperty("com.datasphere.config.enable-monitor-persistence");
        final boolean b = localProp == null || !localProp.equalsIgnoreCase("FALSE");
        if (HazelcastSingleton.isAvailable()) {
            HazelcastSingleton.get().getMap("#ClusterSettings").put((Object)"com.datasphere.config.enable-monitor-persistence", (Object)b);
        }
        return b;
    }
    
    public static int getDbExpireTime() {
        if (HazelcastSingleton.isAvailable()) {
            final Object hazProp = HazelcastSingleton.get().getMap("#ClusterSettings").get((Object)"com.datasphere.config.monitor-db-max");
            if (hazProp != null) {
                try {
                    final Integer result = (Integer)hazProp;
                    return result;
                }
                catch (ClassCastException e) {
                    MonitorModel.logger.error((Object)e);
                }
            }
        }
        final String localProp = System.getProperty("com.datasphere.config.monitor-db-max");
        if (localProp != null && !localProp.isEmpty()) {
            final Integer result = Integer.valueOf(localProp);
            if (HazelcastSingleton.isAvailable()) {
                HazelcastSingleton.get().getMap("#ClusterSettings").put((Object)"com.datasphere.config.monitor-db-max", (Object)result);
            }
            return result;
        }
        return 86400000;
    }
    
    public static void resetDbConnection() {
        MonitorModel.theDb = null;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (MonitorModel.logger.isInfoEnabled()) {
            MonitorModel.logger.info((Object)"Waiting until HD Monitor has completed last task");
        }
        MonitorModel.running = false;
        if (this.executor != null) {
            try {
                if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                    this.executor.shutdownNow();
                    if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                        System.err.println("PeriodicPersistence_Scheduler did not terminate");
                    }
                }
            }
            catch (InterruptedException ie) {
                this.executor.shutdown();
                Thread.currentThread().interrupt();
            }
        }
        MonitorModel.monitorDataMap.clear();
        if (MonitorModel.logger.isInfoEnabled()) {
            MonitorModel.logger.info((Object)"HD Monitor shut down");
        }
    }
    
    private static List<MonitorEvent> getDbMonitorEventsFromES(final UUID serverID, final UUID entityID, final Long startTime, final Long endTime, final Collection<String> datumNames, final String orderBy, final Integer maxEvents) {
        final List<MonitorEvent> result = new ArrayList<MonitorEvent>();
        try {
            final Map<String, Object> params = new HashMap<String, Object>();
            if (serverID != null) {
                params.put("serverID", serverID);
            }
            if (entityID != null) {
                params.put("entityID", entityID);
            }
            if (startTime != null) {
                params.put("startTime", startTime);
            }
            if (endTime != null) {
                params.put("endTime", endTime);
            }
            if (datumNames != null && !datumNames.isEmpty()) {
                final List<MonitorEvent.Type> enumDatumNames = new ArrayList<MonitorEvent.Type>();
                for (final String datum : datumNames) {
                    try {
                        final MonitorEvent.Type enumDatumName = MonitorEvent.Type.valueOf(datum);
                        enumDatumNames.add(enumDatumName);
                    }
                    catch (Throwable t) {
                        MonitorModel.logger.error((Object)("Invalid datum name: " + datum + " specified in monitor query"));
                    }
                }
                params.put("datumNames", enumDatumNames);
            }
            String isAscending = "true";
            if (orderBy.equals("DESC")) {
                isAscending = "false";
            }
            final String queryGetMonitorEventsWithFilter = "{ \"select\":[ \"" + MonitorEvent.getFields() + "\" ],  \"from\":  [ \"" + "$Internal.MONITORING" + "\" ]" + MonitorEvent.getWhereClauseJson(params) + MonitorEvent.getOrderByClause(isAscending) + '}';
            final ObjectNode queryJson = (ObjectNode)com.datasphere.hdstore.Utility.readTree(queryGetMonitorEventsWithFilter);
            final HDStoreManager manager = MonitorModel.monitorEventDataType.getHDStore().getManager();
            final HDQuery query = manager.prepareQuery((JsonNode)queryJson);
            for (final HD jsonNodes : query.execute()) {
                if (result.size() >= maxEvents) {
                    break;
                }
                final HD hd = jsonNodes;
                result.add(new MonitorEvent(hd));
            }
        }
        catch (HDStoreMissingException exception) {
            final String monitoringStoreName = InternalType.MONITORING.getHDStoreName();
            if (!monitoringStoreName.equals(exception.hdStoreName)) {
                throw exception;
            }
        }
        return result;
    }
    
    private static void writeToCache(final List<MonitorEvent> events) {
        final Map<MonitorData.Key, MonitorData> entityServerValues = new HashMap<MonitorData.Key, MonitorData>();
        for (final MonitorEvent event : events) {
            final MonitorData.Key key = new MonitorData.Key(event.entityID, event.serverID);
            MonitorData data = entityServerValues.get(key);
            if (data == null) {
                data = new MonitorData(event.entityID, event.serverID);
                entityServerValues.put(data.key, data);
            }
            data.addSingleValue(event.type.name(), (event.valueString != null) ? event.valueString : event.valueLong);
            data.addSingleEvent(event);
            data.addSingleValue("TIMESTAMP", event.timeStamp);
        }
        addCurrentMonitorValues(entityServerValues);
    }
    
    private static void persistInES(final List<MonitorEvent> events) {
        final long startTime = System.currentTimeMillis();
        final List<HD> hds = makeMonitoringHDs(events);
        MonitorModel.monitorEventDataType.insert(hds, null);
        if (MonitorModel.logger.isDebugEnabled()) {
            final long timeTaken = System.currentTimeMillis() - startTime;
            MonitorModel.logger.debug((Object)("time taken to insert " + events.size() + " Monitor hds in ES: " + timeTaken + " ms"));
        }
    }
    
    private static void persistHealthRecordInES(final HealthRecord events) {
        final long startTime = System.currentTimeMillis();
        final HD hds = events.gethd();
        MonitorModel.healthRecordDataType.insert(hds);
        if (MonitorModel.logger.isDebugEnabled()) {
            final long timeTaken = System.currentTimeMillis() - startTime;
            MonitorModel.logger.debug((Object)("time taken to insert 1 Health hds in ES: " + timeTaken + " ms"));
        }
    }
    
    private static List<HD> makeMonitoringHDs(final List<MonitorEvent> events) {
        final List<HD> hds = new ArrayList<HD>();
        for (final MonitorEvent event : events) {
            hds.add(event.gethd());
        }
        return hds;
    }
    
    private static PersistenceLayer getDb() {
        if (MonitorModel.theDb == null) {
            synchronized (PersistenceLayer.class) {
                if (MonitorModel.theDb == null) {
                    if (Server.persistenceIsEnabled()) {
                        try {
                            if (MonitorModel.logger.isDebugEnabled()) {
                                MonitorModel.logger.debug((Object)"HD Monitor is ON, testing connection...");
                            }
                            final Map<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
                            final String clusterName = HazelcastSingleton.getClusterName();
                            final MetaInfo.Initializer initializer = startUpMap.get(clusterName);
                            final String DBUname = initializer.MetaDataRepositoryUname;
                            final String DBPassword = initializer.MetaDataRepositoryPass;
                            final String DBName = initializer.MetaDataRepositoryDBname;
                            final String DBLocation = initializer.MetaDataRepositoryLocation;
                            final Map<String, Object> props = new HashMap<String, Object>();
                            if (DBUname != null && !DBUname.isEmpty()) {
                                props.put("javax.persistence.jdbc.user", DBUname);
                            }
                            if (DBPassword != null && !DBPassword.isEmpty()) {
                                props.put("javax.persistence.jdbc.password", DBPassword);
                            }
                            if (DBName != null && !DBName.isEmpty() && DBUname != null && !DBUname.isEmpty() && DBPassword != null && !DBPassword.isEmpty() && DBLocation != null && !DBLocation.isEmpty()) {
                                props.put("javax.persistence.jdbc.url", "jdbc:derby://" + DBLocation + "/" + DBName + ";user=" + DBUname + ";password=" + DBPassword);
                            }
                            final PersistenceLayer pl = PersistenceFactory.createPersistenceLayer("derby-server-monitor", PersistenceFactory.PersistingPurpose.MONITORING, "derby-server-monitor", props);
                            pl.init("derby-server-monitor");
                            MonitorModel.theDb = pl;
                        }
                        catch (PersistenceUnitLoadingException e) {
                            MonitorModel.logger.error((Object)"Error initializing the HD Monitor persistence unit, which indicates a bad configuration", (Throwable)e);
                        }
                        catch (Exception e2) {
                            MonitorModel.logger.error((Object)("Unexpected error when building database connection: " + e2.getMessage()));
                        }
                    }
                    else if (MonitorModel.logger.isInfoEnabled()) {
                        MonitorModel.logger.info((Object)"HD Monitor is OFF; turn it on by specifying com.datasphere.config.enable-monitor=True");
                    }
                }
            }
        }
        return MonitorModel.theDb;
    }
    
    public List<MonitorEvent> getMonitorEventsSinceLastHealthReport(final long endTime) {
        final HealthMonitor service = new HealthMonitorImpl();
        try {
            final HealthRecordCollection c = service.getHealthRecordsByCount(1, 0L);
            if (c == null || c.healthRecords.size() < 1) {
                return null;
            }
            final HD next = c.healthRecords.iterator().next();
            final JsonNode prevReportEndTimeNode = next.get("endTime");
            final long startTime = prevReportEndTimeNode.asLong();
            final HealthRecordCollection hrc = service.getHealthRecordsByTime(startTime, endTime);
            final Set<HD> hrcEvents = hrc.healthRecords;
            final List<MonitorEvent> result = new ArrayList<MonitorEvent>();
            for (final HD w : hrcEvents) {
                result.add(new MonitorEvent(w));
                if (Logger.getLogger("Monitor").isDebugEnabled()) {
                    Logger.getLogger("Monitor").debug((Object)("Number of Monitor Events since previous Health Report: " + result.size()));
                }
            }
            return result;
        }
        catch (SearchPhaseExecutionException spee) {
            if (spee.shardFailures().length > 0) {
                final StringBuilder reason = new StringBuilder();
                int ii = 0;
                for (final ShardSearchFailure ssf : spee.shardFailures()) {
                    if (ssf.reason() != null) {
                        reason.append(ii++).append(". ").append(ssf.reason() + "\n\n");
                    }
                }
                if (MonitorModel.logger.isInfoEnabled()) {
                    MonitorModel.logger.info((Object)("Some shard searches failed which is expected before first health report: " + reason));
                }
            }
            return null;
        }
        catch (Exception e) {
            MonitorModel.logger.error((Object)e.getMessage(), (Throwable)e);
            return null;
        }
    }
    
    private HealthRecord rollHealthReport(final long now) throws Exception {
        try {
            final Set<MetaInfo.Flow> mos = (Set<MetaInfo.Flow>)MonitorModel.mdLocalCache.getByEntityType(EntityType.FLOW);
            final Map<String, AppHealth> appHealthMap = new HashMap<String, AppHealth>();
            for (final MetaInfo.MetaObject mo : mos) {
                if (!mo.getName().equals("MonitoringProcessApp")) {
                    if (mo.getName().equals("MonitoringSourceApp")) {
                        continue;
                    }
                    if (mo.getMetaInfoStatus().isAdhoc()) {
                        continue;
                    }
                    if (mo.getMetaInfoStatus().isAnonymous()) {
                        continue;
                    }
                    final String status = getCompStatus(mo.uuid);
                    final AppHealth appHealth = new AppHealth(mo.getFullName(), status, mo.getCtime());
                    appHealthMap.put(mo.getFullName(), appHealth);
                }
            }
            this.currentHealthReportBuilder.setAppHealthMap(appHealthMap);
        }
        catch (MetaDataRepositoryException e1) {
            MonitorModel.logger.error((Object)"Failed to create app health details for health report", (Throwable)e1);
        }
        try {
            final int clusterSize = HazelcastSingleton.get().getCluster().getMembers().size();
            this.currentHealthReportBuilder.setClusterSize(clusterSize);
            final Set agentSet = MonitorModel.mdLocalCache.getByEntityType(EntityType.AGENT);
            final int agentCount = (agentSet != null) ? agentSet.size() : 0;
            this.currentHealthReportBuilder.setAgentCount(agentCount);
        }
        catch (Exception e3) {
            MonitorModel.logger.error((Object)"Unable to determine Hazelcast cluster size for Health Record");
        }
        final MetaInfo.Initializer initializer = this.getInitializerObject();
        final boolean isCxnAlive = this.metaDataDBDetails.isConnectionAlive(initializer.getMetaDataRepositoryLocation(), initializer.getMetaDataRepositoryDBname(), initializer.getMetaDataRepositoryUname(), initializer.getMetaDataRepositoryPass());
        this.currentHealthReportBuilder.setDerbyAlive(isCxnAlive);
        this.currentHealthReportBuilder.setElasticSearch(this.rollHealthReportEsIsAlive());
        this.currentHealthReportBuilder.setEndTime(now - 1L);
        final HealthRecord record = this.currentHealthReportBuilder.createHealthRecord();
        try {
            persistHealthRecordInES(record);
            if (MonitorModel.isFailedToInsertFlag) {
                MonitorModel.logger.error((Object)"Successfully inserted health events in elasticsearch after the error");
                MonitorModel.isFailedToInsertFlag = false;
            }
        }
        catch (Throwable e2) {
            if (!MonitorModel.isFailedToInsertFlag) {
                MonitorModel.logger.error((Object)("Failed to insert health events in elasticsearch with error " + e2.getMessage()));
                MonitorModel.isFailedToInsertFlag = true;
            }
        }
        if (Logger.getLogger("Monitor").isDebugEnabled()) {
            Logger.getLogger("Monitor").debug((Object)("Created health object: \n" + record));
        }
        this.currentHealthCreateTime = now;
        (this.currentHealthReportBuilder = new HealthRecordBuilder()).setUuid(new UUID(now));
        this.currentHealthReportBuilder.setStartTime(now);
        return record;
    }
    
    private MetaInfo.Initializer getInitializerObject() {
        final Map<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
        final String clusterName = HazelcastSingleton.getClusterName();
        return startUpMap.get(clusterName);
    }
    
    private boolean rollHealthReportEsIsAlive() {
        final String qu;
        final String queryBetweenTwoTimestamps = qu = String.format("{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\":  { \"and\": [{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s },{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s }]}}", MonitorModel.monitorEventDataType.getHDStore().getName(), "gte", "TIMESTAMP", String.valueOf(0), "lt", "TIMESTAMP", String.valueOf(0));
        try {
            final JsonNode jsonQuery = com.datasphere.hdstore.Utility.objectMapper.readTree(qu);
            final HDQuery q = MonitorModel.monitorModelPersistedHDStore.prepareQuery(jsonQuery);
            q.execute();
            return true;
        }
        catch (IOException e) {
            MonitorModel.logger.warn((Object)"Unable to do a trivial ES query, indicating that ES is unavailable", (Throwable)e);
            return false;
        }
    }
    
    public static String renderCpuPercent(final long nanosPerSec) {
        final double cpuPercent = nanosPerSec / 1.0E9 * 100.0;
        final String cpuPercentString = String.format("%2.1f%%", cpuPercent);
        return cpuPercentString;
    }
    
    public static String renderCpuPerNodePercent(final long nanosPerSec, final int nodes) {
        final double cpuPercent = nanosPerSec / 1.0E9 / nodes * 100.0;
        final String cpuPercentString = String.format("%2.1f%%", cpuPercent);
        return cpuPercentString;
    }
    
    static {
        MonitorModel.logger = Logger.getLogger((Class)MonitorModel.class);
        MonitorModelID = new UUID("9d31a65a-169e-6f32-b105-cea16c56c149");
        KAFKA_SERVER_UUID = new UUID("CAFCA000-2292-4535-99E5-A376C5F50042");
        KAFKA_ENTITY_UUID = new UUID("CAFCA111-9992-AF11-BBE5-E472C0F0BC4F");
        ES_SERVER_UUID = new UUID("E1A571C0-2292-4525-09EE-3656C5F50042");
        ES_ENTITY_UUID = new UUID("E1A571C1-9992-AF98-B0E5-E472C0F0BC40");
        uuidMap = new HashMap<UUID, String>();
        MonitorModel.persistPeriod = 60000;
        MonitorModel.insertQueue = new ArrayDeque<List<MonitorEvent>>();
        kafkaMonitor = new KafkaMonitor();
        elasticsearchMonitor = new ElasticsearchMonitor();
        MonitorModel.running = true;
        MonitorModel.persistMonitor = true;
        MonitorModel.persistHealth = true;
        persistChangeLock = new ReentrantLock();
        MonitorModel.monitorModelPersistedHDStore = null;
        MonitorModel.monitorEventDataType = null;
        MonitorModel.healthRecordHDStore = null;
        MonitorModel.healthRecordDataType = null;
        MonitorModel.mdLocalCache = new MDLocalCache();
        mbs = ManagementFactory.getPlatformMBeanServer();
        beanMap = new HashMap<String, Object>();
        MonitorModel.isFailedToInsertFlag = false;
        MonitorModel.decimalFormat = new DecimalFormat("0.00");
        (MonitorModel.rateDatum = new ArrayList<String>()).add(MonitorEvent.Type.LOOKUPS_RATE.toString());
        MonitorModel.rateDatum.add(MonitorEvent.Type.INPUT_RATE.toString());
        MonitorModel.rateDatum.add(MonitorEvent.Type.RECEIVED_RATE.toString());
        MonitorModel.rateDatum.add(MonitorEvent.Type.LOCAL_HITS_RATE.toString());
        MonitorModel.rateDatum.add(MonitorEvent.Type.REMOTE_HITS_RATE.toString());
        MonitorModel.monitorDataMap = new ConcurrentHashMap<MonitorData.Key, MonitorData>();
        MonitorModel.serverTxMap = new HashMap<UUID, Long>();
        MonitorModel.serverRxMap = new HashMap<UUID, Long>();
        MonitorModel.theDb = null;
    }
    
    public static class ResultKey extends HDKey
    {
        private static final long serialVersionUID = 2995663006723860813L;
        private String stringRep;
        
        public ResultKey() {
        }
        
        public ResultKey(final UUID id, final UUID key) {
            this.id = id;
            this.key = key;
            this.stringRep = this.toString();
        }
        
        @Override
        public void setId(final String id) {
            this.id = new UUID(id);
            this.stringRep = this.toString();
        }
        
        @Override
        public String getId() {
            return this.id.toString();
        }
        
        public void setKey(final String id) {
            this.key = new UUID(id);
            this.stringRep = this.toString();
        }
        
        public String getKey() {
            return this.key.toString();
        }
        
        @Override
        public int hashCode() {
            return this.stringRep.hashCode();
        }
        
        @Override
        public boolean equals(final Object obj) {
            return obj instanceof ResultKey && this.stringRep.equals(((ResultKey)obj).stringRep);
        }
        
        @Override
        public String toString() {
            return "{\"id\":\"" + this.id + "\",\"key\":\"" + this.key + "\"}";
        }
    }
    
    public static class MonitorStats implements Serializable
    {
        private static final long serialVersionUID = 4262383695115160633L;
        public Object min;
        public Object max;
        public Object ave;
        public Object sum;
        public Object count;
        public Object diff;
        public Object resets;
        
        @Override
        public String toString() {
            return "{min:" + this.min + ", max: " + this.max + "ave:" + this.ave + ", sum: " + this.sum + "count:" + this.count + ", diff: " + this.diff + "resets:" + this.resets + "}";
        }
    }
}
