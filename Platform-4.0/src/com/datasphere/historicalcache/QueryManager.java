package com.datasphere.historicalcache;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jctools.queues.MpscCompoundQueue;
import org.joda.time.LocalTime;

import com.datasphere.cache.CacheAccessor;
import com.datasphere.cache.ICache;
import com.datasphere.classloading.HDLoader;
import com.datasphere.common.errors.CacheError;
import com.datasphere.common.errors.IError;
import com.datasphere.common.exc.RecordException;
import com.datasphere.errorhandling.DatallException;
import com.datasphere.errorhandling.DatallRuntimeException;
import com.datasphere.event.Event;
import com.datasphere.event.SimpleEvent;
import com.datasphere.exception.CacheException;
import com.datasphere.exception.SecurityException;
import com.datasphere.historicalcache.converter.CacheRecordConverter;
import com.datasphere.historicalcache.converter.DataExtractor;
import com.datasphere.intf.EventSink;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.AbstractEventSink;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.DataType;
import com.datasphere.hdstore.HD;
import com.datasphere.hdstore.HDQuery;
import com.datasphere.hdstore.HDStore;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.HDStores;
import com.datasphere.hdstore.exceptions.HDStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;

import javassist.CannotCompileException;
import javassist.NotFoundException;

public class QueryManager
{
    private static final Logger logger;
    private IMap<String, List<Long>> snapshotIdListMap;
    public long localSnapshotId;
    private int ERROR_LIMIT;
    private int RECORD_MISS;
    public static final String CACHE_STATUS = "#cacheStatus-";
    public static final String CACHE_NODE_SNAPSHOT = "#cacheNodeSnapshotMap-";
    public static final String CACHE_SNAPSHOT_ID_LIST = "#cacheSnapshotIdQueue-";
    private DataType cacheDataType;
    UpsertCacheRecord processor;
    InsertCacheRecord insertLogic;
    DataExtractor extractor;
    ScheduledExecutorService es;
    String key;
    public BaseProcess adapter;
    public Map<String, Object> readerProps;
    public Map<String, Object> parserProps;
    public Map<String, Object> queryProps;
    public QueryType type;
    public String cacheName;
    Channel output;
    final AtomicLong linesRead;
    private int REMOTE_EXECUTION_QUEUE_SIZE;
    public BlockingQueue<Future<?>> futures;
    protected CacheRefreshTime refreshTime;
    private Class<?> modalNameClass;
    HistoricalCache historicalCache;
    Cache cacheObj;
    AtomicBoolean isQuotesTrimmed;
    CacheProperties cacheProperties;
    HDStore cacheStorage;
    String primaryKey;
    private IMap<String, Map<UUID, Long>> nodeSnapshotMapAll;
    
    public QueryManager(final CacheInfo cacheInfo, final HistoricalCache historicalCache_1_0) throws DatallException {
        this.localSnapshotId = 0L;
        this.ERROR_LIMIT = 200;
        this.RECORD_MISS = 0;
        this.cacheDataType = null;
        this.processor = new UpsertCacheRecord();
        this.insertLogic = new InsertCacheRecord();
        this.extractor = null;
        this.output = null;
        this.linesRead = new AtomicLong();
        this.REMOTE_EXECUTION_QUEUE_SIZE = 65536;
        this.futures = new MpscCompoundQueue<Future<?>>(this.REMOTE_EXECUTION_QUEUE_SIZE);
        this.refreshTime = null;
        this.modalNameClass = null;
        this.historicalCache = null;
        this.cacheObj = null;
        this.isQuotesTrimmed = new AtomicBoolean();
        this.cacheProperties = null;
        this.cacheStorage = null;
        this.primaryKey = null;
        this.cacheObj = cacheInfo.cacheObj;
        this.historicalCache = historicalCache_1_0;
        this.cacheName = cacheInfo.getCacheName();
        this.adapter = cacheInfo.getAdapter();
        this.readerProps = cacheInfo.getReaderProps();
        this.output = cacheInfo.output;
        if (this.readerProps.containsKey("trimquote")) {
            this.isQuotesTrimmed.set((Boolean)this.readerProps.get("trimquote"));
        }
        this.parserProps = cacheInfo.parserProps;
        this.queryProps = cacheInfo.getQueryProps();
        final String adapterClassName = (String)this.readerProps.get("AdapterClassName");
        this.type = QueryType.UNDEFINED;
        if (adapterClassName != null) {
            if (adapterClassName.equals("com.datasphere.proc.CSVReader_1_0")) {
                this.readerProps.put("enableFLM", false);
                this.type = QueryType.CSV;
            }
            else if (adapterClassName.equals("com.datasphere.proc.DatabaseReader_1_0")) {
                this.type = QueryType.Database;
            }
            else if (adapterClassName.equals("com.datasphere.proc.FileReader_1_0") || adapterClassName.equals("com.datasphere.proc.HDFSReader_1_0")) {
                this.type = QueryType.READERPARSER;
                this.readerProps.put("enableFLM", true);
            }
        }
        else if (cacheInfo.cacheObj.getDataSource() instanceof Stream) {
            this.type = QueryType.EVENTTABLE;
        }
        try {
            this.cacheProperties = new CacheProperties(cacheInfo.getQueryProps(), this.cacheObj);
        }
        catch (NoSuchFieldException e1) {
            throw new DatallException((IError)CacheError.NO_PRIMARY_KEY_SELECTED, (Throwable)e1, this.cacheName, (String[])null);
        }
        catch (SecurityException e2) {
            throw new DatallException((IError)CacheError.CACHE_SECURITY_VIOLATION, (Throwable)e2, this.cacheName, (String[])null);
        }
        catch (MetaDataRepositoryException e3) {
            throw new DatallException((IError)CacheError.CACHE_TYPE_NOT_FOUND, (Throwable)e3, this.cacheName, (String[])null);
        }
        this.key = (String)cacheInfo.getQueryProps().get("keytomap");
        this.es = this.cacheObj.getScheduler();
        if (this.queryProps.containsKey("refreshinterval") || this.queryProps.containsKey("refreshStartTime")) {
            Object refreshIntervalVal = this.queryProps.get("refreshinterval");
            if (refreshIntervalVal == null) {
                refreshIntervalVal = "86400000000";
            }
            final Object refreshStartTime = this.queryProps.get("refreshStartTime");
            long refreshInterval;
            try {
                refreshInterval = Utility.parseInterval(refreshIntervalVal.toString()).value;
            }
            catch (RuntimeException e4) {
                refreshInterval = Long.parseLong(refreshIntervalVal.toString());
            }
            LocalTime startTime = null;
            if (refreshStartTime != null) {
                startTime = Utility.parseLocalTime(refreshStartTime.toString());
            }
            this.refreshTime = new CacheRefreshTime(refreshInterval, startTime);
        }
    }
    
    public void close() throws DatallException {
        final MetaInfo.Cache cacheObject = (MetaInfo.Cache)this.cacheObj.getMetaInfo();
        final Class clazz = this.getModalName();
        final HDLoader loader = HDLoader.get();
        Field field = null;
        final Field[] fields = this.getModalName().getFields();
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].getName().equalsIgnoreCase(this.primaryKey)) {
                field = fields[i];
                break;
            }
        }
        if (field == null && QueryManager.logger.isInfoEnabled()) {
            QueryManager.logger.info("Keytomap field does not match to any field in type");
        }
        final CacheRecordConverter crc = new CacheRecordConverter(loader, cacheObject.getNsName(), cacheObject.getName(), clazz, (field == null) ? null : this.primaryKey);
        crc.removeCacheRecordConverter();
    }
    
    public void populateCache(final List<UUID> servers, final ICache cacheObject) throws Exception {
        final IMap<String, Pair<List<UUID>, Long>> cacheStatus = this.getCacheStatusMap();
        boolean doUpdate = false;
        cacheStatus.lock(this.cacheName);
        try {
            final Pair<List<UUID>, Long> lastCacheStatus = (Pair<List<UUID>, Long>)cacheStatus.get(this.cacheName);
            if (lastCacheStatus == null) {
                doUpdate = true;
            }
            else {
                final List<UUID> lastServersList = new ArrayList<UUID>(lastCacheStatus.first);
                final int lastServerListSize = lastServersList.size();
                lastServersList.retainAll(servers);
                final int numRemovedServers = lastServerListSize - lastServersList.size();
                final Set<ICache.CacheInfo> cacheInfoSet = (Set<ICache.CacheInfo>)cacheObject.getCacheInfo();
                int numReplicas = 2;
                if (cacheInfoSet.size() > 0) {
                    final Iterator<ICache.CacheInfo> iterator = cacheInfoSet.iterator();
                    if (iterator.hasNext()) {
                        final ICache.CacheInfo c = iterator.next();
                        numReplicas = c.replicationFactor;
                    }
                }
                if (lastServersList.size() == 0 || (numReplicas > 0 && numRemovedServers >= numReplicas)) {
                    doUpdate = true;
                }
            }
            if (doUpdate) {
                this.addToSnapshotIdList(this.localSnapshotId);
                this.populateCache(cacheObject);
            }
            this.updateCacheStatus((Map<String, Pair<List<UUID>, Long>>)cacheStatus, servers);
        }
        finally {
            cacheStatus.unlock(this.cacheName);
        }
    }
    
    private void updateCacheStatus(final Map<String, Pair<List<UUID>, Long>> cacheStatus, final List<UUID> servers) {
        final Pair<List<UUID>, Long> newStatus = new Pair<List<UUID>, Long>(servers, this.localSnapshotId);
        cacheStatus.put(this.cacheName, newStatus);
    }
    
    public void addToSnapshotIdList(final long snapshotId) throws MetaDataRepositoryException {
        final IMap<String, List<Long>> snapshotIdListMap = this.getSnapshotIdListMap();
        List<Long> snapshotIdList = (List<Long>)snapshotIdListMap.get(this.cacheName);
        if (snapshotIdList == null) {
            snapshotIdList = new ArrayList<Long>();
        }
        snapshotIdList.add(snapshotId);
        snapshotIdListMap.put(this.cacheName, snapshotIdList);
    }
    
    public IMap<String, Pair<List<UUID>, Long>> getCacheStatusMap() throws MetaDataRepositoryException {
        final UUID appId = this.cacheObj.getCurrentApp().getUuid();
        return getCacheStatusMap(appId);
    }
    
    private IMap<String, Map<UUID, Long>> getNodeSnapshotMapForApp() throws MetaDataRepositoryException {
        if (this.nodeSnapshotMapAll == null) {
            final UUID appId = this.cacheObj.getCurrentApp().getUuid();
            final HazelcastInstance hz = HazelcastSingleton.get();
            this.nodeSnapshotMapAll = hz.getMap("#cacheNodeSnapshotMap-" + appId);
        }
        return this.nodeSnapshotMapAll;
    }
    
    private static IMap<String, Pair<List<UUID>, Long>> getCacheStatusMap(final UUID appId) {
        final HazelcastInstance hz = HazelcastSingleton.get();
        return hz.getMap("#cacheStatus-" + appId);
    }
    
    private synchronized IMap<String, List<Long>> getSnapshotIdListMap() throws MetaDataRepositoryException {
        if (this.snapshotIdListMap == null) {
            final UUID appId = this.cacheObj.getCurrentApp().getUuid();
            final HazelcastInstance hz = HazelcastSingleton.get();
            this.snapshotIdListMap = hz.getMap("#cacheSnapshotIdQueue-" + appId);
        }
        return this.snapshotIdListMap;
    }
    
    public long populateCache(final ICache cacheObject) throws DatallException {
        this.RECORD_MISS = 0;
        this.ERROR_LIMIT = 200;
        long size = 0L;
        try {
            if (this.type == QueryType.JPA) {
                this.executeJPA(this.key, cacheObject);
            }
            else if (this.type == QueryType.Database || this.type == QueryType.CSV || this.type == QueryType.READERPARSER) {
                size = this.loadCache(this.key, cacheObject);
            }
            else if (this.type == QueryType.ES) {
                this.reloadElasticSearchData(cacheObject);
            }
            else if (this.type == QueryType.UNDEFINED) {
                throw new DatallRuntimeException((IError)CacheError.CACHE_INCOMPATIBLE_ADAPTER, (Throwable)null, "CACHE", new String[0]);
            }
            if (!cacheObject.isClosed()) {
                try {
                    this.cacheObj.setCacheSize(cacheObject.size());
                }
                catch (IllegalArgumentException ex) {}
            }
            else {
                this.futures.clear();
                this.futures = null;
            }
        }
        catch (DatallRuntimeException e) {
            throw e;
        }
        catch (Exception e2) {
            throw new DatallException((IError)CacheError.CACHE_POPULATE_ERROR, (Throwable)e2, this.cacheName, (String[])null);
        }
        return size;
    }
    
    public void removeFromSnapshotIdList() throws MetaDataRepositoryException {
        final IMap<String, List<Long>> snapshotIdListMap = this.getSnapshotIdListMap();
        final List<Long> snapshotIdList = (List<Long>)snapshotIdListMap.get(this.cacheName);
        final Long id = snapshotIdList.remove(0);
        String removedCacheName;
        if (id != 0L) {
            removedCacheName = this.historicalCache.getCacheSnapshotName(this.cacheName, id);
        }
        else {
            removedCacheName = this.cacheName;
        }
        final ITopic distributedTopic = HazelcastSingleton.get().getTopic("#CACHE-REMOVAL" + this.historicalCache.uniqueIdentifier);
        distributedTopic.publish(removedCacheName);
        snapshotIdListMap.put(this.cacheName, snapshotIdList);
    }
    
    protected long getEvictSnapshotid() throws MetaDataRepositoryException {
        final List<Long> snapshotIdList = (List<Long>)this.getSnapshotIdListMap().get(this.cacheName);
        if (snapshotIdList.size() < 2) {
            return -1L;
        }
        final long oldestSnapshotId = snapshotIdList.get(0);
        Iterator iter = ((Map)this.getNodeSnapshotMapForApp().get(this.cacheName)).entrySet().iterator();
        
        for (final Map.Entry<UUID, Long> entry : ((Map)this.getNodeSnapshotMapForApp().get(this.cacheName)).entrySet()) {
            if (entry.getValue() <= oldestSnapshotId) {
                return -1L;
            }
        }
        return oldestSnapshotId;
    }
    
    public static String registerCallback(final UUID appId, final EntryListener<String, Pair<List<UUID>, Long>> entryListener) {
        final IMap<String, Pair<List<UUID>, Long>> cacheStatus = getCacheStatusMap(appId);
        return cacheStatus.addEntryListener((EntryListener)entryListener, true);
    }
    
    public static void unRegisterCallback(final UUID appId, final String registerId) {
        final IMap<String, Pair<List<UUID>, Long>> cacheStatus = getCacheStatusMap(appId);
        cacheStatus.removeEntryListener(registerId);
    }
    
    private Class<?> getModalName() throws DatallException {
        if (this.modalNameClass == null) {
            final String className = (String)this.readerProps.get("modalname");
            try {
                final Class<?> c = ClassLoader.getSystemClassLoader().loadClass(className);
                this.modalNameClass = c;
            }
            catch (ClassNotFoundException e) {
                throw new DatallException((IError)CacheError.CACHE_TYPE_NOT_FOUND, (Throwable)e, this.cacheName, (String[])null);
            }
        }
        return this.modalNameClass;
    }
    
    public void executeJPA(final String key, final ICache<Object, List<DARecord>> cacheObject) throws Exception {
        final MultiMap mhm = (MultiMap)new MultiValueMap();
        try {
            this.adapter.init(this.readerProps);
        }
        catch (Exception e) {
            throw new CacheException(e.getMessage());
        }
        final AtomicBoolean finished = new AtomicBoolean(false);
        final Class<?> CInfo = this.getModalName();
        int index = 0;
        final Field[] FInfo = CInfo.getFields();
        for (int i = 0; i < FInfo.length; ++i) {
            if (FInfo[i].getName().equalsIgnoreCase(key)) {
                index = i;
                break;
            }
        }
        final int keyIndex = index;
        final EventSink ev_sink = new AbstractEventSink() {
            @Override
            public void receive(final int channel, final Event event) throws Exception {
                if (event == null) {
                    finished.set(true);
                    return;
                }
                if (event instanceof SimpleEvent) {
                    ((SimpleEvent)event).init(System.currentTimeMillis());
                }
                mhm.put(RecordKey.createKey(new Object[] { event.getPayload()[keyIndex] }), event);
            }
        };
        this.adapter.addEventSink(ev_sink);
        while (!finished.get()) {
            this.adapter.receive(0, null);
        }
        this.adapter.removeEventSink(ev_sink);
        int w_count = 0;
        int total_w_count = 0;
        final int total_rq_size = 0;
        for (final Object value : mhm.keySet()) {
            final List<DARecord> wc = new ArrayList<DARecord>();
            for (final Object o : (List)mhm.get(value)) {
                wc.add(new DARecord(o));
            }
            List<DARecord> oldData = cacheObject.get(value);
            if (oldData == null) {
                oldData = new ArrayList<DARecord>();
            }
            cacheObject.put(value, oldData);
            ++w_count;
            total_w_count += wc.size();
        }
        if (QueryManager.logger.isInfoEnabled()) {
            QueryManager.logger.info((cacheObject.getName() + " wc count for " + w_count + " wcs = " + total_w_count));
            QueryManager.logger.info((cacheObject.getName() + " total rangeQueue size = " + total_rq_size));
        }
        this.adapter.close();
    }
    
    public void createCacheRecordExtractor() throws NotFoundException, CannotCompileException, IllegalAccessException, InstantiationException, ClassNotFoundException, IOException, DatallException {
        if (this.extractor != null) {
            return;
        }
        final MetaInfo.Cache cacheObject = (MetaInfo.Cache)this.cacheObj.getMetaInfo();
        final Class clazz = this.getModalName();
        final HDLoader loader = HDLoader.get();
        Field field = null;
        final Field[] fields = this.getModalName().getFields();
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].getName().equalsIgnoreCase(this.primaryKey)) {
                field = fields[i];
                break;
            }
        }
        if (field == null && QueryManager.logger.isInfoEnabled()) {
            QueryManager.logger.info("Keytomap field does not match to any field in type");
        }
        final CacheRecordConverter crc = new CacheRecordConverter(loader, cacheObject.getNsName(), cacheObject.getName(), clazz, (field == null) ? null : this.primaryKey);
        final Class<?> extractorClass = crc.generateCacheRecordConverter(this.cacheObj.KVStorageType.fields);
        this.extractor = (DataExtractor)extractorClass.newInstance();
    }
    
    public synchronized long loadCache(final String key, final ICache cacheObject) throws Exception {
        this.linesRead.set(0L);
        try {
            if (this.parserProps != null && !this.parserProps.isEmpty()) {
                this.adapter.init(this.readerProps, this.parserProps, this.cacheObj.getMetaID(), BaseServer.getServerName(), null, false, null);
            }
            else {
                this.adapter.init(this.readerProps);
            }
        }
        catch (Exception e) {
            throw new CacheException(e.getMessage());
        }
        final AtomicBoolean finished = new AtomicBoolean(false);
        final DataExtractor finalExtractor = this.extractor;
        String skipInvalidProp = (String)this.queryProps.get("skipinvalid");
        if (skipInvalidProp == null) {
            skipInvalidProp = "false";
        }
        final Boolean skipInvalid = Boolean.valueOf(skipInvalidProp);
        final EventSink ev_sink = new AbstractEventSink() {
            @Override
            public void receive(final int channel, final Event event) throws Exception {
                if (cacheObject.isClosed()) {
                    return;
                }
                try {
                    final Object[] o = (Object[])event.getPayload()[0];
                    assert o.length > 0;
                    if (o == null) {
                        finished.set(true);
                        QueryManager.this.flushFutures();
                        return;
                    }
                    if (o.length != QueryManager.this.cacheObj.KVStorageType.fields.size()) {
                        if (!skipInvalid) {
                            throw new DatallRuntimeException((IError)CacheError.INVALID_RECORD_LENGTH, (Throwable)null, "Cache: " + QueryManager.this.cacheName, new String[] { "Expecting record of field length: " + QueryManager.this.cacheObj.KVStorageType.fields.size() + " Found: " + o.length });
                        }
                        if (QueryManager.this.ERROR_LIMIT > 0) {
                            QueryManager.logger.warn(("For cache " + QueryManager.this.cacheName + ", incoming record size and Cache type are not matching <" + o.length + "> columns in data and <" + QueryManager.this.cacheObj.KVStorageType.fields.size() + "> columns in type, event"));
                            QueryManager.this.ERROR_LIMIT--;
                        }
                        QueryManager.this.RECORD_MISS++;
                    }
                    Object genObject = null;
                    Object keyval = null;
                    Pair pair;
                    try {
                        pair = finalExtractor.convertToObject(o);
                    }
                    catch (Exception e) {
                        throw new DatallRuntimeException((IError)CacheError.FIELD_CONVERSION_FAILURE, (Throwable)null, "Cache " + QueryManager.this.cacheName, new String[] { "Converting field failed " + e.getMessage() });
                    }
                    keyval = pair.first;
                    if (keyval == null) {
                        throw new DatallRuntimeException((IError)CacheError.NULL_KEY, (Throwable)null, "Cache", new String[0]);
                    }
                    genObject = pair.second;
                    QueryManager.this.futures.put(((CacheAccessor)cacheObject).futureInvoke(RecordKey.createRecordKey(keyval), QueryManager.this.insertLogic, genObject, QueryManager.this.localSnapshotId));
                    if (QueryManager.this.linesRead.incrementAndGet() % (QueryManager.this.REMOTE_EXECUTION_QUEUE_SIZE - 10) == 0L) {
                        QueryManager.this.flushFutures();
                    }
                }
                catch (DatallRuntimeException strex) {
                    QueryManager.this.linesRead.incrementAndGet();
                    if (!skipInvalid) {
                        throw strex;
                    }
                }
                catch (Exception e2) {
                    QueryManager.this.linesRead.incrementAndGet();
                    if (cacheObject.isClosed()) {
                        return;
                    }
                    QueryManager.logger.error((QueryManager.this.cacheName + " - Problem obtain cache data from " + event + ", " + e2.getMessage()), (Throwable)e2);
                }
            }
        };
        this.adapter.addEventSink(ev_sink);
        try {
            while (!finished.get()) {
                this.adapter.receive(0, null);
            }
        }
        catch (RecordException e3) {
            this.flushFutures();
            finished.set(true);
        }
        catch (DatallRuntimeException strex) {
            throw strex;
        }
        catch (Exception e2) {
            throw new DatallException((IError)CacheError.CACHE_READER_FAILURE, (Throwable)e2, this.cacheName, (String[])null);
        }
        if (this.type == QueryType.CSV) {
            String fileName = (String)this.readerProps.get("directory");
            if (!fileName.endsWith("/")) {
                fileName += '/';
            }
            fileName += this.readerProps.get("wildcard");
            final File f = new File(fileName);
            if (!f.exists()) {
                throw new CacheException("File not found or file is empty: " + fileName);
            }
        }
        this.adapter.removeEventSink(ev_sink);
        if (this.RECORD_MISS > 0) {
            QueryManager.logger.warn(("For cache " + this.cacheName + ", wrong data is received " + this.RECORD_MISS + " times"));
        }
        if (QueryManager.logger.isInfoEnabled()) {
            QueryManager.logger.info((cacheObject.getName() + " population complete with " + this.linesRead.get() + " objects"));
        }
        this.adapter.close();
        return this.linesRead.get();
    }
    
    private void reloadElasticSearchData(final ICache cacheObject) throws DatallException {
        final String queryString = String.format(" { \"select\": [ \"*\" ],  \"from\":   [ \"%s\" ]  }", "$Internal." + this.cacheObj.getMetaFullName());
        try {
            final HDQuery hdQuery = this.cacheStorage.getManager().prepareQuery(com.datasphere.hdstore.Utility.objectMapper.readTree(queryString));
            final Iterator<HD> iterator;
            final Iterator<HD> hds = iterator = hdQuery.execute().iterator();
            while (iterator.hasNext()) {
                final HD type = iterator.next();
                type.remove("$id");
                final Object obj = com.datasphere.hdstore.Utility.objectMapper.readValue(type.toString(), (Class)this.getModalName());
                final List<DARecord> list = new ArrayList<DARecord>();
                list.add(new DARecord(obj));
                Object[] objKeys = null;
                try {
                    objKeys = this.cacheProperties.getKeys(obj);
                }
                catch (IllegalArgumentException ex) {}
                catch (IllegalAccessException ex2) {}
                cacheObject.invoke(RecordKey.cacheRecordKeyCreator(objKeys), (EntryProcessor)this.processor, new Object[] { list });
            }
        }
        catch (HDStoreException e1) {
            throw new DatallException((IError)CacheError.EVENTTABLE_QUERY_FAILED, (Throwable)e1, this.cacheName, (String[])null);
        }
        catch (IOException e2) {
            throw new DatallException((IError)CacheError.EVENTTABLE_DOCUMENT_READ_FAIL, (Throwable)e2, this.cacheName, (String[])null);
        }
    }
    
    private HD cacheDataConverter(final Object object, final Object[] objKeys) {
        final ObjectNode hdJson = (ObjectNode)com.datasphere.hdstore.Utility.objectMapper.valueToTree;
        final String[] ignorableEvents = { "_id", "idstring", "key", "originTimeStamp", "timeStamp" };
        for (int i = 0; i < ignorableEvents.length; ++i) {
            hdJson.remove(ignorableEvents[i]);
        }
        final String id = StringUtils.join(objKeys, "_");
        final HD hd = new HD();
        hd.setAll(hdJson);
        hd.put("$id", id);
        return hd;
    }
    
    public void upsert(final ICache cache, final ITaskEvent event) throws IllegalArgumentException, IllegalAccessException {
        for (final DARecord batchItem : event.batch()) {
            final Object[] objKeys = this.cacheProperties.getKeys(batchItem.data);
            if (this.cacheProperties.backedByElasticSearch) {
                this.cacheDataType.insert(this.cacheDataConverter(batchItem.data, objKeys));
            }
            final List<DARecord> list = new ArrayList<DARecord>();
            list.add(batchItem);
            cache.invoke(RecordKey.cacheRecordKeyCreator(objKeys), (EntryProcessor)this.processor, new Object[] { list });
        }
    }
    
    private void flushFutures() throws InterruptedException, ExecutionException {
        Future<?> future = null;
        while ((future = this.futures.poll()) != null) {
            future.get();
        }
        if (this.cacheObj.getInsertInfo() != null) {
            QueryManager.logger.info(("For Cache " + this.cacheName + " bulk insert data point reached - logical record count: " + this.linesRead.get()));
        }
    }
    
    public void updateSnapshotIdForNode(final UUID node, final long id) throws MetaDataRepositoryException {
        final IMap<String, Map<UUID, Long>> nodeSnapshotMapForApp = this.getNodeSnapshotMapForApp();
        nodeSnapshotMapForApp.lock(this.cacheName);
        Map<UUID, Long> nodeSnapshotMap = (Map<UUID, Long>)nodeSnapshotMapForApp.get(this.cacheName);
        if (nodeSnapshotMap == null) {
            nodeSnapshotMap = new HashMap<UUID, Long>();
        }
        nodeSnapshotMap.put(node, id);
        nodeSnapshotMapForApp.put(this.cacheName, nodeSnapshotMap);
        nodeSnapshotMapForApp.unlock(this.cacheName);
        QueryManager.logger.info(("Updated SnapshotId for node: " + node.getUUIDString() + " with id: " + id));
    }
    
    static {
        logger = Logger.getLogger(QueryManager.class.getName());
    }
    
    public enum QueryType
    {
        UNDEFINED, 
        JPA, 
        Database, 
        CSV, 
        HTTP, 
        READERPARSER, 
        EVENTTABLE, 
        ES;
    }
    
    class CacheProperties
    {
        String[] uniqueKeys;
        Field[] fieldAccessor;
        Boolean backedByElasticSearch;
        MetaInfo.Type cacheType;
        Map<String, CacheDataType> javaTypeToHDType;
        String persistPolicy;
        
        private DataType getCacheDataType(final HDStore hdStore, final String dataTypeName, final JsonNode dataTypeSchema) {
            final DataType contextType = hdStore.setDataType(dataTypeName, dataTypeSchema, null);
            if (contextType == null) {
                final String hdStoreName = hdStore.getName();
                throw new HDStoreException(String.format("Unable to create data type '%s' in EventTable '%s'", dataTypeName, hdStoreName));
            }
            return contextType;
        }
        
        private JsonNode makeFieldNode(final String name, final String javaTypeName) {
            ObjectNode result = null;
            final CacheDataType hdDataType = this.javaTypeToHDType.get(javaTypeName);
            if (hdDataType != null) {
                result = com.datasphere.hdstore.Utility.objectMapper.createObjectNode();
                result.put("name", name);
                result.put("type", hdDataType.typeName);
                result.put("nullable", hdDataType.nullable);
            }
            return (JsonNode)result;
        }
        
        private ArrayNode createDataTypeSchema(final MetaInfo.Type type) {
            final ArrayNode dataTypeSchema = com.datasphere.hdstore.Utility.objectMapper.createArrayNode();
            for (final Map.Entry<String, String> field : type.fields.entrySet()) {
                final String fieldName = field.getKey();
                final String fieldType = field.getValue();
                final JsonNode fieldNode = this.makeFieldNode(fieldName, fieldType);
                if (fieldNode != null) {
                    dataTypeSchema.add(fieldNode);
                }
                else {
                    final String typeName = type.getFullName();
                    QueryManager.logger.warn(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'", fieldType, fieldName, typeName));
                }
            }
            return dataTypeSchema;
        }
        
        private JsonNode createHDStoreSchema() {
            final ObjectNode result = com.datasphere.hdstore.Utility.objectMapper.createObjectNode();
            final ArrayNode contextItems = this.createDataTypeSchema(this.cacheType);
            final JsonNode metadataItems = (JsonNode)com.datasphere.hdstore.Utility.objectMapper.createArrayNode();
            contextItems.add(this.makeFieldNode("$id", "string"));
            result.set("context", (JsonNode)contextItems);
            if (metadataItems.size() > 0) {
                result.set("metadata", metadataItems);
            }
            return (JsonNode)result;
        }
        
        public CacheProperties(final Map<String, Object> cachePropertyMap, final Cache c) throws MetaDataRepositoryException, NoSuchFieldException, SecurityException, DatallException {
            this.uniqueKeys = null;
            this.fieldAccessor = null;
            this.backedByElasticSearch = false;
            this.cacheType = null;
            this.javaTypeToHDType = new HashMap<String, CacheDataType>();
            this.persistPolicy = (String)cachePropertyMap.get("persistPolicy");
            if (this.persistPolicy != null && this.persistPolicy.equalsIgnoreCase("TRUE")) {
                this.backedByElasticSearch = true;
            }
            if (this.backedByElasticSearch) {
                this.cacheType = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(c.getTypename(), HSecurityManager.TOKEN);
                this.javaTypeToHDType.put("byte", new CacheDataType("integer", false));
                this.javaTypeToHDType.put("short", new CacheDataType("integer", false));
                this.javaTypeToHDType.put("int", new CacheDataType("integer", false));
                this.javaTypeToHDType.put("java.lang.Byte", new CacheDataType("integer", true));
                this.javaTypeToHDType.put("java.lang.Short", new CacheDataType("integer", true));
                this.javaTypeToHDType.put("java.lang.Integer", new CacheDataType("integer", true));
                this.javaTypeToHDType.put("long", new CacheDataType("long", false));
                this.javaTypeToHDType.put("java.lang.Long", new CacheDataType("long", true));
                this.javaTypeToHDType.put("float", new CacheDataType("double", false));
                this.javaTypeToHDType.put("double", new CacheDataType("double", false));
                this.javaTypeToHDType.put("java.lang.Float", new CacheDataType("double", true));
                this.javaTypeToHDType.put("java.lang.Double", new CacheDataType("double", true));
                this.javaTypeToHDType.put("java.lang.Number", new CacheDataType("double", true));
                this.javaTypeToHDType.put("boolean", new CacheDataType("boolean", false));
                this.javaTypeToHDType.put("java.lang.Boolean", new CacheDataType("boolean", true));
                this.javaTypeToHDType.put("string", new CacheDataType("string", false));
                this.javaTypeToHDType.put("java.lang.String", new CacheDataType("string", true));
                this.javaTypeToHDType.put("datetime", new CacheDataType("datetime", false));
                this.javaTypeToHDType.put("org.joda.time.DateTime", new CacheDataType("datetime", true));
                final HDStoreManager manager = HDStores.getInstance("elasticsearch", cachePropertyMap);
                QueryManager.this.cacheStorage = manager.getOrCreate("$Internal." + c.getMetaFullName(), cachePropertyMap);
                final JsonNode hdStoreSchema = this.createHDStoreSchema();
                final String contextTypeName = c.getMetaNsName() + ':' + c.getMetaName();
                QueryManager.this.cacheDataType = this.getCacheDataType(QueryManager.this.cacheStorage, contextTypeName, hdStoreSchema);
            }
            QueryManager.this.primaryKey = (String)cachePropertyMap.get("keytomap");
            if (QueryManager.this.primaryKey == null) {
                final Set<String> keySet = cachePropertyMap.keySet();
                if (keySet != null) {
                    for (final String key : keySet) {
                        if (key.equalsIgnoreCase("keytomap")) {
                            QueryManager.this.primaryKey = (String)cachePropertyMap.get(key);
                        }
                    }
                }
                if (QueryManager.this.primaryKey == null) {
                    throw new RuntimeException("No primary key selected.");
                }
            }
            final String uniqueKey = (String)cachePropertyMap.get("uniquekey");
            if (uniqueKey != null) {
                this.uniqueKeys = uniqueKey.split(",");
            }
            if (this.uniqueKeys == null) {
                this.fieldAccessor = new Field[1];
            }
            else {
                this.fieldAccessor = new Field[this.uniqueKeys.length + 1];
            }
            for (final Field declaredFieldName : QueryManager.this.getModalName().getDeclaredFields()) {
                if (declaredFieldName.getName().equalsIgnoreCase(QueryManager.this.primaryKey)) {
                    this.fieldAccessor[0] = declaredFieldName;
                    break;
                }
            }
            for (int i = 1; i < this.fieldAccessor.length; ++i) {
                this.fieldAccessor[i] = QueryManager.this.getModalName().getField(this.uniqueKeys[i - 1]);
            }
            if (this.backedByElasticSearch) {
                if (QueryManager.this.type != QueryType.EVENTTABLE) {
                    throw new IllegalStateException("Type already defined");
                }
                QueryManager.this.type = QueryType.ES;
            }
        }
        
        public Object[] getKeys(final Object data) throws IllegalArgumentException, IllegalAccessException {
            final Object[] keyset = new Object[this.fieldAccessor.length];
            for (int i = 0; i < keyset.length; ++i) {
                keyset[i] = this.fieldAccessor[i].get(data);
            }
            return keyset;
        }
        
        private class CacheDataType
        {
            public final String typeName;
            public final boolean nullable;
            
            CacheDataType(final String typeName, final boolean nullable) {
                this.typeName = typeName;
                this.nullable = nullable;
            }
            
            @Override
            public boolean equals(final Object obj) {
                if (obj instanceof CacheDataType) {
                    final CacheDataType instance = CacheDataType.class.cast(obj);
                    return instance.typeName.equals(this.typeName) && instance.nullable == this.nullable;
                }
                return false;
            }
            
            @Override
            public int hashCode() {
                return this.typeName.hashCode() ^ (this.nullable ? 1 : 0);
            }
        }
    }
    
    public static class InsertCacheRecord implements EntryProcessor<Object, List<DARecord>, Boolean>, Serializable
    {
        private static final long serialVersionUID = 3232629587572334849L;
        
        public Boolean process(final MutableEntry<Object, List<DARecord>> arg0, final Object... arguments) throws EntryProcessorException {
            if (arguments == null || arguments.length == 0 || arguments.length != 2) {
                throw new EntryProcessorException("Arguments cannot be null or empty");
            }
            final Object genObject = arguments[0];
            final Long SID = (Long)arguments[1];
            final List<DARecord> oldData = (List<DARecord>)arg0.getValue();
            if (oldData != null) {
                final List<DARecord> dataList = new ArrayList<DARecord>();
                dataList.add(new DARecord(genObject));
                oldData.addAll(dataList);
                arg0.setValue(oldData);
            }
            else {
                final List<DARecord> dataList = new ArrayList<DARecord>(1);
                dataList.add(new DARecord(genObject));
                arg0.setValue(dataList);
            }
            return null;
        }
    }
    
    public static class UpsertCacheRecord implements EntryProcessor<Object, List<DARecord>, Boolean>, Serializable
    {
        private static final long serialVersionUID = -9200219203241311642L;
        
        public Boolean process(final MutableEntry<Object, List<DARecord>> arg0, final Object... arguments) throws EntryProcessorException {
            if (arguments == null || arguments.length == 0) {
                throw new EntryProcessorException("Arguments cannot be null or empty");
            }
            if (!(arguments[0] instanceof List)) {
                throw new EntryProcessorException("Argument needs to be a List");
            }
            final List<DARecord> newData = (List<DARecord>)arguments[0];
            final List<DARecord> oldEvent = (List<DARecord>)arg0.getValue();
            final List<DARecord> newEvent = new ArrayList<DARecord>(0);
            if (oldEvent != null && oldEvent.get(0) != null && oldEvent.get(0).data instanceof SimpleEvent && ((SimpleEvent)oldEvent.get(0).data).getIDString().equals(((SimpleEvent)newData.get(0).data).getIDString())) {
                return false;
            }
            newEvent.addAll(newData);
            arg0.setValue(newEvent);
            return true;
        }
    }
}
