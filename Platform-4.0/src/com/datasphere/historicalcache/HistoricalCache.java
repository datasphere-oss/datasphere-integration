package com.datasphere.historicalcache;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.cache.Cache.Entry;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;

import org.apache.log4j.Logger;
import org.joda.time.LocalTime;

import com.datasphere.cache.CacheConfiguration;
import com.datasphere.cache.CacheManager;
import com.datasphere.cache.CachingProvider;
import com.datasphere.cache.ICache;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.datasphere.errorhandling.DatallException;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.uuid.UUID;

public class HistoricalCache implements Serializable
{
    private static Logger logger;
    private final ReentrantLock lock;
    public CacheInfo cacheLoad;
    public ICache<Object, List<DARecord>> oldCache;
    public ICache<Object, List<DARecord>> currentCache;
    QueryManager qm;
    Channel output;
    private String cacheRefreshRegisterId;
    private String cacheRemovalRegisterId;
    private List serversList;
    private AtomicLong transactionIdCounter;
    private Map<Long, Long> transactionSnapshotMap;
    public long latestSnapshotId;
    private CachingProvider provider;
    public CacheManager manager;
    private ITopic distributedTopic;
    String uniqueIdentifier;
    public ScheduledFuture<?> refreshIntervalFuture;
    private boolean isCacheLeader;
    
    public HistoricalCache(final BaseServer srv) {
        this.lock = new ReentrantLock();
        this.qm = null;
        this.output = null;
        this.transactionIdCounter = new AtomicLong(1100L);
        this.latestSnapshotId = 0L;
        this.provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
        this.manager = (CacheManager)this.provider.getCacheManager();
        this.distributedTopic = null;
        this.uniqueIdentifier = null;
        this.refreshIntervalFuture = null;
    }
    
    public IBatch get(final RecordKey key, final Long transactionId) {
        if (key.isNull()) {
            return (IBatch)Batch.emptyBatch();
        }
        final long snapshotId = this.getSnapshotId(transactionId);
        String cacheName = this.qm.cacheName;
        if (snapshotId != 0L) {
            cacheName = this.getCacheSnapshotName(cacheName, snapshotId);
        }
        final ICache<Object, List<DARecord>> cache = (ICache<Object, List<DARecord>>)(ICache)this.manager.getCache(cacheName);
        if (cache == null) {
            throw new IllegalStateException("Cache " + cacheName + " with snapshotId " + snapshotId + " does not exist.");
        }
        final List<DARecord> val = cache.get(key);
        if (val == null) {
            return (IBatch)Batch.emptyBatch();
        }
        return (IBatch)Batch.asBatch(val);
    }
    
    public Batch get(final Long transactionId) {
        final long snapshotId = (transactionId == -1L) ? this.latestSnapshotId : this.getSnapshotId(transactionId);
        this.lock.lock();
        Batch b = null;
        try {
            String cacheName = this.qm.cacheName;
            if (snapshotId != 0L) {
                cacheName = this.getCacheSnapshotName(cacheName, snapshotId);
            }
            final ICache<Object, List<DARecord>> cache = (ICache<Object, List<DARecord>>)(ICache)this.manager.getCache(cacheName);
            b = new Batch() {
                @Override
                public int size() {
                    return (int)cache.size();
                }
                
                @Override
                public Iterator<DARecord> iterator() {
                    return new Iterator<DARecord>() {
                        Iterator<Entry<Object, List<DARecord>>> it = cache.iterator();
                        Iterator<DARecord> subIter = null;
                        
                        @Override
                        public boolean hasNext() {
                            if (this.subIter == null) {
                                return this.it.hasNext();
                            }
                            if (this.subIter.hasNext()) {
                                return this.subIter.hasNext();
                            }
                            return this.it.hasNext();
                        }
                        
                        @Override
                        public DARecord next() {
                            if (this.subIter != null) {
                                if (this.subIter.hasNext()) {
                                    return this.subIter.next();
                                }
                                if (!this.it.hasNext()) {
                                    return null;
                                }
                                final Entry<Object, List<DARecord>> entry = this.it.next();
                                if (entry.getValue() == null) {
                                    if (HistoricalCache.logger.isInfoEnabled()) {
                                        HistoricalCache.logger.info((Object)("Snapshot " + snapshotId + " is either old or deleted."));
                                    }
                                    this.subIter = null;
                                    return null;
                                }
                                this.subIter = ((List)entry.getValue()).iterator();
                                if (this.subIter.hasNext()) {
                                    return this.subIter.next();
                                }
                                return null;
                            }
                            else {
                                final Entry<Object, List<DARecord>> entry = this.it.next();
                                if (entry.getValue() == null) {
                                    if (HistoricalCache.logger.isInfoEnabled()) {
                                        HistoricalCache.logger.info((Object)("Snapshot " + snapshotId + " is either old or deleted."));
                                    }
                                    this.subIter = null;
                                    return null;
                                }
                                this.subIter = ((List)entry.getValue()).iterator();
                                if (this.subIter.hasNext()) {
                                    return this.subIter.next();
                                }
                                return null;
                            }
                        }
                        
                        @Override
                        public void remove() {
                        }
                    };
                }
            };
        }
        finally {
            this.lock.unlock();
        }
        return b;
    }
    
    public String getCacheSnapshotName(final String cacheName, final Long snapshotId) {
        return (snapshotId == null) ? cacheName : (cacheName + "-" + snapshotId);
    }
    
    public void init(final Map<String, Object> reader_properties, final Map<String, Object> parser_properties, final Map<String, Object> query_properties, final BaseProcess bp, final String name, final Channel output, final com.datasphere.historicalcache.Cache cacheObj) throws Exception {
        this.cacheLoad = new CacheInfo(name, bp, reader_properties, parser_properties, query_properties, output, cacheObj);
        this.uniqueIdentifier = this.cacheLoad.cacheObj.getMetaInfo().getUuid().getUUIDString();
        this.currentCache = (ICache<Object, List<DARecord>>)this.createNewCache(null);
        this.output = this.cacheLoad.getOutputChannel();
        this.qm = new QueryManager(this.cacheLoad, this);
        this.transactionSnapshotMap = new ConcurrentHashMap<Long, Long>();
        this.qm.updateSnapshotIdForNode(Server.getServer().getServerID(), this.latestSnapshotId);
    }
    
    public CacheInfo getCacheInfo() {
        return this.cacheLoad;
    }
    
    public void stop() throws Exception {
        if (this.cacheLoad != null) {
            this.cacheLoad.stop();
        }
        if (this.cacheRemovalRegisterId != null) {
            this.distributedTopic.removeMessageListener(this.cacheRemovalRegisterId);
            this.cacheRemovalRegisterId = null;
        }
    }
    
    public void upsert(final ITaskEvent event) {
        try {
            this.qm.upsert(this.currentCache, event);
        }
        catch (IllegalArgumentException | IllegalAccessException ex2) {
            HistoricalCache.logger.warn((Object)ex2.getMessage());
        }
    }
    
    public void start(final List<UUID> servers) throws Exception {
        if (HistoricalCache.logger.isDebugEnabled()) {
            HistoricalCache.logger.debug((Object)("Starting cache " + this.cacheLoad.getCacheName()));
        }
        this.distributedTopic = HazelcastSingleton.get().getTopic("#CACHE-REMOVAL" + this.uniqueIdentifier);
        if (this.cacheRemovalRegisterId == null) {
            this.cacheRemovalRegisterId = this.distributedTopic.addMessageListener((MessageListener)new MessageListener() {
                public void onMessage(final Message message) {
                    if (HistoricalCache.logger.isDebugEnabled()) {
                        HistoricalCache.logger.debug((Object)("Cache removal: " + message.getMessageObject()));
                    }
                    HistoricalCache.this.manager.destroyCache((String)message.getMessageObject());
                }
            });
        }
        this.serversList = servers;
        this.cacheLoad.start(servers);
        this.cacheRefreshRegisterId = QueryManager.registerCallback(this.cacheLoad.getCuurentAppId(), (EntryListener<String, Pair<List<UUID>, Long>>)new refreshCacheListener());
        if (this.getLowestServerId(servers).equals((Object)Server.server.getServerID())) {
            HistoricalCache.logger.info((Object)("Got the lock to be the QueryManager for cache " + this.qm.cacheName + " with servers " + servers));
            this.isCacheLeader = true;
            try {
                this.qm.createCacheRecordExtractor();
            }
            catch (Exception e) {
                HistoricalCache.logger.error((Object)("Cache Record Extractor failed " + e.getMessage()), (Throwable)e);
                throw e;
            }
            this.qm.populateCache(servers, this.currentCache);
        }
        if (this.qm.refreshTime != null && this.refreshIntervalFuture == null) {
            long initialDelay;
            final long refreshInterval = initialDelay = this.qm.refreshTime.getRefreshInterval();
            if (this.qm.refreshTime.getStartTime() != null) {
                final LocalTime now = LocalTime.now();
                final LocalTime delay = this.qm.refreshTime.subtract(now);
                initialDelay = delay.getMillisOfDay() * 1000L;
            }
            this.refreshIntervalFuture = this.qm.es.scheduleAtFixedRate(new CacheRefreshOperator(), initialDelay, refreshInterval, TimeUnit.MICROSECONDS);
        }
    }
    
    private UUID getLowestServerId(final List<UUID> servers) {
        UUID lowestId = servers.get(0);
        for (final UUID server : servers) {
            if (server.compareTo(lowestId) < 0) {
                lowestId = server;
            }
        }
        return lowestId;
    }
    
    public long beginTransaction() {
        final long transactionId = this.transactionIdCounter.incrementAndGet();
        this.lock.lock();
        try {
            this.transactionSnapshotMap.put(transactionId, this.latestSnapshotId);
        }
        finally {
            this.lock.unlock();
        }
        return transactionId;
    }
    
    public void endTransaction(final long transactionId) {
        final long snapshotId = this.getSnapshotId(transactionId);
        this.removeTransactionId(transactionId);
        this.lock.lock();
        try {
            if (this.latestSnapshotId != snapshotId) {
                try {
                    this.updateLatestSnapshotIdInUse();
                }
                catch (MetaDataRepositoryException e) {
                    HistoricalCache.logger.error((Object)("Failed to end transaction with id: " + transactionId), (Throwable)e);
                }
            }
        }
        finally {
            this.lock.unlock();
        }
    }
    
    private void updateLatestSnapshotIdInUse() throws MetaDataRepositoryException {
        for (final Map.Entry<Long, Long> entry : this.transactionSnapshotMap.entrySet()) {
            if (entry.getValue() < this.latestSnapshotId) {
                return;
            }
        }
        this.qm.updateSnapshotIdForNode(Server.getServer().getServerID(), this.latestSnapshotId);
    }
    
    private void removeTransactionId(final long transactionId) {
        this.transactionSnapshotMap.remove(transactionId);
    }
    
    public void close() throws MetaDataRepositoryException, DatallException {
        if (this.cacheRefreshRegisterId != null) {
            QueryManager.unRegisterCallback(this.cacheLoad.getCuurentAppId(), this.cacheRefreshRegisterId);
        }
        if (this.cacheLoad != null) {
            this.cacheLoad.close();
            this.cacheLoad = null;
        }
        if (this.refreshIntervalFuture != null) {
            this.refreshIntervalFuture.cancel(true);
            this.refreshIntervalFuture = null;
            if (this.qm != null && this.qm.futures != null) {
                this.qm.futures.clear();
            }
        }
        if (this.qm != null) {
            this.qm.close();
        }
        if (this.distributedTopic != null) {
            this.distributedTopic.destroy();
        }
        this.currentCache = null;
        this.oldCache = null;
        this.qm = null;
    }
    
    private long getSnapshotId(final Long transactionId) {
        final Long snapshotId = this.transactionSnapshotMap.get(transactionId);
        if (snapshotId == null) {
            throw new IllegalStateException("No snapshotId was found for transactionId: " + transactionId);
        }
        return snapshotId;
    }
    
    public long getLatestSnapshotId() {
        return this.latestSnapshotId;
    }
    
    private void refresh() throws MetaDataRepositoryException {
        final IMap<String, Pair<List<UUID>, Long>> cacheStatus = this.qm.getCacheStatusMap();
        final String cacheName = this.getCacheSnapshotName(this.qm.cacheName, this.qm.localSnapshotId + 1L);
        cacheStatus.lock(this.qm.cacheName);
        try {
            final QueryManager qm = this.qm;
            ++qm.localSnapshotId;
            if (this.qm.localSnapshotId < 0L && HistoricalCache.logger.isInfoEnabled()) {
                HistoricalCache.logger.info((Object)String.format(Constants.WRONG_SNAPSHOT_ID, this.qm.cacheName));
            }
            HistoricalCache.logger.info((Object)String.format(Constants.LOCAL_SNAPSHOT_ID, this.qm.cacheName, this.qm.localSnapshotId));
            this.qm.addToSnapshotIdList(this.qm.localSnapshotId);
            final ICache cacheObject = (ICache)this.manager.getCache(cacheName);
            if (cacheObject == null && HistoricalCache.logger.isDebugEnabled()) {
                HistoricalCache.logger.debug((Object)("Shouldn't be null: " + cacheName));
            }
            this.qm.populateCache(cacheObject);
            final Pair<List<UUID>, Long> newStatus = (Pair<List<UUID>, Long>)cacheStatus.get((Object)this.qm.cacheName);
            if (newStatus == null) {
                throw new RuntimeException(String.format(Constants.STATUS_IS_NULL, this.qm.cacheName));
            }
            newStatus.second = this.qm.localSnapshotId;
            cacheStatus.put(this.qm.cacheName, newStatus);
            this.cacheLoad.getCacheObj().setLastRefreshTime(System.currentTimeMillis());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            cacheStatus.unlock(this.qm.cacheName);
        }
        this.evictOldSnapshot();
    }
    
    public void evictOldSnapshot() {
        try {
            if (HistoricalCache.logger.isInfoEnabled()) {
                HistoricalCache.logger.info((Object)"EVICT SNAPSHOT STARTED");
            }
            long evictSnapshotId;
            for (evictSnapshotId = this.qm.getEvictSnapshotid(); evictSnapshotId == -1L; evictSnapshotId = this.qm.getEvictSnapshotid()) {
                try {
                    Thread.sleep(500L);
                }
                catch (InterruptedException e) {
                    HistoricalCache.logger.info((Object)"Snapshot eviction thread interrupted. ", (Throwable)e);
                    return;
                }
            }
            this.qm.removeFromSnapshotIdList();
            if (HistoricalCache.logger.isInfoEnabled()) {
                HistoricalCache.logger.info((Object)("EVICTED SNAPSHOT WITH ID: " + evictSnapshotId));
            }
        }
        catch (MetaDataRepositoryException e2) {
            if (HistoricalCache.logger.isInfoEnabled()) {
                HistoricalCache.logger.info((Object)e2.getMessage());
            }
        }
    }
    
    private ICache createNewCache(final Long localSnapshotId) throws Exception {
        if (HistoricalCache.logger.isDebugEnabled()) {
            HistoricalCache.logger.debug((Object)"Cache switch happens here");
        }
        final int numReplicas = CacheConfiguration.getNumReplicasFromProps(this.cacheLoad.queryProps);
        final MutableConfiguration<Object, List<DARecord>> configuration = new CacheConfiguration<Object, List<DARecord>>(numReplicas, CacheConfiguration.PartitionManagerType.SIMPLE);
        configuration.setStoreByValue(false);
        configuration.setStatisticsEnabled(false);
        ICache newCache;
        if (localSnapshotId == null) {
            newCache = (ICache)this.manager.createCache(this.cacheLoad.getCacheName(), configuration);
        }
        else {
            newCache = (ICache)this.manager.createCache(this.getCacheSnapshotName(this.cacheLoad.getCacheName(), localSnapshotId), configuration);
        }
        if (this.serversList != null) {
            this.manager.startCache(this.getCacheSnapshotName(this.cacheLoad.getCacheName(), localSnapshotId), this.serversList);
        }
        if (HistoricalCache.logger.isDebugEnabled()) {
            HistoricalCache.logger.debug((Object)("Cache switch finished to : " + newCache.getName()));
        }
        return newCache;
    }
    
    static {
        HistoricalCache.logger = Logger.getLogger((Class)HistoricalCache.class);
    }
    
    private class refreshCacheListener implements EntryListener<String, Pair<List<UUID>, Long>>
    {
        public void entryAdded(final EntryEvent<String, Pair<List<UUID>, Long>> event) {
            this.handleCacheRefresh(event);
        }
        
        public void entryRemoved(final EntryEvent<String, Pair<List<UUID>, Long>> event) {
            this.handleCacheRefresh(event);
        }
        
        public void entryUpdated(final EntryEvent<String, Pair<List<UUID>, Long>> event) {
            this.handleCacheRefresh(event);
        }
        
        public void entryEvicted(final EntryEvent<String, Pair<List<UUID>, Long>> event) {
            this.handleCacheRefresh(event);
        }
        
        public void mapEvicted(final MapEvent event) {
            throw new IllegalStateException("This should not be called");
        }
        
        public void mapCleared(final MapEvent event) {
            throw new IllegalStateException("This should not be called");
        }
        
        private void handleCacheRefresh(final EntryEvent<String, Pair<List<UUID>, Long>> event) {
            if (!((String)event.getKey()).equals(HistoricalCache.this.cacheLoad.getCacheName())) {
                return;
            }
            HistoricalCache.this.lock.lock();
            try {
                if (HistoricalCache.logger.isInfoEnabled()) {
                    HistoricalCache.logger.info((Object)String.format(Constants.CALLBACK_SID_RECEIVED, HistoricalCache.this.getCacheInfo().getCacheName(), ((Pair)event.getValue()).second));
                    HistoricalCache.logger.info((Object)String.format(Constants.OLD_SNAPSHOT_ID, HistoricalCache.this.getCacheInfo().getCacheName(), HistoricalCache.this.getLatestSnapshotId()));
                }
                HistoricalCache.this.latestSnapshotId = (long)((Pair)event.getValue()).second;
                HistoricalCache.this.updateLatestSnapshotIdInUse();
                if (HistoricalCache.this.latestSnapshotId != 0L) {
                    HistoricalCache.this.oldCache = HistoricalCache.this.currentCache;
                    HistoricalCache.this.currentCache = (ICache<Object, List<DARecord>>)(ICache)HistoricalCache.this.manager.getCache(HistoricalCache.this.getCacheSnapshotName(HistoricalCache.this.qm.cacheName, HistoricalCache.this.latestSnapshotId));
                }
                if (HistoricalCache.logger.isInfoEnabled()) {
                    HistoricalCache.logger.info((Object)String.format(Constants.NEW_SNAPSHOT_ID, HistoricalCache.this.getCacheInfo().getCacheName(), HistoricalCache.this.getLatestSnapshotId()));
                }
            }
            catch (MetaDataRepositoryException e) {
                HistoricalCache.logger.error((Object)("Failed to update snapshotId in use for id: " + HistoricalCache.this.latestSnapshotId), (Throwable)e);
            }
            finally {
                HistoricalCache.this.lock.unlock();
            }
        }
    }
    
    class CacheRefreshOperator implements Runnable
    {
        private void refreshSetup(final ICountDownLatch countDownLatchForRefresh) {
            if (HistoricalCache.this.isCacheLeader) {
                countDownLatchForRefresh.trySetCount(HistoricalCache.this.serversList.size());
            }
        }
        
        @Override
        public void run() {
            if (HistoricalCache.logger.isDebugEnabled()) {
                HistoricalCache.logger.debug((Object)("before reset : " + HistoricalCache.this.currentCache.size()));
            }
            try {
                final ICountDownLatch countDownLatchForRefresh = HazelcastSingleton.get().getCountDownLatch("#CACHE-LATCH-" + HistoricalCache.this.cacheLoad.cacheName);
                this.refreshSetup(countDownLatchForRefresh);
                if (HistoricalCache.logger.isDebugEnabled()) {
                    HistoricalCache.logger.debug((Object)("before reset : " + countDownLatchForRefresh.getCount()));
                }
                HistoricalCache.this.createNewCache(HistoricalCache.this.latestSnapshotId + 1L);
                if (HistoricalCache.this.isCacheLeader) {
                    while (countDownLatchForRefresh.getCount() == 0) {}
                }
                countDownLatchForRefresh.countDown();
                for (int count = countDownLatchForRefresh.getCount(); count != 0; count = countDownLatchForRefresh.getCount()) {
                    try {
                        Thread.sleep(1000L);
                    }
                    catch (InterruptedException e) {
                        HistoricalCache.logger.info((Object)"Countdown latch thread interrupted. ", (Throwable)e);
                        return;
                    }
                }
                if (HistoricalCache.this.isCacheLeader) {
                    HistoricalCache.this.refresh();
                }
            }
            catch (Exception e2) {
                HistoricalCache.logger.error((Object)"Error executing cache query", (Throwable)e2);
            }
            if (HistoricalCache.logger.isInfoEnabled()) {
                HistoricalCache.logger.info((Object)("after insert : " + HistoricalCache.this.currentCache.size()));
            }
        }
    }
}
