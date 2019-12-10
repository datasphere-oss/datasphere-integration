package com.datasphere.historicalcache;

import com.datasphere.runtime.channels.*;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.proc.*;
import com.datasphere.utility.*;
import com.datasphere.utility.Utility;

import java.util.concurrent.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.anno.*;
import com.datasphere.cache.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.*;
import com.datasphere.proc.events.commands.*;
import com.datasphere.errorhandling.*;
import com.datasphere.common.errors.*;
import com.datasphere.security.*;
import com.datasphere.runtime.components.*;
import com.datasphere.hdstore.exceptions.*;
import com.datasphere.hdstore.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.util.*;
import com.datasphere.metaRepository.*;

@PropertyTemplate(name = "cache", type = AdapterType.hdstore, properties = { @PropertyTemplateProperty(name = "REPLICA", type = Integer.class, required = false, defaultValue = "2"), @PropertyTemplateProperty(name = "INSERT_RATE", type = Integer.class, required = false, defaultValue = "100000"), @PropertyTemplateProperty(name = "INSERT_INFO", type = Boolean.class, required = false, defaultValue = "false") })
public class Cache extends FlowComponent implements PubSub, Channel.NewSubscriberAddedCallback, Compound, Restartable
{
    private static Logger logger;
    private final Channel output;
    public final MetaInfo.Cache cacheMetaInfo;
    private final HistoricalCache historicalCacheInstance;
    private final BaseProcess cacheAdapter;
    public final MetaInfo.Type KVStorageType;
    private volatile long lastRefreshTime;
    private Publisher dataSource;
    private volatile boolean running;
    private Boolean insertInfo;
    Long prevSize;
    Long prevLh;
    Long prevLhRate;
    Long prevLm;
    Long prevLmRate;
    Long prevRh;
    Long prevRhRate;
    Long prevRm;
    Long prevRmRate;
    Long prevLrt;
    volatile long cacheSize;
    
    public Cache(final MetaInfo.Cache cacheMetaInfo, final BaseServer srv, final BaseProcess cacheAdapter) throws Exception {
        super(srv, cacheMetaInfo);
        this.lastRefreshTime = 0L;
        this.running = false;
        this.insertInfo = null;
        this.prevSize = null;
        this.prevLh = null;
        this.prevLhRate = null;
        this.prevLm = null;
        this.prevLmRate = null;
        this.prevRh = null;
        this.prevRhRate = null;
        this.prevRm = null;
        this.prevRmRate = null;
        this.prevLrt = null;
        this.cacheSize = 0L;
        this.KVStorageType = (MetaInfo.Type)srv.getObject(cacheMetaInfo.getTypename());
        this.cacheMetaInfo = cacheMetaInfo;
        this.setInsertInfo((cacheMetaInfo.getQuery_properties().get("INSERT_INFO") == null) ? null : Utility.getPropertyBoolean(cacheMetaInfo.getQuery_properties(), "INSERT_INFO"));
        this.cacheMetaInfo.getReader_properties().put("positionbyeof", false);
        this.cacheMetaInfo.getReader_properties().put("breakonnorecord", true);
        this.cacheAdapter = cacheAdapter;
        (this.output = srv.createChannel(this)).addCallback(this);
        this.historicalCacheInstance = new HistoricalCache(srv);
    }
    
    public void registerAndInitCache() throws Exception {
        PartitionManager.registerCache(this.cacheMetaInfo.getFullName(), this.cacheMetaInfo.uuid);
        if (this.cacheAdapter != null) {
            final Map<String, Object> reader_prop = this.cacheMetaInfo.reader_properties;
            reader_prop.put("AdapterClassName", this.cacheMetaInfo.adapterClassName);
        }
        this.historicalCacheInstance.init(this.cacheMetaInfo.reader_properties, this.cacheMetaInfo.parser_properties, this.cacheMetaInfo.query_properties, this.cacheAdapter, this.cacheMetaInfo.getFullName(), this.output, this);
        this.lastRefreshTime = System.currentTimeMillis();
    }
    
    public ScheduledExecutorService getScheduler() {
        return this.srv().getScheduler();
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void close() throws Exception {
        this.output.close();
        if (this.cacheAdapter != null) {
            this.cacheAdapter.close();
        }
        this.historicalCacheInstance.close();
    }
    
    @Override
    public void notifyMe(final Link link) {
        if (Cache.logger.isInfoEnabled()) {
            Cache.logger.info((Object)("new subscriber attached to cache " + this.getMetaName()));
        }
        final Range range = Range.createRange(this);
        final TaskEvent ws = TaskEvent.createWindowStateEvent(range);
        try {
            link.subscriber.receive(link.linkID, (ITaskEvent)ws);
        }
        catch (Exception e) {
            Cache.logger.error((Object)"Problem receiving data from cache", (Throwable)e);
        }
    }
    
    public void setCacheSize(final long size) {
        this.cacheSize = size;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final CacheInfo hCacheInfo = this.historicalCacheInstance.getCacheInfo();
        if (hCacheInfo != null) {
            final Long size = this.cacheSize;
            final CacheStats stats = this.historicalCacheInstance.currentCache.getStats();
            final Long lh = stats.getLocalHits();
            final Long lm = stats.getLocalMisses();
            final Long rh = stats.getRemoteHits();
            final Long rm = stats.getRemoteMisses();
            Long rate = null;
            boolean showedActivity = false;
            final long timeStamp = monEvs.getTimeStamp();
            if (!lh.equals(this.prevLh)) {
                monEvs.add(MonitorEvent.Type.LOCAL_HITS, lh);
                showedActivity = true;
                if (this.prevLh != null && this.prevTimeStamp != null) {
                    final Long lhRate = 1000L * (lh - this.prevLh) / (timeStamp - this.prevTimeStamp);
                    if (!lhRate.equals(this.prevLhRate)) {
                        rate = lhRate;
                        monEvs.add(MonitorEvent.Type.LOCAL_HITS_RATE, lhRate);
                    }
                    this.prevLhRate = lhRate;
                }
                this.prevLh = lh;
            }
            if (!lm.equals(this.prevLm)) {
                monEvs.add(MonitorEvent.Type.LOCAL_MISSES, lm);
                showedActivity = true;
                if (this.prevLm != null && this.prevTimeStamp != null) {
                    final Long lmRate = 1000L * (lm - this.prevLm) / (timeStamp - this.prevTimeStamp);
                    if (!lmRate.equals(this.prevLmRate)) {
                        monEvs.add(MonitorEvent.Type.LOCAL_MISSES_RATE, lmRate);
                    }
                    this.prevLmRate = lmRate;
                }
                this.prevLm = lm;
            }
            if (!rh.equals(this.prevRh)) {
                monEvs.add(MonitorEvent.Type.REMOTE_HITS, rh);
                showedActivity = true;
                if (this.prevRh != null && this.prevTimeStamp != null) {
                    final Long rhRate = 1000L * (rh - this.prevRh) / (timeStamp - this.prevTimeStamp);
                    if (!rhRate.equals(this.prevRhRate)) {
                        if (rate != null) {
                            rate += rhRate;
                        }
                        else {
                            rate = rhRate;
                        }
                        monEvs.add(MonitorEvent.Type.REMOTE_HITS_RATE, rhRate);
                    }
                    this.prevRhRate = rhRate;
                }
                this.prevRh = rh;
            }
            if (!rm.equals(this.prevRm)) {
                monEvs.add(MonitorEvent.Type.REMOTE_MISSES, rm);
                showedActivity = true;
                if (this.prevRm != null && this.prevTimeStamp != null) {
                    final Long rmRate = 1000L * (rm - this.prevRm) / (timeStamp - this.prevTimeStamp);
                    if (!rmRate.equals(this.prevRmRate)) {
                        monEvs.add(MonitorEvent.Type.REMOTE_MISSES_RATE, rmRate);
                    }
                    this.prevRmRate = rmRate;
                }
                this.prevRm = rm;
            }
            if (this.prevLrt == null || this.lastRefreshTime != this.prevLrt) {
                monEvs.add(MonitorEvent.Type.CACHE_REFRESH, Long.valueOf(this.lastRefreshTime));
                showedActivity = true;
                this.prevLrt = this.lastRefreshTime;
            }
            if (!size.equals(this.prevSize)) {
                monEvs.add(MonitorEvent.Type.CACHE_SIZE, size);
                this.prevSize = size;
            }
            if (rate != null) {
                monEvs.add(MonitorEvent.Type.RATE, rate);
            }
            if (showedActivity) {
                monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
            }
        }
    }
    
    public IBatch get(final Long transactionId) {
        try {
            this.setProcessThread();
            return (IBatch)this.historicalCacheInstance.get(transactionId);
        }
        catch (Exception ex) {
            Cache.logger.error((Object)("exception receiving events by cache:" + this.cacheMetaInfo.getFullName()));
            this.notifyAppMgr(EntityType.CACHE, this.getCacheName(), this.getMetaID(), ex, "cache get all", transactionId);
            throw new RuntimeException(ex);
        }
    }
    
    public IBatch get(final RecordKey key, final Long transactionId) {
        try {
            this.setProcessThread();
            return this.historicalCacheInstance.get((RecordKey)new CacheKey(key.singleField), transactionId);
        }
        catch (Exception ex) {
            Cache.logger.error((Object)("exception receiving events by cache:" + this.cacheMetaInfo.getFullName()));
            this.notifyAppMgr(EntityType.CACHE, this.getCacheName(), this.getMetaID(), ex, "cache get all", transactionId, key);
            throw new RuntimeException(ex);
        }
    }
    
    public String getCacheName() {
        return this.cacheMetaInfo.getFullName();
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        try {
            this.historicalCacheInstance.upsert(event);
        }
        catch (Exception ex) {
            this.notifyAppMgr(EntityType.CACHE, this.getCacheName(), this.getMetaID(), ex, "cache receive", linkID, event);
            throw new DSSException((IError)CacheError.UPSERT_FAIL, (Throwable)ex, this.getMetaFullName(), (String[])null);
        }
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        final String nameOfStream = (String)this.cacheMetaInfo.getReader_properties().get("NAME");
        if (nameOfStream != null) {
            MetaInfo.Stream stream;
            if (!nameOfStream.contains(".")) {
                stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, this.getMetaNsName(), nameOfStream, null, HSecurityManager.TOKEN);
            }
            else {
                stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, Utility.splitDomain(nameOfStream), Utility.splitName(nameOfStream), null, HSecurityManager.TOKEN);
            }
            if (stream != null) {
                this.dataSource = flow.getPublisher(stream.getUuid());
            }
        }
    }
    
    @Override
    public void start() throws Exception {
        if (this.dataSource == null) {
            return;
        }
        if (this.running) {
            return;
        }
        this.srv().subscribe(this.dataSource, this);
        this.running = true;
    }
    
    public static void drop(final MetaInfo.Cache cacheMetaObject) {
        if (cacheMetaObject.adapterClassName == null && cacheMetaObject.backedByElasticSearch()) {
            final Map<String, Object> properties = cacheMetaObject.getQuery_properties();
            final String cacheName = "$Internal." + cacheMetaObject.getFullName();
            try {
                final HDStoreManager manager = HDStores.getInstance(properties);
                manager.remove(cacheName);
            }
            catch (HDStoreException e) {
                Cache.logger.info((Object)("Problem removing HDStore " + cacheName), (Throwable)e);
            }
        }
    }
    
    @Override
    public void stop() throws Exception {
        if (this.dataSource == null) {
            return;
        }
        if (!this.running) {
            return;
        }
        this.srv().unsubscribe(this.dataSource, this);
        this.running = false;
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    public Publisher getDataSource() {
        return this.dataSource;
    }
    
    public UUID getTypename() {
        return this.cacheMetaInfo.getTypename();
    }
    
    public void startCache(final List<UUID> servers) throws Exception {
        this.historicalCacheInstance.start(servers);
    }
    
    public void stopCache() throws Exception {
        this.historicalCacheInstance.stop();
    }
    
    public MetaInfo.Flow getCurrentApp() throws MetaDataRepositoryException {
        return this.cacheMetaInfo.getCurrentApp();
    }
    
    public Boolean getInsertInfo() {
        return this.insertInfo;
    }
    
    public void setInsertInfo(final Boolean insertInfo) {
        this.insertInfo = insertInfo;
    }
    
    public void setLastRefreshTime(final long lastRefreshTime) {
        this.lastRefreshTime = lastRefreshTime;
    }
    
    public long beginTransaction() {
        return this.historicalCacheInstance.beginTransaction();
    }
    
    public void endTransaction(final long transactionId) {
        this.historicalCacheInstance.endTransaction(transactionId);
    }
    
    public long getLatestSnapshotId() {
        return this.historicalCacheInstance.getLatestSnapshotId();
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    static {
        Cache.logger = Logger.getLogger((Class)Cache.class);
    }
}
