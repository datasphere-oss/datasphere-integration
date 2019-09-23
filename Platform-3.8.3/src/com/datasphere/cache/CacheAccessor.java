package com.datasphere.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.apache.log4j.Logger;

import com.datasphere.distribution.WAIndex;
import com.datasphere.distribution.WAQuery;
import com.datasphere.distribution.WASimpleQuery;
import com.datasphere.exception.CacheNotRunningException;
import com.datasphere.exception.RuntimeInterruptedException;
import com.datasphere.jmqmessaging.Executable;
import com.datasphere.jmqmessaging.Executor;
import com.datasphere.runtime.ConsistentHashRing;
import com.datasphere.runtime.IPartitionManager;
import com.datasphere.runtime.PartitionManager;
import com.datasphere.runtime.SimplePartitionManager;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
/*
 * 缓存访问端点
 */
public class CacheAccessor<K, V> implements ICache<K, V>, ConsistentHashRing.UpdateListener
{
    private static Logger logger;
    // 缓存管理器
    private final CacheManager waCacheManager;
    // 缓存名称
    private final String cacheName;
    // 配置
    private final Configuration<K, V> configuration;
    // 是否关闭
    private volatile boolean isClosed;
    // 是否运行
    private volatile boolean running;
    // 分区管理器
    public transient IPartitionManager pm;
    // 缓存统计信息
    private final CacheStats stats;
    // 内置缓存存储
    private final CacheStorage<K, V> localCache;
    // 缓存访问执行器
    private Executor<CacheAccessor<K, V>> executor;
    
    public CacheAccessor(final CacheManager waCacheManager, final String cacheName, final ClassLoader classLoader, final Configuration<K, V> c) {
        this.running = false;
        this.waCacheManager = waCacheManager;
        this.cacheName = cacheName;
        this.configuration = c;
        int numReplicas = 2;	//副本数量
        CacheConfiguration.PartitionManagerType pmType = CacheConfiguration.PartitionManagerType.LEGACY;
        if (c instanceof CacheConfiguration) {
            numReplicas = ((CacheConfiguration)c).getNumReplicas();
            pmType = ((CacheConfiguration)c).getPartitionManagerType();
        }
        this.isClosed = false;
        // 如果分区类型是LEGACY, 则创建标准的分区管理器；如果分区类型是SIMPLE, 则创建简单的分区管理器
        if (pmType.equals(CacheConfiguration.PartitionManagerType.LEGACY)) {
            this.pm = new PartitionManager(cacheName, numReplicas, this);
        }
        else {
            this.pm = new SimplePartitionManager(cacheName, numReplicas);
        }
        // 添加更新监听器
        this.pm.addUpdateListener(this);
        this.stats = new CacheStats((Cache<?, ?>)this);
        this.localCache = new CacheStorage<K, V>(cacheName, this.pm.getNumberOfPartitions());
        this.executor = new Executor<CacheAccessor<K, V>>(cacheName, this);
    }
    
    @Override
    public CacheStats getStats() {
        return this.stats;
    }
    // 获取本地缓存信息
    public CacheInfo getLocalCacheInfo() {
        try {
            final CacheInfo info = new CacheInfo();
            info.cacheName = this.cacheName;
            info.serverID = this.pm.getLocal();
            info.replicationFactor = this.pm.getNumReplicas();
            info.replicaSizes = new int[this.pm.getNumReplicas()];
            info.replicaParts = new int[this.pm.getNumReplicas()];
            if (CacheAccessor.logger.isInfoEnabled()) {
                CacheAccessor.logger.info((Object)("Local cache info call on " + info.serverID + " for " + this.cacheName));
            }
            for (int i = 0; i < this.localCache.partitions.length; ++i) {
                if (this.localCache.partitions[i] != null) {
                    final int size = this.localCache.partitions[i].size();
                    boolean hasLocal = false;
                    for (int r = 0; r < this.pm.getNumReplicas(); ++r) {
                        if (this.pm.isLocalPartitionById(i, r)) {
                            if (!hasLocal) {
                                final CacheInfo cacheInfo = info;
                                ++cacheInfo.numParts;
                                final CacheInfo cacheInfo2 = info;
                                cacheInfo2.totalSize += size;
                            }
                            hasLocal = true;
                            final int[] replicaSizes = info.replicaSizes;
                            final int n = r;
                            replicaSizes[n] += size;
                            final int[] replicaParts = info.replicaParts;
                            final int n2 = r;
                            ++replicaParts[n2];
                            info.parts.add(new PartInfo(i, size, r));
                        }
                    }
                    if (!hasLocal) {
                        final CacheInfo cacheInfo3 = info;
                        cacheInfo3.totalStaleSize += size;
                        final CacheInfo cacheInfo4 = info;
                        ++cacheInfo4.numStaleParts;
                        info.parts.add(new PartInfo(i, size, -1));
                    }
                }
            }
            for (final Map.Entry<String, WAIndex<?, K, V>> index : this.localCache.indices.entrySet()) {
                info.indices.put(index.getKey(), index.getValue().size());
            }
            if (CacheAccessor.logger.isInfoEnabled()) {
                CacheAccessor.logger.info((Object)("Local cache info call on " + info.serverID + " for " + this.cacheName + ": P=" + info.numParts + " S=" + info.numStaleParts));
            }
            return info;
        }
        catch (Throwable e) {
            CacheAccessor.logger.error((Object)"Error", e);
            throw e;
        }
    }
    // 启动缓存
    public synchronized void start(final List<UUID> servers) throws Exception {
        if (this.running) {
            return;
        }
        CacheAccessor.logger.info((Object)("Got a request to start cache " + this.getName()));
        if (this.pm instanceof SimplePartitionManager) {
            if (servers == null) {
                throw new IllegalStateException("servers list is null");
            }
            CacheAccessor.logger.info((Object)("Start repartitioning for cache " + this.cacheName + " with servers " + servers));
            this.pm.setServers(servers);
            CacheAccessor.logger.info((Object)("done repartitioning for cache " + this.cacheName + " with servers " + servers));
        }
        this.executor.start();
        this.running = true;
    }
    
    public synchronized void startLite() throws Exception {
        if (this.running) {
            return;
        }
        this.executor.start();
        this.running = true;
    }
    // 停止缓存
    public synchronized void stop() throws Exception {
        if (!this.running) {
            return;
        }
        CacheAccessor.logger.info((Object)("Got request to stop the cache " + this.cacheName));
        this.executor.stop();
        this.running = false;
    }
    
    @Override
    public Set<CacheInfo> getCacheInfo() {
        final Set<CacheInfo> results = new HashSet<CacheInfo>();
        final List<Future<CacheInfo>> futures = new ArrayList<Future<CacheInfo>>();
        final GetCacheInfo<K, V> getCacheInfoE = new GetCacheInfo<K, V>();
        for (final UUID uuid : this.pm.getPeers()) {
            futures.add(this.executor.execute(uuid, (Executable<CacheAccessor<K, V>, CacheInfo>)getCacheInfoE));
        }
        for (final Future<CacheInfo> future : futures) {
            try {
                final CacheInfo info = future.get();
                results.add(info);
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)"Problem executing get cache info", (Throwable)ex2);
            }
        }
        return results;
    }
    // 添加索引
    @Override
    public <I> void addIndex(final String name, final WAIndex.Type type, final WAIndex.FieldAccessor<I, V> accessor) {
        this.localCache.addIndex(name, type, accessor, this.pm);
    }
    
    public <I> Map<K, V> getIndexedEqual(final String name, final I value) {
        return this.localCache.getIndexedEqual(name, value);
    }
    // 添加索引范围
    public <I> Map<K, V> getIndexedRange(final String name, final I startVal, final I endVal) {
        return this.localCache.getIndexedRange(name, startVal, endVal);
    }
    // 获得查询结果
    public Map<K, Map<String, Object>> query(final WAQuery<K, V> query) {
        if (!this.running) {
            throw new CacheNotRunningException("Problem getting value for Query" + query + ". Cache is in stopped state");
        }
        final List<Future<Map<K, Map<String, Object>>>> futures = new ArrayList<Future<Map<K, Map<String, Object>>>>();
        final Query<K, V> queryE = new Query<K, V>(query);
        for (final UUID uuid : this.pm.getPeers()) {
            futures.add(this.executor.execute(uuid, (Executable<CacheAccessor<K, V>, Map<K, Map<String, Object>>>)queryE));
        }
        // 查询合并结果
        for (final Future<Map<K, Map<String, Object>>> future : futures) {
            try {
                final Map<K, Map<String, Object>> results = future.get();
                query.mergeResults(results);
            }
            catch (InterruptedException e) {
                CacheAccessor.logger.error((Object)("Problem executing query " + query), (Throwable)e);
                throw new RuntimeInterruptedException(e);
            }
            catch (ExecutionException e2) {
                CacheAccessor.logger.error((Object)("Problem executing query " + query), (Throwable)e2);
                throw new RuntimeException("Problem executing query " + query, e2);
            }
        }
        return query.getResults();
    }
    
    public void simpleQuery(final WASimpleQuery<K, V> query) {
        final List<Future<List<V>>> futures = new ArrayList<Future<List<V>>>();
        final SimpleQuery<K, V> queryE = new SimpleQuery<K, V>(query);
        for (final UUID uuid : this.pm.getPeers()) {
            futures.add(this.executor.execute(uuid, (Executable<CacheAccessor<K, V>, List<V>>)queryE));
        }
        for (final Future<List<V>> future : futures) {
            try {
                final List<V> results = future.get();
                query.mergeQueryResults(results);
            }
            catch (InterruptedException e) {
                CacheAccessor.logger.error((Object)("Problem executing query " + query), (Throwable)e);
                throw new RuntimeInterruptedException(e);
            }
            catch (ExecutionException e2) {
                CacheAccessor.logger.error((Object)("Problem executing query " + query), (Throwable)e2);
                throw new RuntimeException("Problem executing query " + query, e2);
            }
        }
    }
    // 查询信息统计
    public WAQuery.ResultStats queryStats(final WAQuery<K, V> query) {
        final List<Future<WAQuery.ResultStats>> futures = new ArrayList<Future<WAQuery.ResultStats>>();
        final QueryStats<K, V> queryStatsE = new QueryStats<K, V>(query);
        for (final UUID uuid : this.pm.getPeers()) {
            futures.add(this.executor.execute(uuid, (Executable<CacheAccessor<K, V>, WAQuery.ResultStats>)queryStatsE));
        }
        // 查询开始时间和结束时间
        WAQuery.ResultStats res = null;
        for (final Future<WAQuery.ResultStats> future : futures) {
            try {
                final WAQuery.ResultStats resR = future.get();
                if (res == null) {
                    res = resR;
                }
                else {
                    if (res.startTime == 0L || resR.startTime < res.startTime) {
                        res.startTime = resR.startTime;
                    }
                    if (res.endTime == 0L || resR.endTime > res.endTime) {
                        res.endTime = resR.endTime;
                    }
                    final WAQuery.ResultStats resultStats = res;
                    resultStats.count += resR.count;
                }
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)("Problem executing query " + query), (Throwable)ex2);
            }
        }
        return res;
    }
    
    public Set<K> localKeys() {
        final Set<K> keys = new HashSet<K>();
        for (int i = 0; i < this.pm.getNumberOfPartitions(); ++i) {
            if (this.pm.isLocalPartitionById(i, 0)) {
                keys.addAll((Collection<? extends K>)this.localCache.getKeys(i));
            }
        }
        return keys;
    }
    
    public Map<K, V> localEntries() {
        final Map<K, V> entries = new HashMap<K, V>();
        for (int i = 0; i < this.pm.getNumberOfPartitions(); ++i) {
            if (this.pm.isLocalPartitionById(i, 0)) {
                entries.putAll((Map<? extends K, ? extends V>)this.localCache.getAll(i));
            }
        }
        return entries;
    }
    // 获得 Lookup 搜索
    private UUID getLookupWhere(final int partId) {
        if (this.pm.hasLocalPartitionForId(partId)) {
            return this.pm.getLocal();
        }
        return this.pm.getFirstPartitionOwnerForPartition(partId);
    }
    
    public Future<V> futureGet(final K key) {
        final int partId = this.pm.getPartitionId(key);
        final UUID where = this.getLookupWhere(partId);
        final Get<K, V> getE = new Get<K, V>(partId, key);
        final Future<V> future = this.executor.execute(where, (Executable<CacheAccessor<K, V>, V>)getE);
        return future;
    }
    // 计算命中率
    public V get(final K key) {
        if (!this.running) {
            throw new CacheNotRunningException("Problem getting value for " + key + ". Cache is in stopped state");
        }
        if (key == null) {
            return null;
        }
        try {
            final Executor.ExecutableFuture<CacheAccessor<K, V>, V> future = (Executor.ExecutableFuture<CacheAccessor<K, V>, V>)(Executor.ExecutableFuture)this.futureGet(key);
            final V value = future.get();
            if (value == null) {
                this.stats.incrementMisses();
                if (future.isLocal()) {
                    this.stats.incrementLocalMisses();
                }
                else {
                    this.stats.incrementRemoteMisses();
                }
            }
            else {
                this.stats.incrementHits();
                if (future.isLocal()) {
                    this.stats.incrementLocalHits();
                }
                else {
                    this.stats.incrementRemoteHits();
                }
            }
            return value;
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem getting value for " + key), (Throwable)ex2);
            throw new RuntimeException("Problem getting value for " + key, ex2);
        }
    }
    
    public Map<K, V> getAll(final Set<? extends K> ks) {
        if (!this.running) {
            throw new CacheNotRunningException("Problem getting value. Cache is in stopped state");
        }
        if (ks == null) {
            return null;
        }
        if (ks.isEmpty()) {
            return new HashMap<K, V>();
        }
        final Map<K, V> result = new HashMap<K, V>();
        final List<Executor.ExecutableFuture<CacheAccessor<K, V>, V>> futures = new ArrayList<Executor.ExecutableFuture<CacheAccessor<K, V>, V>>();
        for (final K key : ks) {
            futures.add((Executor.ExecutableFuture)this.futureGet(key));
        }
        for (final Executor.ExecutableFuture<CacheAccessor<K, V>, V> future : futures) {
            V value = null;
            final K key2 = (K)((Get)future.getRequest().executable).key;
            try {
                value = future.get();
                if (value == null) {
                    this.stats.incrementMisses();
                    if (future.isLocal()) {
                        this.stats.incrementLocalMisses();
                    }
                    else {
                        this.stats.incrementRemoteMisses();
                    }
                }
                else {
                    this.stats.incrementHits();
                    if (future.isLocal()) {
                        this.stats.incrementLocalHits();
                    }
                    else {
                        this.stats.incrementRemoteHits();
                    }
                }
                if (value == null) {
                    continue;
                }
                result.put(key2, value);
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)("Problem getting value for " + key2), (Throwable)ex2);
            }
        }
        return result;
    }
    // 查询缓存中所有数据
    @Override
    public List<V> getAllData() {
        if (!this.running) {
            throw new CacheNotRunningException("Problem getting value. Cache is in stopped state");
        }
        final List<V> ret = new ArrayList<V>();
        for (final Cache.Entry<K, V> entry : this) {
            ret.add((V)entry.getValue());
        }
        return ret;
    }
    // 获得缓存中所有的键
    @Override
    public Set<K> getAllKeys() {
        if (!this.running) {
            throw new CacheNotRunningException("Problem getting value. Cache is in stopped state");
        }
        final Set<K> result = new HashSet<K>();
        final List<Executor.ExecutableFuture<CacheAccessor<K, V>, Set<K>>> futures = new ArrayList<Executor.ExecutableFuture<CacheAccessor<K, V>, Set<K>>>();
        for (int i = 0; i < this.pm.getNumberOfPartitions(); ++i) {
            final UUID where = this.getLookupWhere(i);
            futures.add((Executor.ExecutableFuture)this.executor.execute(where, (Executable<CacheAccessor<K, V>, Object>)new GetKeys(i)));
        }
        for (final Executor.ExecutableFuture<CacheAccessor<K, V>, Set<K>> future : futures) {
            try {
                final Set<K> keys = future.get();
                if (keys == null) {
                    continue;
                }
                result.addAll((Collection<? extends K>)keys);
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)"Problem getting all keys", (Throwable)ex2);
            }
        }
        return result;
    }
    // 在所有缓存节点中执行
    private <T> Future<T> executeOnAllReplicas(final int partId, final Executable<CacheAccessor<K, V>, T> executable) {
        final List<UUID> uuids = this.pm.getAllReplicas(partId);
        UUID first = null;
        if (uuids.contains(this.pm.getLocal())) {
            first = this.pm.getLocal();
        }
        else {
            first = uuids.get(0);
        }
        uuids.remove(first);
        final Future<T> future = this.executor.execute(first, executable);
        for (final UUID uuid : uuids) {
            this.executor.execute(uuid, executable);
        }
        return future;
    }
    // 缓存基本读写数据方法
    // 放入KV键值对
    public void put(final K key, final V value) {
        if (key == null) {
            return;
        }
        if (value == null) {
            return;
        }
        final int partId = this.pm.getPartitionId(key);
        final Put<K, V> putE = new Put<K, V>(partId, key, value);
        this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, ?>)putE);
        this.stats.incrementPuts();
    }
    
    public V getAndPut(final K key, final V value) {
        if (key == null) {
            return null;
        }
        if (value == null) {
            return null;
        }
        final int partId = this.pm.getPartitionId(key);
        final GetAndPut<K, V> getAndPutE = new GetAndPut<K, V>(partId, key, value);
        final Future<V> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, V>)getAndPutE);
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            CacheAccessor.logger.error((Object)"Cache get/put Interrupted");
            throw new RuntimeInterruptedException(e);
        }
        catch (ExecutionException e2) {
            CacheAccessor.logger.error((Object)("Problem getting and putting value " + value + " for key " + key), (Throwable)e2);
            throw new RuntimeException("Problem getting and putting value " + value + " for key " + key, e2);
        }
    }
    
    public void putAll(final Map<? extends K, ? extends V> map) {
        if (map != null) {
            if (!map.isEmpty()) {
                for (final Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                    this.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }
    
    public boolean putIfAbsent(final K key, final V value) {
        if (key == null) {
            return false;
        }
        if (value == null) {
            return false;
        }
        final int partId = this.pm.getPartitionId(key);
        final PutIfAbsent<K, V> putIfAbsentE = new PutIfAbsent<K, V>(partId, key, value);
        final Future<Boolean> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, Boolean>)putIfAbsentE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem putting if absent value " + value + " for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem putting if absent value " + value + " for key " + key, ex2);
        }
    }
    // 是否包含某键
    public boolean containsKey(final K key) {
        final int partId = this.pm.getPartitionId(key);
        final UUID where = this.getLookupWhere(partId);
        final ContainsKey<K, V> containsKeyE = new ContainsKey<K, V>(partId, key);
        final Future<Boolean> future = this.executor.execute(where, (Executable<CacheAccessor<K, V>, Boolean>)containsKeyE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem with containsKey for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem with containsKey for key " + key, ex2);
        }
    }
    
    public boolean remove(final K key) {
        if (key == null) {
            return false;
        }
        final int partId = this.pm.getPartitionId(key);
        final Remove<K, V> removeE = new Remove<K, V>(partId, key);
        final Future<Boolean> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, Boolean>)removeE);
        this.stats.incrementRemovals();
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem removing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem removing value for key " + key, ex2);
        }
    }
    
    public boolean remove(final K key, final V value) {
        if (key == null) {
            return false;
        }
        if (value == null) {
            return false;
        }
        final int partId = this.pm.getPartitionId(key);
        final RemoveWithValue<K, V> removeE = new RemoveWithValue<K, V>(partId, key, value);
        final Future<Boolean> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, Boolean>)removeE);
        this.stats.incrementRemovals();
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem removing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem removing value for key " + key, ex2);
        }
    }
    
    public V getAndRemove(final K key) {
        if (key == null) {
            return null;
        }
        final int partId = this.pm.getPartitionId(key);
        final GetAndRemove<K, V> removeE = new GetAndRemove<K, V>(partId, key);
        final Future<V> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, V>)removeE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem removing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem removing value for key " + key, ex2);
        }
    }
    // 删除所有值
    public void removeAll(final Set<? extends K> ks) {
        if (ks != null) {
            if (!ks.isEmpty()) {
                for (final K key : ks) {
                    this.remove(key);
                }
            }
        }
    }
    
    public void removeAll() {
        final Clear<K, V> clearE = new Clear<K, V>();
        for (final UUID uuid : this.pm.getPeers()) {
            this.executor.execute(uuid, (Executable<CacheAccessor<K, V>, ?>)clearE);
        }
    }
    // 替换旧值
    public boolean replace(final K key, final V oldValue, final V newValue) {
        if (key == null) {
            return false;
        }
        if (oldValue == null) {
            return false;
        }
        if (newValue == null) {
            return false;
        }
        final int partId = this.pm.getPartitionId(key);
        final Replace<K, V> replaceE = new Replace<K, V>(partId, key, oldValue, newValue);
        final Future<Boolean> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, Boolean>)replaceE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem replacing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem replacing value for key " + key, ex2);
        }
    }
    
    public boolean replace(final K key, final V value) {
        if (key == null) {
            return false;
        }
        if (value == null) {
            return false;
        }
        final int partId = this.pm.getPartitionId(key);
        final Replace<K, V> replaceE = new Replace<K, V>(partId, key, value, null);
        final Future<Boolean> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, Boolean>)replaceE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem replacing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem replacing value for key " + key, ex2);
        }
    }
    
    public V getAndReplace(final K key, final V value) {
        if (key == null) {
            return null;
        }
        if (value == null) {
            return null;
        }
        final int partId = this.pm.getPartitionId(key);
        final GetAndReplace<K, V> replaceE = new GetAndReplace<K, V>(partId, key, value);
        final Future<V> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, V>)replaceE);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem getting and replacing value for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem getting and replacing value for key " + key, ex2);
        }
    }
    // 启动对缓存操作的统计
    @Override
    public boolean statisticsEnabled() {
        return this.getConfiguration(CompleteConfiguration.class).isStatisticsEnabled();
    }
    
    public void loadAll(final Set<? extends K> ks, final boolean b, final CompletionListener completionListener) {
    }
    
    public void clear() {
        this.removeAll();
    }
    
    @Override
    public long size() {
        long size = 0L;
        final List<Executor.ExecutableFuture<CacheAccessor<K, V>, Integer>> futures = new ArrayList<Executor.ExecutableFuture<CacheAccessor<K, V>, Integer>>();
        for (int i = 0; i < this.pm.getNumberOfPartitions(); ++i) {
            final UUID where = this.getLookupWhere(i);
            final Size<K, V> sizeE = new Size<K, V>(i);
            final Future<Integer> future = this.executor.execute(where, (Executable<CacheAccessor<K, V>, Integer>)sizeE);
            futures.add((Executor.ExecutableFuture)future);
        }
        for (final Executor.ExecutableFuture<CacheAccessor<K, V>, Integer> future2 : futures) {
            Integer value = null;
            try {
                value = future2.get();
                size += value;
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)"Problem getting value for size", (Throwable)ex2);
            }
        }
        return size;
    }
    // 获得配置项
    public <C extends Configuration<K, V>> C getConfiguration(final Class<C> cClass) {
        if (cClass.isInstance(this.configuration)) {
            return cClass.cast(this.configuration);
        }
        throw new IllegalArgumentException("The configuration class " + cClass + "is not supported by this implementation.");
    }
    
    public <T> Future<T> futureInvoke(final K key, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) {
        final int partId = this.pm.getPartitionId(key);
        final Invoke<K, V, T> invokeE = new Invoke<K, V, T>(partId, key, kvtEntryProcessor, objects);
        final Future<T> future = this.executeOnAllReplicas(partId, (Executable<CacheAccessor<K, V>, T>)invokeE);
        return future;
    }
    
    public <T> T invoke(final K key, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) throws EntryProcessorException {
        final Future<T> future = this.futureInvoke(key, kvtEntryProcessor, objects);
        try {
            return future.get();
        }
        catch (InterruptedException | ExecutionException ex2) {
            CacheAccessor.logger.error((Object)("Problem with invoke for key " + key), (Throwable)ex2);
            throw new RuntimeException("Problem with invoke for key " + key, ex2);
        }
    }
    
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(final Set<? extends K> ks, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) {
        if (ks == null) {
            return null;
        }
        if (ks.isEmpty()) {
            return new HashMap<K, EntryProcessorResult<T>>();
        }
        final Map<K, EntryProcessorResult<T>> results = new HashMap<K, EntryProcessorResult<T>>();
        for (final K key : ks) {
            final Future<T> future = this.futureInvoke(key, kvtEntryProcessor, objects);
            final EntryProcessorResult<T> result = (EntryProcessorResult<T>)new EntryProcessorResult<T>() {
                public T get() throws EntryProcessorException {
                    try {
                        return future.get();
                    }
                    catch (Throwable t) {
                        throw new EntryProcessorException("Problem invoking operation", t);
                    }
                }
            };
            results.put(key, result);
        }
        return results;
    }
    
    public String getName() {
        return this.cacheName;
    }
    // 使用JVM CacheManager 作为缓存管理器
    public javax.cache.CacheManager getCacheManager() {
        return (javax.cache.CacheManager)this.waCacheManager;
    }
    
    public void close() {
        try {
            this.stop();
        }
        catch (Exception e) {
            CacheAccessor.logger.error((Object)("Stop failed with exception: " + e.getMessage()), (Throwable)e);
        }
        this.executor.close();
        this.executor = null;
        this.pm.doShutdown();
        this.localCache.clear();
        this.isClosed = true;
        this.waCacheManager.signalClosed(this.cacheName);
    }
    
    public boolean isClosed() {
        return this.isClosed;
    }
    
    public <T> T unwrap(final Class<T> tClass) {
        return null;
    }
    // 注册缓存条目监听器
    public void registerCacheEntryListener(final CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
    }
    // 注销缓存条目监听器
    public void deregisterCacheEntryListener(final CacheEntryListenerConfiguration<K, V> kvCacheEntryListenerConfiguration) {
    }
    
    public Iterator<Cache.Entry<K, V>> iterator() {
        return new CacheIterator();
    }
    
    @Override
    public Iterator<Cache.Entry<K, V>> iterator(final Filter<K, V> filter) {
        return new CacheIterator(filter);
    }
    // 更新一致性哈希环
    @Override
    public void update(final ConsistentHashRing.Update update) {
        final Map<K, V> map = this.localCache.getAll(update.partId);
        for (final Map.Entry<K, V> entry : map.entrySet()) {
            final Put<K, V> put = new Put<K, V>(update.partId, entry.getKey(), entry.getValue());
            this.executor.execute(update.to, (Executable<CacheAccessor<K, V>, ?>)put);
        }
    }
    
    @Override
    public void updateStart() {
    }
    
    @Override
    public void updateEnd() {
    }
    
    static {
        CacheAccessor.logger = Logger.getLogger((Class)CacheAccessor.class);
    }
    
    public static class GetCacheInfo<K, V> extends Executable<CacheAccessor<K, V>, CacheInfo> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 3370459232723650619L;
        
        public GetCacheInfo() {
        }
        
        public GetCacheInfo(final int partId) {
        }
        
        public void write(final Kryo kryo, final Output output) {
        }
        
        public void read(final Kryo kryo, final Input input) {
        }
        
        @Override
        public CacheInfo call() {
            return this.on().getLocalCacheInfo();
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class Query<K, V> extends Executable<CacheAccessor<K, V>, Map<K, Map<String, Object>>> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -1211445453738758129L;
        WAQuery<K, V> query;
        
        public Query() {
        }
        
        public Query(final WAQuery<K, V> query) {
            this.query = query;
        }
        
        public void write(final Kryo kryo, final Output output) {
            kryo.writeClassAndObject(output, (Object)this.query);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.query = (WAQuery<K, V>)kryo.readClassAndObject(input);
        }
        
        @Override
        public Map<K, Map<String, Object>> call() {
            this.query.run(this.on());
            final Map<K, Map<String, Object>> results = this.query.getResults();
            return results;
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class SimpleQuery<K, V> extends Executable<CacheAccessor<K, V>, List<V>> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -1211445453738758129L;
        WASimpleQuery<K, V> query;
        
        public SimpleQuery() {
        }
        
        public SimpleQuery(final WASimpleQuery<K, V> query) {
            this.query = query;
        }
        
        public void write(final Kryo kryo, final Output output) {
            kryo.writeClassAndObject(output, (Object)this.query);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.query = (WASimpleQuery<K, V>)kryo.readClassAndObject(input);
        }
        
        @Override
        public List<V> call() {
            return this.query.executeQuery(this.on());
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 计算查询统计
    public static class QueryStats<K, V> extends Executable<CacheAccessor<K, V>, WAQuery.ResultStats> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -1211445453738758129L;
        WAQuery<K, V> query;
        
        public QueryStats() {
        }
        
        public QueryStats(final WAQuery<K, V> query) {
            this.query = query;
        }
        
        public void write(final Kryo kryo, final Output output) {
            kryo.writeClassAndObject(output, (Object)this.query);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.query = (WAQuery<K, V>)kryo.readClassAndObject(input);
        }
        
        @Override
        public WAQuery.ResultStats call() {
            return this.query.getResultStats(this.on());
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 获得KV键值对
    public static class Get<K, V> extends Executable<CacheAccessor<K, V>, V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -3419228679622253410L;
        int partId;
        K key;
        
        public Get() {
        }
        
        public Get(final int partId, final K key) {
            this.partId = partId;
            this.key = key;
        }
        // 读取写入操作，采用Kryo进行序列化
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
        }
        // 读取写入操作，采用Kryo进行序列化
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
        }
        
        @Override
        public V call() {
            return this.on().localCache.get(this.partId, this.key);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class GetKeys<K, V> extends Executable<CacheAccessor<K, V>, Set<K>> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -5918407631356834191L;
        int partId;
        
        public GetKeys() {
        }
        
        public GetKeys(final int partId) {
            this.partId = partId;
        }
        // 读取写入操作，采用Kryo进行序列化
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
        }
        // 读取写入操作，采用Kryo进行序列化
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
        }
        
        @Override
        public Set<K> call() {
            return this.on().localCache.getKeys(this.partId);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class Put<K, V> extends Executable<CacheAccessor<K, V>, Void> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -6719130226518833311L;
        int partId;
        K key;
        V value;
        
        public Put() {
        }
        
        public Put(final int partId, final K key, final V value) {
            this.partId = partId;
            this.key = key;
            this.value = value;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.value);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.value = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public Void call() {
            this.on().localCache.put(this.partId, this.key, this.value);
            return null;
        }
        
        @Override
        public boolean hasResponse() {
            return false;
        }
    }
    
    public static class GetAndPut<K, V> extends Executable<CacheAccessor<K, V>, V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 52199364398826039L;
        int partId;
        K key;
        V value;
        
        public GetAndPut() {
        }
        
        public GetAndPut(final int partId, final K key, final V value) {
            this.partId = partId;
            this.key = key;
            this.value = value;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.value);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.value = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public V call() {
            return this.on().localCache.getAndPut(this.partId, this.key, this.value);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class PutIfAbsent<K, V> extends Executable<CacheAccessor<K, V>, Boolean> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -862917368429223418L;
        int partId;
        K key;
        V value;
        
        public PutIfAbsent() {
        }
        
        public PutIfAbsent(final int partId, final K key, final V value) {
            this.partId = partId;
            this.key = key;
            this.value = value;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.value);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.value = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public Boolean call() {
            return this.on().localCache.putIfAbsent(this.partId, this.key, this.value);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 包含键位
    public static class ContainsKey<K, V> extends Executable<CacheAccessor<K, V>, Boolean> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -4588200662069927029L;
        int partId;
        K key;
        
        public ContainsKey() {
        }
        
        public ContainsKey(final int partId, final K key) {
            this.partId = partId;
            this.key = key;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
        }
        
        @Override
        public Boolean call() {
            return this.on().localCache.containsKey(this.partId, this.key);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 删除KV对
    public static class Remove<K, V> extends Executable<CacheAccessor<K, V>, Boolean> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 966819231816972979L;
        int partId;
        K key;
        
        public Remove() {
        }
        
        public Remove(final int partId, final K key) {
            this.partId = partId;
            this.key = key;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
        }
        
        @Override
        public Boolean call() {
            return this.on().localCache.remove(this.partId, this.key);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 删除带有值的键值对
    public static class RemoveWithValue<K, V> extends Executable<CacheAccessor<K, V>, Boolean> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 4449649821468236455L;
        int partId;
        K key;
        V value;
        
        public RemoveWithValue() {
        }
        
        public RemoveWithValue(final int partId, final K key, final V value) {
            this.partId = partId;
            this.key = key;
            this.value = value;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.value);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.value = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public Boolean call() {
            return this.on().localCache.remove(this.partId, this.key, this.value);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class GetAndRemove<K, V> extends Executable<CacheAccessor<K, V>, V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -5761092406724838087L;
        int partId;
        K key;
        
        public GetAndRemove() {
        }
        
        public GetAndRemove(final int partId, final K key) {
            this.partId = partId;
            this.key = key;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
        }
        
        @Override
        public V call() {
            return this.on().localCache.getAndRemove(this.partId, this.key);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 清除键值对
    public static class Clear<K, V> extends Executable<CacheAccessor<K, V>, Void> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -1508021321438448281L;
        
        public void write(final Kryo kryo, final Output output) {
        }
        
        public void read(final Kryo kryo, final Input input) {
        }
        
        @Override
        public Void call() {
            this.on().localCache.clear();
            return null;
        }
        
        @Override
        public boolean hasResponse() {
            return false;
        }
    }
    // 替换键值对
    public static class Replace<K, V> extends Executable<CacheAccessor<K, V>, Boolean> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 2688349518552424470L;
        int partId;
        K key;
        V v;
        V v2;
        
        public Replace() {
        }
        
        public Replace(final int partId, final K key, final V v, final V v2) {
            this.partId = partId;
            this.key = key;
            this.v = v;
            this.v2 = v2;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.v);
            kryo.writeClassAndObject(output, (Object)this.v2);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.v = (V)kryo.readClassAndObject(input);
            this.v2 = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public Boolean call() {
            if (this.v2 != null) {
                return this.on().localCache.replace(this.partId, this.key, this.v, this.v2);
            }
            return this.on().localCache.replace(this.partId, this.key, this.v);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class GetAndReplace<K, V> extends Executable<CacheAccessor<K, V>, V> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 3625283743876563517L;
        int partId;
        K key;
        V v;
        
        public GetAndReplace() {
        }
        
        public GetAndReplace(final int partId, final K key, final V v) {
            this.partId = partId;
            this.key = key;
            this.v = v;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.v);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.v = (V)kryo.readClassAndObject(input);
        }
        
        @Override
        public V call() {
            return this.on().localCache.getAndReplace(this.partId, this.key, this.v);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 计算缓存大小
    public static class Size<K, V> extends Executable<CacheAccessor<K, V>, Integer> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 3370459232723650619L;
        int partId;
        
        public Size() {
        }
        
        public Size(final int partId) {
            this.partId = partId;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
        }
        
        @Override
        public Integer call() {
            return this.on().localCache.size(this.partId);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class Invoke<K, V, T> extends Executable<CacheAccessor<K, V>, T> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 3370459232723650619L;
        int partId;
        K key;
        EntryProcessor<K, V, T> kvtEntryProcessor;
        Object[] objects;
        
        public Invoke() {
        }
        
        public Invoke(final int partId, final K key, final EntryProcessor<K, V, T> kvtEntryProcessor, final Object... objects) {
            this.partId = partId;
            this.key = key;
            this.kvtEntryProcessor = kvtEntryProcessor;
            this.objects = objects;
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.key);
            kryo.writeClassAndObject(output, (Object)this.kvtEntryProcessor);
            kryo.writeClassAndObject(output, (Object)this.objects);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.key = (K)kryo.readClassAndObject(input);
            this.kvtEntryProcessor = (EntryProcessor<K, V, T>)kryo.readClassAndObject(input);
            this.objects = (Object[])kryo.readClassAndObject(input);
        }
        
        @Override
        public T call() {
            return this.on().localCache.invoke(this.partId, this.key, this.kvtEntryProcessor, this.objects);
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    
    public static class GetAll<K, V> extends Executable<CacheAccessor<K, V>, Map<K, V>> implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = -3419228679622253410L;
        int partId;
        Filter<K, V> filter;
        
        public GetAll() {
        }
        
        public GetAll(final int partId, final Filter<K, V> filter) {
            this.partId = partId;
            this.filter = filter;
        }
        
        public GetAll(final int partId) {
            this(partId, null);
        }
        
        public void write(final Kryo kryo, final Output output) {
            output.writeInt(this.partId);
            kryo.writeClassAndObject(output, (Object)this.filter);
        }
        
        public void read(final Kryo kryo, final Input input) {
            this.partId = input.readInt();
            this.filter = (Filter<K, V>)kryo.readClassAndObject(input);
        }
        
        @Override
        public Map<K, V> call() {
            final Map<K, V> all = this.on().localCache.getAll(this.partId);
            Map<K, V> results = null;
            if (this.filter != null) {
                results = new HashMap<K, V>();
                for (final Map.Entry<K, V> entry : all.entrySet()) {
                    if (this.filter.matches(entry.getKey(), entry.getValue())) {
                        results.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            else {
                results = all;
            }
            return results;
        }
        
        @Override
        public boolean hasResponse() {
            return true;
        }
    }
    // 缓存条目
    public class CacheEntry implements Cache.Entry<K, V>
    {
        final K k;
        final V v;
        
        public CacheEntry(final K k, final V v) {
            this.k = k;
            this.v = v;
        }
        
        public K getKey() {
            return this.k;
        }
        
        public V getValue() {
            return this.v;
        }
        
        public <T> T unwrap(final Class<T> clazz) {
            return null;
        }
    }
    
    public class CacheIterator implements Iterator<Cache.Entry<K, V>>
    {
        Filter<K, V> filter;
        int partId;
        Map<K, V> currentPart;
        Future<Map<K, V>> nextPart;
        Iterator<Map.Entry<K, V>> currentIterator;
        
        Future<Map<K, V>> getPart(final int part) {
            final UUID where = CacheAccessor.this.getLookupWhere(part);
            final GetAll<K, V> getAllE = new GetAll<K, V>(part, this.filter);
            final Future<Map<K, V>> future = CacheAccessor.this.executor.execute(where, getAllE);
            return future;
        }
        
        public CacheIterator() {
           
        }
        
        public CacheIterator(final Filter<K, V> filter) {
            this.partId = 0;
            this.currentPart = null;
            this.nextPart = null;
            this.currentIterator = null;
            this.filter = filter;
            final Future<Map<K, V>> currentPartFuture = this.getPart(this.partId++);
            this.nextPart = this.getPart(this.partId++);
            try {
                this.currentPart = currentPartFuture.get();
            }
            catch (InterruptedException | ExecutionException ex2) {
                CacheAccessor.logger.error((Object)"Problem retrieving initial partition", (Throwable)ex2);
            }
        }
        
        @Override
        public boolean hasNext() {
            if (this.currentIterator == null) {
                if (this.currentPart == null) {
                    return false;
                }
                this.currentIterator = this.currentPart.entrySet().iterator();
            }
            if (this.currentIterator.hasNext()) {
                return true;
            }
            if (this.nextPart != null) {
                try {
                    this.currentPart = this.nextPart.get();
                    this.currentIterator = null;
                    if (this.partId < CacheAccessor.this.pm.getNumberOfPartitions()) {
                        this.nextPart = this.getPart(this.partId++);
                    }
                    else {
                        this.nextPart = null;
                    }
                    return this.hasNext();
                }
                catch (InterruptedException | ExecutionException ex2) {
                    CacheAccessor.logger.error((Object)"Problem retrieving next partition", (Throwable)ex2);
                }
            }
            return false;
        }
        
        @Override
        public Cache.Entry<K, V> next() {
            if (this.currentIterator == null) {
                throw new RuntimeException("Current Iterator is null");
            }
            final Map.Entry<K, V> mapEntry = this.currentIterator.next();
            return (Cache.Entry<K, V>)new CacheEntry(mapEntry.getKey(), mapEntry.getValue());
        }
        
        @Override
        public void remove() {
            final Map.Entry<K, V> mapEntry = this.currentIterator.next();
            CacheAccessor.this.remove(mapEntry.getKey());
        }
    }
}
