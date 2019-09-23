package com.datasphere.historicalcache;

import java.util.List;
import java.util.Map;

import javax.cache.Caching;

import org.apache.log4j.Logger;

import com.datasphere.cache.CacheManager;
import com.datasphere.cache.CachingProvider;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.uuid.UUID;

public class CacheInfo
{
    private static Logger logger;
    final String cacheName;
    final BaseProcess adapter;
    final Map<String, Object> readerProps;
    final Map<String, Object> parserProps;
    final Map<String, Object> queryProps;
    final Channel output;
    final CacheManager manager;
    final Cache cacheObj;
    
    public CacheInfo(final String cacheName, final BaseProcess adapter, final Map<String, Object> readerProps, final Map<String, Object> parserProps, final Map<String, Object> queryProps, final Channel output, final Cache cacheObj) {
        this.cacheObj = cacheObj;
        final CachingProvider provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
        this.manager = (CacheManager)provider.getCacheManager();
        this.cacheName = cacheName;
        this.adapter = adapter;
        this.readerProps = readerProps;
        this.parserProps = parserProps;
        this.queryProps = queryProps;
        this.output = output;
        if (CacheInfo.logger.isInfoEnabled()) {
            if (adapter != null) {
                CacheInfo.logger.info((Object)("Cache " + cacheName + " is created with adapter type : " + adapter.getType()));
            }
            else {
                CacheInfo.logger.info((Object)("EventTable " + cacheName + " is created with"));
            }
        }
    }
    
    public BaseProcess getAdapter() {
        return this.adapter;
    }
    
    public Map<String, Object> getReaderProps() {
        return this.readerProps;
    }
    
    public Map<String, Object> getQueryProps() {
        return this.queryProps;
    }
    
    public String getCacheName() {
        return this.cacheName;
    }
    
    public void stop() throws Exception {
        this.manager.stopCache(this.cacheName);
    }
    
    public void start(final List<UUID> servers) throws Exception {
        this.manager.startCache(this.cacheName, servers);
    }
    
    public void close() {
        this.manager.close(this.cacheName);
    }
    
    public UUID getCuurentAppId() throws MetaDataRepositoryException {
        return this.cacheObj.getCurrentApp().getUuid();
    }
    
    public Channel getOutputChannel() {
        return this.output;
    }
    
    public Cache getCacheObj() {
        return this.cacheObj;
    }
    
    static {
        CacheInfo.logger = Logger.getLogger((Class)CacheInfo.class);
    }
}
