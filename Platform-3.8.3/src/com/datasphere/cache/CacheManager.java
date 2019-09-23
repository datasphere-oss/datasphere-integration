package com.datasphere.cache;

import java.net.*;
import javax.cache.*;
import org.apache.log4j.*;
import javax.cache.configuration.*;
import com.datasphere.metaRepository.*;
import com.hazelcast.core.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.util.*;

public class CacheManager implements javax.cache.CacheManager
{
    private final CachingProvider cachingProvider;
    private final URI cacheManagerURI;
    private final ClassLoader cacheManagerClassLoader;
    private final Properties cacheManagerProperties;
    private final Map<String, Cache<?, ?>> allCaches;
    private static Logger logger;
    private volatile boolean isClosed;
    
    public CacheManager(final CachingProvider waCachingProvider, final URI cacheManagerURI, final ClassLoader cacheManagerClassLoader, final Properties cacheManagerProperties) {
        this.allCaches = new HashMap<String, Cache<?, ?>>();
        this.cachingProvider = waCachingProvider;
        if (cacheManagerURI == null) {
            throw new NullPointerException("No URI is specified for the Cache Manager.");
        }
        this.cacheManagerURI = cacheManagerURI;
        if (cacheManagerClassLoader == null) {
            throw new NullPointerException("No Class Loader is specified for the Cache Manager.");
        }
        this.cacheManagerClassLoader = cacheManagerClassLoader;
        this.cacheManagerProperties = ((cacheManagerProperties == null) ? new Properties() : new Properties(cacheManagerProperties));
        this.isClosed = false;
    }
    
    public javax.cache.spi.CachingProvider getCachingProvider() {
        return (javax.cache.spi.CachingProvider)this.cachingProvider;
    }
    
    public URI getURI() {
        return this.cacheManagerURI;
    }
    
    public ClassLoader getClassLoader() {
        return this.cacheManagerClassLoader;
    }
    
    public Properties getProperties() {
        return this.cacheManagerProperties;
    }
    
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(final String s, final C c) throws IllegalArgumentException {
        if (this.isClosed()) {
            throw new IllegalStateException("Cache is closed!");
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null");
        }
        if (c == null) {
            throw new NullPointerException("Configuration cannot be null");
        }
        if (c instanceof CacheConfiguration) {
            final CacheConfiguration<K, V> cc = (CacheConfiguration<K, V>)c;
            final HazelcastInstance hz = HazelcastSingleton.get();
            final IMap<String, Integer> cacheReplication = hz.getMap("#cacheReplicationFactors");
            try {
                cacheReplication.lock(s);
                final Integer numReplicas = (Integer)cacheReplication.get(s);
                if (numReplicas != null) {
                    cc.setNumReplicas(numReplicas);
                }
                else {
                    cacheReplication.put(s, cc.getNumReplicas());
                }
            }
            finally {
                cacheReplication.unlock(s);
            }
        }
        synchronized (this.allCaches) {
            Cache<K, V> cache = (Cache<K, V>)this.allCaches.get(s);
            if (cache == null) {
                cache = (Cache<K, V>)new CacheAccessor(this, s, this.getClassLoader(), (javax.cache.configuration.Configuration<Object, Object>)c);
                this.allCaches.put(s, cache);
                return cache;
            }
            return cache;
        }
    }
    
    public <K, V> Cache<K, V> getCache(final String s, final Class<K> kClass, final Class<V> vClass) {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        if (kClass == null) {
            throw new NullPointerException("key Type can not be null");
        }
        if (vClass == null) {
            throw new NullPointerException("value Type can not be null");
        }
        synchronized (this.allCaches) {
            final Cache<K, V> cache = (Cache<K, V>)this.allCaches.get(s);
            if (cache == null) {
                return null;
            }
            final Configuration configuration = cache.getConfiguration((Class)CacheConfiguration.class);
            if (configuration.getKeyType() == null || !configuration.getKeyType().equals(kClass)) {
                throw new ClassCastException("Expected " + configuration.getKeyType() + " but " + kClass + " was passed in.");
            }
            if (configuration.getValueType() != null && configuration.getValueType().equals(vClass)) {
                return cache;
            }
            throw new ClassCastException("Expected " + configuration.getValueType() + " but " + vClass + " was passed in.");
        }
    }
    
    public <K, V> Cache<K, V> getCache(final String s) {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        synchronized (this.allCaches) {
            return (Cache<K, V>)this.allCaches.get(s);
        }
    }
    
    public Iterable<String> getCacheNames() {
        synchronized (this.allCaches) {
            final HashSet<String> set = new HashSet<String>();
            for (final Cache<?, ?> cache : this.allCaches.values()) {
                set.add(cache.getName());
            }
            return (Iterable<String>)Collections.unmodifiableSet(set);
        }
    }
    
    public void destroyCache(final String s) {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        synchronized (this.allCaches) {
            final Cache<?, ?> cache = this.allCaches.get(s);
            if (cache != null) {
                cache.close();
            }
        }
    }
    
    public void signalClosed(final String s) {
        synchronized (this.allCaches) {
            this.allCaches.remove(s);
        }
    }
    
    public void enableManagement(final String s, final boolean b) {
    }
    
    public void enableStatistics(final String s, final boolean b) {
    }
    
    public void close() {
        if (!this.isClosed()) {
            this.isClosed = true;
            synchronized (this.allCaches) {
                for (final Cache<?, ?> cache : this.allCaches.values()) {
                    cache.close();
                }
            }
            this.allCaches.clear();
        }
    }
    
    public void stopCache(final String s) throws Exception {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        synchronized (this.allCaches) {
            final Cache<?, ?> cache = this.allCaches.get(s);
            if (cache != null && cache instanceof CacheAccessor) {
                ((CacheAccessor)cache).stop();
            }
        }
    }
    
    public boolean isClosed() {
        return this.isClosed;
    }
    
    public <T> T unwrap(final Class<T> tClass) {
        return null;
    }
    
    public void startCache(final String s, final List<UUID> servers) throws Exception {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        synchronized (this.allCaches) {
            final Cache<?, ?> cache = this.allCaches.get(s);
            if (cache != null && cache instanceof CacheAccessor) {
                ((CacheAccessor)cache).start(servers);
            }
        }
    }
    
    public void startCacheLite(final String s) throws Exception {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        synchronized (this.allCaches) {
            final Cache<?, ?> cache = this.allCaches.get(s);
            if (cache != null && cache instanceof CacheAccessor) {
                ((CacheAccessor)cache).startLite();
            }
        }
    }
    // 关闭缓存
    public void close(final String s) {
        if (this.isClosed()) {
            throw new IllegalStateException();
        }
        if (s == null) {
            throw new NullPointerException("Cache name cannot be null!");
        }
        synchronized (this.allCaches) {
            final List<Cache<?, ?>> cacheList = new ArrayList<Cache<?, ?>>();
            for (final Cache<?, ?> cache : this.allCaches.values()) {
                if (cache != null && cache instanceof CacheAccessor && cache.getName().split("-")[0].equalsIgnoreCase(s)) {
                    cacheList.add(cache);
                }
            }
            for (final Cache<?, ?> cache : cacheList) {
                ((CacheAccessor)cache).close();
                CacheManager.logger.debug(("Closed cache: " + cache.getName()));
            }
        }
    }
    
    static {
        CacheManager.logger = Logger.getLogger((Class)CacheManager.class);
    }
}
