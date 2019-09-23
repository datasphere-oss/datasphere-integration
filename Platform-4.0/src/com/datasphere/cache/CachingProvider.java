package com.datasphere.cache;

import java.util.*;
import javax.cache.*;
import java.net.*;
import javax.cache.configuration.*;

public class CachingProvider implements javax.cache.spi.CachingProvider
{
    private WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> cacheManagersByClassLoader;
    
    public CachingProvider() {
        this.cacheManagersByClassLoader = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();
    }
    // 获得缓存管理器
    public synchronized CacheManager getCacheManager(final URI uri, final ClassLoader classLoader, final Properties properties) {
        final URI cacheManagerURI = (uri == null) ? this.getDefaultURI() : uri;
        final ClassLoader cacheManagerClassLoader = (classLoader == null) ? this.getDefaultClassLoader() : classLoader;
        final Properties cacheManagerProperties = (properties == null) ? this.getDefaultProperties() : properties;
        HashMap<URI, CacheManager> cacheManagersByURI = this.cacheManagersByClassLoader.get(cacheManagerClassLoader);
        if (cacheManagersByURI == null) {
            cacheManagersByURI = new HashMap<URI, CacheManager>();
        }
        CacheManager cacheManager = cacheManagersByURI.get(cacheManagerURI);
        if (cacheManager == null) {
            cacheManager = (CacheManager)new com.datasphere.cache.CacheManager(this, cacheManagerURI, cacheManagerClassLoader, cacheManagerProperties);
            cacheManagersByURI.put(cacheManagerURI, cacheManager);
        }
        if (!this.cacheManagersByClassLoader.containsKey(cacheManagerClassLoader)) {
            this.cacheManagersByClassLoader.put(cacheManagerClassLoader, cacheManagersByURI);
        }
        return cacheManager;
    }
    
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }
    // 获得默认URI
    public URI getDefaultURI() {
        try {
            return new URI(this.getClass().getName());
        }
        catch (URISyntaxException e) {
            throw new CacheException("Failed to create URI for Cache");
        }
    }
    
    public Properties getDefaultProperties() {
        return null;
    }
    
    public CacheManager getCacheManager(final URI uri, final ClassLoader classLoader) {
        return this.getCacheManager(uri, classLoader, this.getDefaultProperties());
    }
    
    public CacheManager getCacheManager() {
        return this.getCacheManager(this.getDefaultURI(), this.getDefaultClassLoader(), this.getDefaultProperties());
    }
    
    public void close() {
        this.close(this.getDefaultURI(), this.getDefaultClassLoader());
    }
    
    public void close(final ClassLoader classLoader) {
        this.close(this.getDefaultURI(), classLoader);
    }
    
    public void close(final URI uri, final ClassLoader classLoader) {
        final URI cacheManagerURI = (uri == null) ? this.getDefaultURI() : uri;
        final ClassLoader cacheManagerClassLoader = (classLoader == null) ? this.getDefaultClassLoader() : classLoader;
        final HashMap<URI, CacheManager> cacheManagersByURI = this.cacheManagersByClassLoader.get(cacheManagerClassLoader);
        final CacheManager cacheManager = cacheManagersByURI.get(cacheManagerURI);
        if (cacheManager != null) {
            cacheManager.close();
            cacheManagersByURI.remove(cacheManagerURI);
        }
        if (cacheManagersByURI.size() == 0) {
            this.cacheManagersByClassLoader.remove(cacheManagerClassLoader);
        }
    }
    
    public boolean isSupported(final OptionalFeature optionalFeature) {
        return false;
    }
}
