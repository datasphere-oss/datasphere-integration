package com.datasphere.hdstore;

import org.apache.log4j.*;
import java.util.concurrent.atomic.*;
import com.datasphere.hdstore.exceptions.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

public final class HDStores
{
    private static final Class<HDStores> thisClass;
    private static final Logger logger;
    private static final String CLASS_NAME;
    private static final Map<String, HDStoreManager> instanceCache;
    private static final AtomicBoolean started;
    private static final Map<BackgroundTask, String> backgroundTasks;
    
    public static void startup() {
        synchronized (HDStores.started) {
            if (HDStores.started.get()) {
                return;
            }
            HDStores.logger.info((Object)"Starting default storage provider");
            HDStores.started.set(true);
            final HDStoreManager defaultInstance = getInstance("elasticsearch", null);
            final String[] hdStoreNames = defaultInstance.getNames();
            for (int i = 0; i < hdStoreNames.length; ++i) {
                final String hdStoreName = hdStoreNames[i];
                HDStores.logger.info((Object)String.format("Discovered HDStore '%s'", hdStoreName));
            }
            HDStores.logger.info((Object)String.format("Default storage provider, '%s', started", defaultInstance.getInstanceName()));
        }
    }
    
    public static void shutdown() {
        synchronized (HDStores.started) {
            for (final HDStoreManager manager : HDStores.instanceCache.values()) {
                manager.shutdown();
            }
            HDStores.instanceCache.clear();
            HDStores.started.set(false);
        }
    }
    
    public static synchronized com.datasphere.hdstore.elasticsearch.HDStoreManager getAnyElasticsearchInstance() {
        for (final Map.Entry<String, HDStoreManager> entry : HDStores.instanceCache.entrySet()) {
            if (entry.getValue() instanceof com.datasphere.hdstore.elasticsearch.HDStoreManager) {
                return (com.datasphere.hdstore.elasticsearch.HDStoreManager)entry.getValue();
            }
        }
        return null;
    }
    
    public static synchronized List<com.datasphere.hdstore.elasticsearch.HDStoreManager> getAllElasticsearchInstances() {
        final List<com.datasphere.hdstore.elasticsearch.HDStoreManager> result = new ArrayList<com.datasphere.hdstore.elasticsearch.HDStoreManager>();
        for (final Map.Entry<String, HDStoreManager> entry : HDStores.instanceCache.entrySet()) {
            if (entry.getValue() instanceof com.datasphere.hdstore.elasticsearch.HDStoreManager) {
                result.add((com.datasphere.hdstore.elasticsearch.HDStoreManager)entry.getValue());
            }
        }
        return result;
    }
    
    public static synchronized HDStoreManager getInstance(final String instanceProviderName, final Map<String, Object> instanceProperties) {
        final String providerClassName = getProviderClassName(instanceProviderName);
        HDStoreManager instance;
        try {
            final Class<? extends HDStoreManager> providerClass = (Class<? extends HDStoreManager>)Class.forName(providerClassName, false, ClassLoader.getSystemClassLoader());
            final String instanceName = getInstanceName(instanceProviderName, instanceProperties);
            instance = HDStores.instanceCache.get(instanceName);
            if (instance == null) {
                final HDStoreManager newInstance = (HDStoreManager)providerClass.getConstructor(String.class, Map.class, String.class).newInstance(instanceProviderName, instanceProperties, instanceName);
                HDStores.instanceCache.put(instanceName, newInstance);
                instance = newInstance;
            }
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException ex2) {
            throw new HDStoreException(String.format("Unable to instantiate instance of '%s' for provider '%s'", providerClassName, instanceProviderName), ex2);
        }
        return instance;
    }
    
    private static String getInstanceName(final String instanceProviderName, final Map<String, Object> instanceProperties) {
        if (instanceProviderName == null || instanceProviderName.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument");
        }
        final Class<? extends HDStoreManager> providerClass = getProviderClass(instanceProviderName);
        String instanceName = null;
        if (providerClass != null) {
            try {
                final Method getInstanceName = providerClass.getMethod("getInstanceName", Map.class);
                instanceName = (String)getInstanceName.invoke(null, instanceProperties);
            }
            catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex2) {
                HDStores.logger.error((Object)String.format("Provider '%s' does not implement 'public static String getInstanceName(String providerName, Map<String, Object> properties)'", instanceProviderName), (Throwable)ex2);
            }
        }
        return instanceName;
    }
    
    public static HDStoreManager getInstance(final Map<String, Object> instanceProperties) {
        final String instanceProviderName = getHDStoreProviderName(instanceProperties);
        return getInstance(instanceProviderName, instanceProperties);
    }
    
    private static String getHDStoreProviderName(final Map<String, Object> instanceProperties) {
        final String result = (instanceProperties == null) ? null : (String)instanceProperties.get("storageProvider");
        return (result != null) ? result : "elasticsearch";
    }
    
    private static String getProviderClassName(final String instanceProviderName) {
        if (instanceProviderName == null || instanceProviderName.isEmpty()) {
            throw new IllegalArgumentException("Invalid argument");
        }
        final Class baseClass = HDStoreManager.class;
        final Package basePackage = baseClass.getPackage();
        return basePackage.getName() + '.' + instanceProviderName.toLowerCase() + '.' + baseClass.getSimpleName();
    }
    
    private static Class<? extends HDStoreManager> getProviderClass(final String instanceProviderName) {
        Class<? extends HDStoreManager> result = null;
        final String providerClassName = getProviderClassName(instanceProviderName);
        try {
            result = (Class<? extends HDStoreManager>)Class.forName(providerClassName);
        }
        catch (ClassNotFoundException exception) {
            throw new HDStoreException(String.format("Unable to instantiate instance of '%s' for provider '%s'", providerClassName, instanceProviderName), exception);
        }
        return result;
    }
    
    public static void registerBackgroundTask(final BackgroundTask backgroundTask) {
        final String hdStoreName = backgroundTask.getHDStoreName();
        HDStores.backgroundTasks.put(backgroundTask, hdStoreName);
    }
    
    public static void unregisterBackgroundTask(final BackgroundTask backgroundTask) {
        HDStores.backgroundTasks.remove(backgroundTask);
    }
    
    public static void terminateBackgroundTasks(final String hdStoreName) {
        for (final Map.Entry<BackgroundTask, String> entry : HDStores.backgroundTasks.entrySet()) {
            final BackgroundTask backgroundTask = entry.getKey();
            final String taskHDStoreName = entry.getValue();
            if (hdStoreName.equals(taskHDStoreName)) {
                backgroundTask.terminate();
            }
        }
    }
    
    static {
        thisClass = HDStores.class;
        logger = Logger.getLogger((Class)HDStores.thisClass);
        CLASS_NAME = HDStores.thisClass.getSimpleName();
        instanceCache = new HashMap<String, HDStoreManager>(1);
        started = new AtomicBoolean(false);
        backgroundTasks = new ConcurrentHashMap<BackgroundTask, String>(0);
    }
}
