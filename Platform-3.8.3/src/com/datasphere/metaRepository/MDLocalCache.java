package com.datasphere.metaRepository;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.datasphere.common.errors.CacheError;
import com.datasphere.common.errors.IError;
import com.datasphere.errorhandling.DatallRuntimeException;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class MDLocalCache
{
    private Map<UUID, MetaInfo.Server> serverCache;
    private Map<String, MetaInfo.MetaObject> urlToMetaObject;
    private Map<Integer, Set<String>> eTypeToMetaObjectUrl;
    private Map<UUID, String> uuidToURL;
    private static Logger logger;
    private MetadataRepository metaInstance;
    private BlockingQueue<MetaObjectMapUpdate> initializationQueue;
    private final AtomicBoolean initializedFlag;
    
    public MDLocalCache() {
        this.serverCache = null;
        this.urlToMetaObject = null;
        this.metaInstance = MetadataRepository.getINSTANCE();
        this.initializedFlag = new AtomicBoolean(false);
        this.serverCache = new ConcurrentHashMap<UUID, MetaInfo.Server>();
        this.urlToMetaObject = new ConcurrentHashMap<String, MetaInfo.MetaObject>();
        this.uuidToURL = new ConcurrentHashMap<UUID, String>();
        this.eTypeToMetaObjectUrl = new HashMap<Integer, Set<String>>();
        this.initializationQueue = new LinkedBlockingQueue<MetaObjectMapUpdate>(10000);
        this.metaInstance.registerListenerForServer(new ServerListener());
        this.metaInstance.registerListenerForMetaObject(new UrlToMetaObjectListener());
        MDCache.getInstance().registerListenerForUUIDToUrlMap(new UUIDToUrlListener());
        try {
            final Set<MetaInfo.Server> serverSet = (Set<MetaInfo.Server>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, HSecurityManager.TOKEN);
            for (final MetaInfo.Server server : serverSet) {
                this.serverCache.put(server.getUuid(), server);
            }
            final IMap<String, MetaInfo.MetaObject> urlmetaObjects = MDCache.getInstance().getUrlToMetaObject();
            for (final Map.Entry<String, MetaInfo.MetaObject> mob : urlmetaObjects.entrySet()) {
                this.putMetaObjectInMaps(mob.getKey(), mob.getValue());
                MDLocalCache.logger.debug((Object)("Added object in maps through initialization: " + mob));
            }
            final IMap<UUID, String> uuidToURLMap = MDCache.getInstance().getUuidToURL();
            for (final Map.Entry<UUID, String> uuidUrl : uuidToURLMap.entrySet()) {
                this.uuidToURL.put(uuidUrl.getKey(), uuidUrl.getValue());
            }
            this.dequeAndUpdateMap();
            synchronized (this.initializedFlag) {
                this.dequeAndUpdateMap();
                this.initializedFlag.set(true);
                this.initializationQueue = null;
            }
        }
        catch (MetaDataRepositoryException e) {
            MDLocalCache.logger.error((Object)"Error while initializing MDLocalCache: ", (Throwable)e);
            throw new DatallRuntimeException((IError)CacheError.MDLOCALCACHE_INITIALIZATION_ERROR, (Throwable)e, "MDLocalCache", new String[0]);
        }
        MDLocalCache.logger.info((Object)(MDLocalCache.class.getName() + " is initialized"));
    }
    
    private void dequeAndUpdateMap() {
        try {
            while (!this.initializationQueue.isEmpty()) {
                final MetaObjectMapUpdate mapUpdate = this.initializationQueue.take();
                switch (mapUpdate.getOperation()) {
                    case ADD: {
                        this.putMetaObjectInMaps(mapUpdate.getKey(), mapUpdate.getValue());
                        continue;
                    }
                    case REMOVE: {
                        this.removeMetaObjectInMaps(mapUpdate.getKey(), mapUpdate.getValue());
                        continue;
                    }
                }
            }
        }
        catch (InterruptedException ex) {
            MDLocalCache.logger.error((Object)ex);
        }
    }
    
    private synchronized void removeMetaObjectInMaps(final String url, final MetaInfo.MetaObject metaObject) {
        final Integer key = metaObject.getType().ordinal();
        final Set<String> urlSet = this.eTypeToMetaObjectUrl.get(key);
        if (urlSet != null) {
            urlSet.remove(url);
            this.eTypeToMetaObjectUrl.put(key, urlSet);
            if (urlSet.size() == 0) {
                this.eTypeToMetaObjectUrl.remove(key);
            }
        }
        this.urlToMetaObject.remove(url);
        MDLocalCache.logger.debug((Object)("Removed metaobject: " + metaObject + " URL: " + url));
    }
    
    public Set<MetaInfo.Server> getServerSet() throws MetaDataRepositoryException {
        return new HashSet<MetaInfo.Server>(this.serverCache.values());
    }
    
    public Map<UUID, MetaInfo.Server> getServerMap() throws MetaDataRepositoryException {
        return this.serverCache;
    }
    
    public MetaInfo.MetaObject getMetaObjectByUUID(final UUID uuid) throws MetaDataRepositoryException {
        MetaInfo.MetaObject got = null;
        final String vUrl = this.uuidToURL.get(uuid);
        if (vUrl != null) {
            got = this.urlToMetaObject.get(vUrl);
        }
        if (got == null) {
            got = this.serverCache.get(uuid);
        }
        return got;
    }
    
    public MetaInfo.Server getServerByUUID(final UUID serverID) throws MetaDataRepositoryException {
        MetaInfo.Server server = this.serverCache.get(serverID);
        if (server == null) {
            server = (MetaInfo.Server)this.metaInstance.getMetaObjectByUUID(serverID, HSecurityManager.TOKEN);
            if (server != null) {
                this.serverCache.put(serverID, server);
            }
        }
        return server;
    }
    
    public synchronized Set<?> getByEntityType(final EntityType eType) {
        if (eType == EntityType.SERVER) {
            return new HashSet<Object>(this.serverCache.values());
        }
        final Collection<String> resultCollection = this.eTypeToMetaObjectUrl.get(eType.ordinal());
        if (resultCollection != null) {
            final Set<MetaInfo.MetaObject> mSet = new HashSet<MetaInfo.MetaObject>();
            for (final String muri : resultCollection) {
                final MetaInfo.MetaObject mObject = this.urlToMetaObject.get(muri);
                mSet.add(mObject);
            }
            return mSet;
        }
        return null;
    }
    
    private synchronized void putMetaObjectInMaps(final String url, final MetaInfo.MetaObject mob) {
        this.urlToMetaObject.put(url, mob);
        Set<String> urlSet = this.eTypeToMetaObjectUrl.get(mob.getType().ordinal());
        if (urlSet == null) {
            urlSet = new HashSet<String>();
        }
        urlSet.add(url);
        this.eTypeToMetaObjectUrl.put(mob.getType().ordinal(), urlSet);
        MDLocalCache.logger.debug((Object)("Added metaobject: " + mob + " URL: " + url));
    }
    
    static {
        MDLocalCache.logger = Logger.getLogger((Class)MDLocalCache.class);
    }
    
    public class ServerListener implements HazelcastIMapListener
    {
        public void entryAdded(final EntryEvent entryEvent) {
            MDLocalCache.this.serverCache.put((UUID)entryEvent.getKey(), (MetaInfo.Server)entryEvent.getValue());
        }
        
        public void entryRemoved(final EntryEvent entryEvent) {
            MDLocalCache.this.serverCache.remove(entryEvent.getKey());
        }
        
        public void entryUpdated(final EntryEvent entryEvent) {
            this.entryAdded(entryEvent);
        }
    }
    
    public class UrlToMetaObjectListener implements HazelcastIMapListener
    {
        public void entryAdded(final EntryEvent entryEvent) {
            synchronized (MDLocalCache.this.initializedFlag) {
                if (!MDLocalCache.this.initializedFlag.get()) {
                    try {
                        MDLocalCache.this.initializationQueue.put(new MetaObjectMapUpdate(MetaObjectMapUpdate.OPERATION.ADD, (String)entryEvent.getKey(), (MetaInfo.MetaObject)entryEvent.getValue()));
                    }
                    catch (InterruptedException e) {
                        MDLocalCache.logger.error((Object)e);
                    }
                    return;
                }
            }
            final MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)entryEvent.getValue();
            final String url = (String)entryEvent.getKey();
            MDLocalCache.this.putMetaObjectInMaps(url, metaObject);
            MDLocalCache.logger.debug((Object)("Added object in maps through listener" + metaObject));
        }
        
        public void entryRemoved(final EntryEvent entryEvent) {
            synchronized (MDLocalCache.this.initializedFlag) {
                if (!MDLocalCache.this.initializedFlag.get()) {
                    try {
                        MDLocalCache.this.initializationQueue.put(new MetaObjectMapUpdate(MetaObjectMapUpdate.OPERATION.REMOVE, (String)entryEvent.getKey(), (MetaInfo.MetaObject)entryEvent.getValue()));
                    }
                    catch (InterruptedException e) {
                        MDLocalCache.logger.error((Object)e);
                    }
                }
            }
            final String url = (String)entryEvent.getKey();
            final MetaInfo.MetaObject metaObject = MDLocalCache.this.urlToMetaObject.get(url);
            MDLocalCache.this.removeMetaObjectInMaps(url, metaObject);
        }
        
        public void entryUpdated(final EntryEvent entryEvent) {
            this.entryAdded(entryEvent);
        }
    }
    
    public class UUIDToUrlListener implements HazelcastIMapListener
    {
        public void entryAdded(final EntryEvent entryEvent) {
            MDLocalCache.this.uuidToURL.put((UUID)entryEvent.getKey(), (String)entryEvent.getValue());
        }
        
        public void entryRemoved(final EntryEvent entryEvent) {
            MDLocalCache.this.uuidToURL.remove(entryEvent.getKey());
        }
        
        public void entryUpdated(final EntryEvent entryEvent) {
            MDLocalCache.this.uuidToURL.put((UUID)entryEvent.getKey(), (String)entryEvent.getValue());
        }
    }
    
    private static class MetaObjectMapUpdate
    {
        private OPERATION operation;
        private String key;
        private MetaInfo.MetaObject value;
        
        public MetaObjectMapUpdate(final OPERATION operation, final String key, final MetaInfo.MetaObject value) {
            this.operation = operation;
            this.key = key;
            this.value = value;
        }
        
        public OPERATION getOperation() {
            return this.operation;
        }
        
        public String getKey() {
            return this.key;
        }
        
        public MetaInfo.MetaObject getValue() {
            return this.value;
        }
        
        private enum OPERATION
        {
            ADD, 
            REMOVE;
        }
    }
}
