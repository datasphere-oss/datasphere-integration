package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.datasphere.cache.CacheAccessor;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.persistence.HStore;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class PartitionManager implements EntryListener<UUID, UUID>, IPartitionManager
{
    private static Logger logger;
    static ServerManager serverManager;
    private static String[] defaultGroups;
    private static List<String> defaultList;
    UUID cacheID;
    List<UUID> deploymentServers;
    public final ConsistentHashRing ring;
    final String cacheName;
    final UUID thisID;
    final MDRepository metadataRepository;
    final CacheAccessor cache;
    Object cacheLock;
    
    private static ServerManager serverManager() {
        if (PartitionManager.serverManager == null) {
            PartitionManager.serverManager = new ServerManager();
        }
        return PartitionManager.serverManager;
    }
    
    public static void registerCache(final String cacheName, final UUID metaUUID) {
        serverManager().registerCache(cacheName, metaUUID);
    }
    
    public static void deregisterCache(final MetaInfo.MetaObject obj) {
        serverManager().removeCacheForMetaObject(obj.uuid);
    }
    
    public static Set<String> getAllCacheNames() {
        return serverManager().getAllCacheNames();
    }
    
    public static void shutdown() {
        if (PartitionManager.serverManager != null) {
            PartitionManager.serverManager.doShutdown();
            PartitionManager.serverManager = null;
        }
    }
    
    public PartitionManager(final String cacheName, final int numReplicas, final CacheAccessor cache) {
        this.cacheLock = new Object();
        this.cacheName = cacheName;
        this.cache = cache;
        this.thisID = HazelcastSingleton.getNodeId();
        this.metadataRepository = MetadataRepository.getINSTANCE();
        this.cacheID = serverManager().getRelatedMetaObject(cacheName);
        this.ring = new ConsistentHashRing(cacheName, numReplicas);
        this.metadataRepository.registerListerForDeploymentInfo((EntryListener<UUID, UUID>)this);
        try {
            this.setDeploymentServers();
        }
        catch (MetaDataRepositoryException e) {
            PartitionManager.logger.error((Object)e.getMessage());
        }
        if (PartitionManager.logger.isInfoEnabled()) {
            PartitionManager.logger.info((Object)("Create PartitionManager " + cacheName + ((this.cacheID == null) ? "standalone" : (" for " + this.cacheID))));
        }
        serverManager().addManagedPartition(this);
        serverManager().keepCacheReference(cacheName);
    }
    
    public void doShutdown() {
        this.ring.shutDown();
    }
    
    private void setDeploymentServers() throws MetaDataRepositoryException {
        synchronized (this.cacheLock) {
            if (this.cacheID != null) {
                final Collection<UUID> wd = this.metadataRepository.getServersForDeployment(this.cacheID, HSecurityManager.TOKEN);
                final Collection<UUID> whereDeployed = new ArrayList<UUID>();
                for (final Object obj : wd) {
                    if (obj instanceof UUID) {
                        whereDeployed.add((UUID)obj);
                    }
                    else {
                        if (!(obj instanceof Collection)) {
                            continue;
                        }
                        whereDeployed.addAll((Collection<? extends UUID>)obj);
                    }
                }
                final Map<UUID, Endpoint> map = HazelcastSingleton.getActiveMembersMap();
                this.deploymentServers = new ArrayList<UUID>();
                for (final UUID s : whereDeployed) {
                    if (map.containsKey(s)) {
                        this.deploymentServers.add(s);
                    }
                }
                if (this.deploymentServers.isEmpty()) {
                    serverManager().removeCacheForMetaObject(this.cacheID);
                }
            }
            else {
                this.deploymentServers = serverManager().getAllServers();
            }
            if (PartitionManager.logger.isInfoEnabled()) {
                PartitionManager.logger.info((Object)("Partition Manager " + this.cacheName + " in Servers {" + this.deploymentServers + "}"));
            }
        }
    }
    
    public void addUpdateListener(final ConsistentHashRing.UpdateListener listener) {
        this.ring.addUpdateListener(listener);
    }
    
    public int getNumberOfPartitions() {
        return this.ring.getNumberOfPartitions();
    }
    
    public void removeUpdateListener(final ConsistentHashRing.UpdateListener listener) {
        this.ring.removeUpdateListener(listener);
    }
    
    public void setServers(final List<UUID> servers) {
        if (PartitionManager.logger.isInfoEnabled()) {
            PartitionManager.logger.info((Object)("Partition Manager for " + this.cacheName + " in Servers {" + servers + "}"));
        }
        this.ring.set(servers);
    }
    
    private boolean nullEqual(final Object one, final Object two) {
        return (one == null && two == null) || (one == null == (two == null) && one.equals(two));
    }
    
    private void modifyDeploymentServers(final UUID objid) throws MetaDataRepositoryException {
        if (this.cacheID == null) {
            this.cacheID = serverManager().getRelatedMetaObject(this.cacheName);
        }
        if (this.cacheID == null || this.cacheID.equals((Object)objid)) {
            this.setDeploymentServers();
            serverManager().update(this);
        }
    }
    
    public int getNumReplicas() {
        return this.ring.getNumReplicas();
    }
    
    public int getPartitionId(final Object key) {
        return this.ring.getPartitionId(key);
    }
    
    public UUID getPartitionOwnerForKey(final Object key, final int n) {
        return this.ring.getUUIDForKey(key, n);
    }
    
    public UUID getPartitionOwnerForPartition(final int partId, final int n) {
        return this.ring.getUUIDForPartition(partId, n);
    }
    
    public UUID getFirstPartitionOwnerForPartition(final int partId) {
        return this.ring.getUUIDForPartition(partId, 0);
    }
    
    public boolean isLocalPartitionByKey(final Object key, final int n) {
        return this.thisID.equals((Object)this.getPartitionOwnerForKey(key, n));
    }
    
    public boolean isLocalPartitionById(final int partId, final int n) {
        return this.thisID.equals((Object)this.getPartitionOwnerForPartition(partId, n));
    }
    
    public boolean hasLocalPartitionForId(final int partId) {
        if (this.ring.isFullyReplicated()) {
            return true;
        }
        for (int i = 0; i < this.ring.getNumReplicas(); ++i) {
            if (this.isLocalPartitionById(partId, i)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean hasLocalPartitionForKey(final Object key) {
        final int partId = this.getPartitionId(key);
        return this.hasLocalPartitionForId(partId);
    }
    
    public List<UUID> getAllReplicas(final int partId) {
        final List<UUID> ret = new ArrayList<UUID>();
        for (int i = 0; i < this.ring.getNumReplicas(); ++i) {
            final UUID uuid = this.getPartitionOwnerForPartition(partId, i);
            if (!ret.contains(uuid)) {
                ret.add(uuid);
            }
        }
        return ret;
    }
    
    public boolean isLocalPartitionByUUID(final UUID uuid) {
        return this.thisID.equals((Object)uuid);
    }
    
    public UUID getLocal() {
        return this.thisID;
    }
    
    public boolean isMultiNode() {
        return this.ring.isMultiNode();
    }
    
    public List<UUID> getPeers() {
        return new ArrayList<UUID>(this.ring.getNodes());
    }
    
    public void entryAdded(final EntryEvent<UUID, UUID> event) {
        final UUID objid = (UUID)event.getKey();
        try {
            this.modifyDeploymentServers(objid);
        }
        catch (MetaDataRepositoryException e) {
            PartitionManager.logger.error((Object)e.getMessage());
        }
    }
    
    public void entryRemoved(final EntryEvent<UUID, UUID> event) {
        final UUID serverID = (UUID)event.getValue();
        final UUID objid = (UUID)event.getKey();
        try {
            this.modifyDeploymentServers(objid);
        }
        catch (MetaDataRepositoryException e) {
            PartitionManager.logger.error((Object)e.getMessage());
        }
        if (this.thisID.equals((Object)serverID)) {
            serverManager().removeCacheForMetaObject(objid);
        }
    }
    
    public void entryUpdated(final EntryEvent<UUID, UUID> event) {
        final UUID objid = (UUID)event.getKey();
        try {
            this.modifyDeploymentServers(objid);
        }
        catch (MetaDataRepositoryException e) {
            PartitionManager.logger.error((Object)e.getMessage());
        }
    }
    
    public void entryEvicted(final EntryEvent<UUID, UUID> event) {
        final UUID serverID = (UUID)event.getValue();
        final UUID objid = (UUID)event.getKey();
        try {
            this.modifyDeploymentServers(objid);
        }
        catch (MetaDataRepositoryException e) {
            PartitionManager.logger.error((Object)e.getMessage());
        }
        if (this.thisID.equals((Object)serverID)) {
            serverManager().removeCacheForMetaObject(objid);
        }
    }
    
    public void mapEvicted(final MapEvent event) {
    }
    
    public void mapCleared(final MapEvent event) {
    }
    
    static {
        PartitionManager.logger = Logger.getLogger((Class)PartitionManager.class);
        PartitionManager.serverManager = null;
        PartitionManager.defaultGroups = new String[] { "default" };
        PartitionManager.defaultList = Arrays.asList(PartitionManager.defaultGroups);
    }
    
    static class ResolverListener implements EntryListener<String, UUID>
    {
        List<String> seenCaches;
        
        ResolverListener() {
            this.seenCaches = new ArrayList<String>();
        }
        
        public void entryAdded(final EntryEvent<String, UUID> event) {
            if (!this.seenCaches.contains(event.getKey())) {
                PartitionManager.serverManager.addReverseResolver((String)event.getKey(), (UUID)event.getValue());
            }
        }
        
        public void entryRemoved(final EntryEvent<String, UUID> event) {
            if (this.seenCaches.contains(event.getKey())) {
                this.seenCaches.remove(event.getKey());
            }
        }
        
        public void entryUpdated(final EntryEvent<String, UUID> event) {
        }
        
        public void entryEvicted(final EntryEvent<String, UUID> event) {
            if (this.seenCaches.contains(event.getKey())) {
                this.seenCaches.remove(event.getKey());
            }
        }
        
        public void mapEvicted(final MapEvent event) {
        }
        
        public void mapCleared(final MapEvent event) {
        }
    }
    
    static class ServerManager implements HazelcastIMapListener<UUID, MetaInfo.Server>
    {
        IMap<String, UUID> cacheResolver;
        IMap<String, List<UUID>> cacheReferences;
        IMap<String, Long> cacheStatus;
        UUID thisId;
        MDRepository metadataRepository;
        Map<UUID, List<String>> reverseResolver;
        final Set<UUID> serverGroups;
        final Map<String, PartitionManager> partitionManagers;
        
        private ServerManager() {
            this.metadataRepository = MetadataRepository.getINSTANCE();
            this.reverseResolver = new HashMap<UUID, List<String>>();
            this.serverGroups = new HashSet<UUID>();
            this.partitionManagers = new ConcurrentHashMap<String, PartitionManager>();
            this.thisId = HazelcastSingleton.getNodeId();
            this.cacheResolver = HazelcastSingleton.get().getMap("#cacheResolver");
            this.cacheReferences = HazelcastSingleton.get().getMap("#cacheReferences");
            this.cacheStatus = HazelcastSingleton.get().getMap("#cacheStatus");
            for (final Map.Entry<String, UUID> entry : this.cacheResolver.entrySet()) {
                this.addReverseResolver(entry.getKey(), entry.getValue());
            }
            this.cacheResolver.addEntryListener((EntryListener)new ResolverListener(), true);
            this.metadataRepository.registerListenerForServer(this);
            try {
                this.updateServerGroups();
            }
            catch (MetaDataRepositoryException e) {
                PartitionManager.logger.error((Object)e.getMessage());
            }
        }
        
        private void updateServerGroups() throws MetaDataRepositoryException {
            synchronized (this.serverGroups) {
                this.serverGroups.clear();
                final Set<UUID> valid = new HashSet<UUID>();
                final Set<Member> hzMembers = (Set<Member>)HazelcastSingleton.get().getCluster().getMembers();
                for (final Member member : hzMembers) {
                    valid.add(new UUID(member.getUuid()));
                }
                final Set<MetaInfo.Server> servers = (Set<MetaInfo.Server>)this.metadataRepository.getByEntityType(EntityType.SERVER, HSecurityManager.TOKEN);
                for (final MetaInfo.Server server : servers) {
                    if (valid.contains(server.getUuid())) {
                        this.serverGroups.add(server.getUuid());
                    }
                }
            }
            if (PartitionManager.logger.isInfoEnabled()) {
                PartitionManager.logger.info((Object)("ServerManager has " + this.serverGroups));
            }
        }
        
        public void registerCache(final String cacheName, final UUID metaUUID) {
            this.cacheResolver.put(cacheName, metaUUID);
        }
        
        public Set<String> getAllCacheNames() {
            return (Set<String>)this.cacheResolver.keySet();
        }
        
        public void keepCacheReference(final String cacheName) {
            try {
                this.cacheReferences.lock(cacheName);
                List<UUID> refs = (List<UUID>)this.cacheReferences.get(cacheName);
                if (refs == null) {
                    refs = new ArrayList<UUID>();
                }
                refs.add(this.thisId);
                this.cacheReferences.put(cacheName, refs);
            }
            finally {
                this.cacheReferences.unlock(cacheName);
            }
        }
        
        private void addReverseResolver(final String cacheName, final UUID metaUUID) {
            synchronized (this.reverseResolver) {
                List<String> currentForUUID = this.reverseResolver.get(metaUUID);
                if (currentForUUID == null) {
                    currentForUUID = new ArrayList<String>();
                }
                if (!currentForUUID.contains(cacheName)) {
                    currentForUUID.add(cacheName);
                }
                this.reverseResolver.put(metaUUID, currentForUUID);
            }
            if (PartitionManager.logger.isInfoEnabled()) {
                PartitionManager.logger.info(("ServerManager Register managed object " + cacheName + " for " + metaUUID));
            }
        }
        
        public UUID getRelatedMetaObject(final String cacheName) {
            return (UUID)this.cacheResolver.get(cacheName);
        }
        
        public void removeCacheForMetaObject(final UUID metaId) {
            synchronized (this.reverseResolver) {
                final List<String> cacheNames = this.reverseResolver.get(metaId);
                if (cacheNames == null) {
                    return;
                }
                for (final String cacheName : cacheNames) {
                    final PartitionManager pm = this.partitionManagers.get(cacheName);
                    if (pm == null) {
                        continue;
                    }
                    pm.cache.close();
                    this.partitionManagers.remove(cacheName);
                    HStore.remove(metaId);
                    this.reverseResolver.remove(metaId);
                    try {
                        this.cacheReferences.lock(cacheName);
                        final List<UUID> refs = (List<UUID>)this.cacheReferences.get(cacheName);
                        if (refs == null) {
                            continue;
                        }
                        if (refs.contains(this.thisId)) {
                            refs.remove(this.thisId);
                        }
                        final Set<UUID> currentServers = this.serverGroups;
                        final Iterator<UUID> refIt = refs.iterator();
                        while (refIt.hasNext()) {
                            final UUID ref = refIt.next();
                            if (!currentServers.contains(ref)) {
                                refIt.remove();
                            }
                        }
                        if (refs.size() == 0) {
                            this.cacheReferences.remove(cacheName);
                            this.cacheResolver.remove(cacheName);
                            this.cacheStatus.remove(cacheName);
                        }
                        else {
                            this.cacheReferences.put(cacheName, refs);
                        }
                    }
                    finally {
                        this.cacheReferences.unlock(cacheName);
                    }
                }
            }
        }
        
        public List<UUID> getAllServers() {
            synchronized (this.serverGroups) {
                return new ArrayList<UUID>(this.serverGroups);
            }
        }
        
        public void addManagedPartition(final PartitionManager manager) {
            synchronized (this.partitionManagers) {
                this.partitionManagers.put(manager.cacheName, manager);
            }
            this.update(manager);
        }
        
        public void update(final PartitionManager manager) {
            final List<UUID> servers = manager.deploymentServers;
            manager.setServers(servers);
        }
        
        private void modifyDeploymentGroups() {
            try {
                this.updateServerGroups();
            }
            catch (MetaDataRepositoryException e) {
                PartitionManager.logger.error((Object)e.getMessage());
            }
            synchronized (this.partitionManagers) {
                for (final PartitionManager manager : this.partitionManagers.values()) {
                    this.update(manager);
                }
            }
        }
        
        public void doShutdown() {
            for (final IPartitionManager manager : this.partitionManagers.values()) {
                manager.doShutdown();
            }
        }
        
        public void entryAdded(final EntryEvent<UUID, MetaInfo.Server> event) {
            this.modifyDeploymentGroups();
        }
        
        public void entryRemoved(final EntryEvent<UUID, MetaInfo.Server> event) {
            this.modifyDeploymentGroups();
        }
        
        public void entryUpdated(final EntryEvent<UUID, MetaInfo.Server> event) {
            this.modifyDeploymentGroups();
        }
    }
}
