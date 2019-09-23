package com.datasphere.metaRepository;

import java.util.EventListener;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MessageListener;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.HazelcastMultiMapListener;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.UUID;

public interface MDCacheInterface
{
    boolean put(final Object p0);
    
    Object get(final EntityType p0, final UUID p1, final String p2, final String p3, final Integer p4, final MDConstants.typeOfGet p5);
    
    Object remove(final EntityType p0, final Object p1, final String p2, final String p3, final Integer p4, final MDConstants.typeOfRemove p5);
    
    boolean contains(final MDConstants.typeOfRemove p0, final UUID p1, final String p2);
    
    boolean clear();
    
    String registerListerForDeploymentInfo(final EntryListener<UUID, UUID> p0);
    
    String registerListenerForShowStream(final MessageListener<MetaInfo.ShowStream> p0);
    
    String registerListenerForStatusInfo(final HazelcastIMapListener p0);
    
    String registerListenerForMetaObject(final HazelcastIMapListener p0);
    
    String registerListenerForServer(final HazelcastIMapListener p0);
    
    String registerListenerForUUIDToUrlMap(final HazelcastIMapListener p0);
    
    String registerListenerForEtypeToMetaObjectMap(final HazelcastMultiMapListener p0);
    
    void removeListerForDeploymentInfo(final String p0);
    
    void removeListenerForShowStream(final String p0);
    
    void removeListenerForStatusInfo(final String p0);
    
    void removeListenerForMetaObject(final String p0);
    
    void removeListenerForServer(final String p0);
    
    String registerCacheEntryListener(final EventListener p0, final MDConstants.mdcListener p1);
}
