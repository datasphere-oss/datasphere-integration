package com.datasphere.metaRepository;

import com.datasphere.runtime.meta.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.components.*;
import com.datasphere.recovery.*;
import com.hazelcast.core.*;
import com.datasphere.runtime.*;
import java.util.*;

public interface MDRepository
{
    void initialize() throws MetaDataRepositoryException;
    
    void putShowStream(final MetaInfo.ShowStream p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void putStatusInfo(final MetaInfo.StatusInfo p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void putDeploymentInfo(final Pair<UUID, UUID> p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void putServer(final MetaInfo.Server p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void putMetaObject(final MetaInfo.MetaObject p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    MetaInfo.MetaObject getMetaObjectByUUID(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    MetaInfo.MetaObject getMetaObjectByName(final EntityType p0, final String p1, final String p2, final Integer p3, final AuthToken p4) throws MetaDataRepositoryException;
    
    Set<?> getByEntityType(final EntityType p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    Set<MetaInfo.MetaObject> getByNameSpace(final String p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    Set<?> getByEntityTypeInNameSpace(final EntityType p0, final String p1, final AuthToken p2) throws MetaDataRepositoryException;
    
    Set<?> getByEntityTypeInApplication(final EntityType p0, final String p1, final String p2, final AuthToken p3) throws MetaDataRepositoryException;
    
    MetaInfo.StatusInfo getStatusInfo(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    Collection<UUID> getServersForDeployment(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    MetaInfo.MetaObject getServer(final String p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    Integer getMaxClassId();
    
    Position getAppCheckpointForFlow(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    MetaInfo.MetaObject revert(final EntityType p0, final String p1, final String p2, final AuthToken p3) throws MetaDataRepositoryException;
    
    void removeMetaObjectByName(final EntityType p0, final String p1, final String p2, final Integer p3, final AuthToken p4) throws MetaDataRepositoryException;
    
    void removeStatusInfo(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void removeDeploymentInfo(final Pair<UUID, UUID> p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void removeMetaObjectByUUID(final UUID p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    void updateMetaObject(final MetaInfo.MetaObject p0, final AuthToken p1) throws MetaDataRepositoryException;
    
    boolean contains(final MDConstants.typeOfRemove p0, final UUID p1, final String p2);
    
    boolean clear(final boolean p0);
    
    String registerListerForDeploymentInfo(final EntryListener<UUID, UUID> p0);
    
    String registerListenerForShowStream(final MessageListener<MetaInfo.ShowStream> p0);
    
    String registerListenerForStatusInfo(final HazelcastIMapListener p0);
    
    String registerListenerForMetaObject(final HazelcastIMapListener p0);
    
    String registerListenerForServer(final HazelcastIMapListener p0);
    
    void removeListerForDeploymentInfo(final String p0);
    
    void removeListenerForShowStream(final String p0);
    
    void removeListenerForStatusInfo(final String p0);
    
    void removeListenerForMetaObject(final String p0);
    
    void removeListenerForServer(final String p0);
    
    String exportMetadataAsJson();
    
    void importMetadataFromJson(final String p0, final Boolean p1);
    
    Map<?, ?> dumpMaps(final AuthToken p0);
    
    boolean removeDGInfoFromServer(final MetaInfo.MetaObject p0, final AuthToken p1) throws MetaDataRepositoryException;
}
