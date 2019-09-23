package com.datasphere.metaRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.EventQueueManager;
import com.datasphere.appmanager.event.Event;
import com.datasphere.drop.DropMetaObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class MDClientOps implements MDRepository
{
    private static Logger logger;
    private final String clusterName;
    private static MDClientOps INSTANCE;
    
    public static MDClientOps getINSTANCE() {
        return MDClientOps.INSTANCE;
    }
    
    public MDClientOps(final String clusterName) {
        this.clusterName = clusterName;
    }
    
    @Override
    public void initialize() {
    }
    
    public Collection executeRemoteOperation(final RemoteCall action) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Collection results = new ArrayList();
        results.add(DistributedExecutionManager.execOnAny(hz, (Callable<Object>)action));
        return results;
    }
    
    @Override
    public void putShowStream(final MetaInfo.ShowStream showStream, final AuthToken token) throws MetaDataRepositoryException {
        final Put showStreamObject = new Put(showStream, token, MDConstants.typeOfPut.SHOWSTREAMOBJECT);
        this.executeRemoteOperation(showStreamObject);
    }
    
    @Override
    public void putStatusInfo(final MetaInfo.StatusInfo statusInfo, final AuthToken token) throws MetaDataRepositoryException {
        final Put putStatusInfo = new Put(statusInfo, token, MDConstants.typeOfPut.STATUS_INFO);
        this.executeRemoteOperation(putStatusInfo);
    }
    
    @Override
    public void putDeploymentInfo(final Pair<UUID, UUID> deploymentInfo, final AuthToken token) throws MetaDataRepositoryException {
        final Put putDeploymentInfo = new Put(deploymentInfo, token, MDConstants.typeOfPut.DEPLOYMENT_INFO);
        this.executeRemoteOperation(putDeploymentInfo);
    }
    
    @Override
    public void putServer(final MetaInfo.Server server, final AuthToken token) throws MetaDataRepositoryException {
        final Put serverObject = new Put(server, token, MDConstants.typeOfPut.SERVERMETAOBJECT);
        this.executeRemoteOperation(serverObject);
    }
    
    @Override
    public void putMetaObject(final MetaInfo.MetaObject metaObject, final AuthToken token) throws MetaDataRepositoryException {
        final Put putMetaObject = new Put(metaObject, token, MDConstants.typeOfPut.METAOBJECT);
        this.executeRemoteOperation(putMetaObject);
    }
    
    @Override
    public MetaInfo.MetaObject getMetaObjectByUUID(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        final Get action = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_UUID, token);
        final Collection<Object> resultSet = (Collection<Object>)this.executeRemoteOperation(action);
        return (MetaInfo.MetaObject)resultSet.toArray()[0];
    }
    
    @Override
    public MetaInfo.MetaObject getMetaObjectByName(final EntityType entityType, final String namespace, final String name, final Integer version, final AuthToken token) throws MetaDataRepositoryException {
        final Get action = new Get(entityType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME, token);
        final Collection<Object> resultSet = (Collection<Object>)this.executeRemoteOperation(action);
        return (MetaInfo.MetaObject)resultSet.toArray()[0];
    }
    
    @Override
    public Set<?> getByEntityType(final EntityType entityType, final AuthToken token) throws MetaDataRepositoryException {
        final Get action = new Get(entityType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE, token);
        final Collection<Object> resultSet = (Collection<Object>)this.executeRemoteOperation(action);
        return (Set<?>)resultSet.toArray()[0];
    }
    
    @Override
    public Set<MetaInfo.MetaObject> getByNameSpace(final String namespace, final AuthToken token) throws MetaDataRepositoryException {
        final Get action = new Get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE, token);
        final Collection<Object> resultSet = (Collection<Object>)this.executeRemoteOperation(action);
        return (Set<MetaInfo.MetaObject>)resultSet.toArray()[0];
    }
    
    @Override
    public Set<?> getByEntityTypeInNameSpace(final EntityType eType, final String namespace, final AuthToken token) throws MetaDataRepositoryException {
        final Get action = new Get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE, token);
        final Collection<Object> resultSet = (Collection<Object>)this.executeRemoteOperation(action);
        return (Set<?>)resultSet.toArray()[0];
    }
    
    @Override
    public Set<?> getByEntityTypeInApplication(final EntityType entityType, final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        return null;
    }
    
    @Override
    public MetaInfo.StatusInfo getStatusInfo(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        final Get statusInfo = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_STATUSINFO, token);
        final Collection<MetaInfo.StatusInfo> collection = (Collection<MetaInfo.StatusInfo>)this.executeRemoteOperation(statusInfo);
        return (MetaInfo.StatusInfo)collection.toArray()[0];
    }
    
    @Override
    public Collection<UUID> getServersForDeployment(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        final Get serversForDeployement = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_SERVERFORDEPLOYMENT, token);
        final Collection<UUID> collection = (Collection<UUID>)this.executeRemoteOperation(serversForDeployement);
        return collection;
    }
    
    @Override
    public MetaInfo.MetaObject getServer(final String name, final AuthToken token) throws MetaDataRepositoryException {
        return null;
    }
    
    @Override
    public Integer getMaxClassId() {
        return null;
    }
    
    @Override
    public Position getAppCheckpointForFlow(final UUID flowUUID, final AuthToken token) throws MetaDataRepositoryException {
        final Get getPosition = new Get(EntityType.POSITION, flowUUID, null, null, null, MDConstants.typeOfGet.BY_POSITIONINFO, token);
        final Collection<Position> collection = (Collection<Position>)this.executeRemoteOperation(getPosition);
        return (Position)collection.toArray()[0];
    }
    
    @Override
    public MetaInfo.MetaObject revert(final EntityType eType, final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        return null;
    }
    
    @Override
    public void removeMetaObjectByName(final EntityType eType, final String namespace, final String name, final Integer version, final AuthToken token) throws MetaDataRepositoryException {
        final Removal action = new Removal(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME, token);
        this.executeRemoteOperation(action);
    }
    
    @Override
    public void removeStatusInfo(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        final Removal removeStatusInfo = new Removal(null, uuid, null, null, null, MDConstants.typeOfRemove.STATUS_INFO, token);
        this.executeRemoteOperation(removeStatusInfo);
    }
    
    @Override
    public void removeDeploymentInfo(final Pair<UUID, UUID> deploymentInfo, final AuthToken token) throws MetaDataRepositoryException {
        final Removal removeDeploymentInfo = new Removal(null, deploymentInfo, null, null, null, MDConstants.typeOfRemove.DEPLOYMENT_INFO, token);
        this.executeRemoteOperation(removeDeploymentInfo);
    }
    
    @Override
    public void removeMetaObjectByUUID(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        final Removal removeMetaObjectByUUID = new Removal(null, uuid, null, null, null, MDConstants.typeOfRemove.BY_UUID, token);
        this.executeRemoteOperation(removeMetaObjectByUUID);
    }
    
    @Override
    public void updateMetaObject(final MetaInfo.MetaObject object, final AuthToken token) throws MetaDataRepositoryException {
        final Update update = new Update(object, token);
        this.executeRemoteOperation(update);
    }
    
    @Override
    public boolean contains(final MDConstants.typeOfRemove contains, final UUID uuid, final String url) {
        return false;
    }
    
    @Override
    public boolean clear(final boolean cleanDB) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Clear action = new Clear();
        final Object cleared = DistributedExecutionManager.execOnAny(hz, (Callable<Object>)action);
        return (boolean)cleared;
    }
    
    @Override
    public String registerListerForDeploymentInfo(final EntryListener<UUID, UUID> entryListener) {
        return null;
    }
    
    @Override
    public String registerListenerForShowStream(final MessageListener<MetaInfo.ShowStream> messageListener) {
        return null;
    }
    
    @Override
    public String registerListenerForStatusInfo(final HazelcastIMapListener mapListener) {
        return null;
    }
    
    @Override
    public String registerListenerForMetaObject(final HazelcastIMapListener mapListener) {
        return null;
    }
    
    @Override
    public String registerListenerForServer(final HazelcastIMapListener mapListener) {
        return null;
    }
    
    @Override
    public void removeListerForDeploymentInfo(final String regId) {
    }
    
    @Override
    public void removeListenerForShowStream(final String regId) {
    }
    
    @Override
    public void removeListenerForStatusInfo(final String regId) {
    }
    
    @Override
    public void removeListenerForMetaObject(final String regId) {
    }
    
    @Override
    public void removeListenerForServer(final String regId) {
    }
    
    @Override
    public String exportMetadataAsJson() {
        return null;
    }
    
    @Override
    public void importMetadataFromJson(final String json, final Boolean replace) {
    }
    
    @Override
    public Map dumpMaps(final AuthToken token) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Dump dump = new Dump(token);
        final Object result = DistributedExecutionManager.execOnAny(hz, (Callable<Object>)dump);
        return (Map)result;
    }
    
    @Override
    public boolean removeDGInfoFromServer(final MetaInfo.MetaObject metaObject, final AuthToken authToken) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        if (!srvs.isEmpty()) {
            final RemoveDGInfoFromServerMetaObject rmDG = new RemoveDGInfoFromServerMetaObject(metaObject, authToken);
            DistributedExecutionManager.exec(hz, (Callable<Object>)rmDG, srvs);
        }
        return true;
    }
    
    public AuthToken authenticate(final String username, final String password) {
        return this.authenticate(username, password, null, null);
    }
    
    public AuthToken authenticate(final String username, final String password, final String clientId, final String type) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Authentication authentication = new Authentication(username, password, clientId, type);
        final Object result = DistributedExecutionManager.execOnAny(hz, (Callable<Object>)authentication);
        return (AuthToken)result;
    }
    
    public List<String> drop(final String objectName, final EntityType objectType, final DropMetaObject.DropRule dropRule, final AuthToken token) {
        final HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
        final Drop drop = new Drop(objectName, objectType, dropRule, token);
        final Object dropped = DistributedExecutionManager.execOnAny(hz, (Callable<Object>)drop);
        return (List<String>)dropped;
    }
    
    static {
        MDClientOps.logger = Logger.getLogger((Class)MDClientOps.class);
        MDClientOps.INSTANCE = new MDClientOps(HazelcastSingleton.get().getName());
    }
    
    public static class ConfirmCommandEvent implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -7423388614430552934L;
        Object obj;
        AuthToken token;
        
        public ConfirmCommandEvent(final Object obj, final AuthToken token) {
            this.obj = obj;
            this.token = token;
        }
        
        @Override
        public Object call() throws MetaDataRepositoryException {
            assert Server.server != null;
            final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
            final PositionInfo positionInfo = (PositionInfo)this.obj;
            if (positionInfo.checkpoint != null) {
                StatusDataStore.getInstance().putPendingAppCheckpoint(positionInfo.flowUuid, positionInfo.checkpoint, positionInfo.commandTimestamp);
            }
            try {
                EventQueueManager.get().sendCommandConfirmationEvent(positionInfo.serverID, positionInfo.commandAction, positionInfo.flowUuid, positionInfo.commandTimestamp);
            }
            catch (Exception e) {
                MDClientOps.logger.error((Object)e);
            }
            return null;
        }
    }
    
    public static class Put implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -7423388614430552934L;
        Object obj;
        AuthToken token;
        MDConstants.typeOfPut typeOfPut;
        
        public Put(final Object obj, final AuthToken token, final MDConstants.typeOfPut typeOfPut) {
            this.obj = obj;
            this.token = token;
            this.typeOfPut = typeOfPut;
        }
        
        @Override
        public Object call() throws MetaDataRepositoryException {
            assert Server.server != null;
            final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
            switch (this.typeOfPut) {
                case STATUS_INFO: {
                    metadataRepository.putStatusInfo((MetaInfo.StatusInfo)this.obj, this.token);
                    break;
                }
                case DEPLOYMENT_INFO: {
                    metadataRepository.putDeploymentInfo((Pair<UUID, UUID>)this.obj, this.token);
                    break;
                }
                case SHOWSTREAMOBJECT: {
                    metadataRepository.putShowStream((MetaInfo.ShowStream)this.obj, this.token);
                    break;
                }
                case METAOBJECT: {
                    metadataRepository.putMetaObject((MetaInfo.MetaObject)this.obj, this.token);
                    break;
                }
                case SERVERMETAOBJECT: {
                    metadataRepository.putServer((MetaInfo.Server)this.obj, this.token);
                    break;
                }
            }
            return null;
        }
    }
    
    public static class Get implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -7423381614430552934L;
        EntityType eType;
        UUID uuid;
        String namespace;
        String name;
        Integer version;
        MDConstants.typeOfGet get;
        AuthToken token;
        
        public Get(final EntityType eType, final UUID uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfGet get, final AuthToken token) {
            this.eType = eType;
            this.uuid = uuid;
            this.namespace = namespace;
            this.name = name;
            this.version = version;
            this.get = get;
            this.token = token;
        }
        
        @Override
        public Object call() throws MetaDataRepositoryException {
            assert Server.server != null;
            Object result = null;
            final MDRepository mdRepository = MetadataRepository.getINSTANCE();
            switch (this.get) {
                case BY_UUID: {
                    result = mdRepository.getMetaObjectByUUID(this.uuid, this.token);
                    break;
                }
                case BY_ENTITY_TYPE: {
                    result = mdRepository.getByEntityType(this.eType, this.token);
                    break;
                }
                case BY_NAMESPACE: {
                    result = mdRepository.getByNameSpace(this.namespace, this.token);
                    break;
                }
                case BY_NAMESPACE_AND_ENTITY_TYPE: {
                    result = mdRepository.getByEntityTypeInNameSpace(this.eType, this.namespace, this.token);
                    break;
                }
                case BY_NAME: {
                    result = mdRepository.getMetaObjectByName(this.eType, this.namespace, this.name, this.version, this.token);
                    break;
                }
                case BY_STATUSINFO: {
                    result = mdRepository.getStatusInfo(this.uuid, this.token);
                    break;
                }
                case BY_SERVERFORDEPLOYMENT: {
                    result = mdRepository.getServersForDeployment(this.uuid, this.token);
                    break;
                }
                case BY_POSITIONINFO: {
                    result = mdRepository.getAppCheckpointForFlow(this.uuid, this.token);
                    break;
                }
            }
            return result;
        }
    }
    
    public static class Removal implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -7423381614430552934L;
        EntityType eType;
        Object uuid;
        String namespace;
        String name;
        Integer version;
        MDConstants.typeOfRemove get;
        AuthToken token;
        
        public Removal(final EntityType eType, final Object uuid, final String namespace, final String name, final Integer version, final MDConstants.typeOfRemove get, final AuthToken token) {
            this.eType = eType;
            this.uuid = uuid;
            this.namespace = namespace;
            this.name = name;
            this.version = version;
            this.get = get;
            this.token = token;
        }
        
        @Override
        public Object call() throws MetaDataRepositoryException {
            assert Server.server != null;
            final MDRepository mdRepository = MetadataRepository.getINSTANCE();
            switch (this.get) {
                case BY_UUID: {
                    mdRepository.removeMetaObjectByUUID((UUID)this.uuid, this.token);
                    break;
                }
                case BY_NAME: {
                    mdRepository.removeMetaObjectByName(this.eType, this.namespace, this.name, this.version, this.token);
                    break;
                }
                case STATUS_INFO: {
                    mdRepository.removeStatusInfo((UUID)this.uuid, this.token);
                    break;
                }
                case DEPLOYMENT_INFO: {
                    mdRepository.removeDeploymentInfo((Pair<UUID, UUID>)this.uuid, this.token);
                    break;
                }
            }
            return null;
        }
    }
    
    public static class Update implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -123436876844552934L;
        MetaInfo.MetaObject object;
        AuthToken token;
        
        public Update(final MetaInfo.MetaObject object, final AuthToken token) {
            this.object = object;
            this.token = token;
        }
        
        @Override
        public Object call() throws Exception {
            assert Server.server != null;
            final MDRepository mdRepository = MetadataRepository.getINSTANCE();
            mdRepository.updateMetaObject(this.object, this.token);
            return null;
        }
    }
    
    public static class Drop implements RemoteCall<Object>
    {
        String objectName;
        EntityType entityType;
        DropMetaObject.DropRule dropRule;
        AuthToken token;
        
        public Drop(final String objectName, final EntityType entityType, final DropMetaObject.DropRule dropRule, final AuthToken token) {
            this.objectName = objectName;
            this.entityType = entityType;
            this.dropRule = dropRule;
            this.token = token;
        }
        
        @Override
        public Object call() throws Exception {
            assert Server.server != null;
            final Context context = Context.createContext(this.token);
            return context.dropObject(this.objectName, this.entityType, this.dropRule);
        }
    }
    
    public static class Clear implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -74133816144552934L;
        
        @Override
        public Object call() throws Exception {
            assert Server.server != null;
            final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
            return metadataRepository.clear(false);
        }
    }
    
    public static class Dump implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -74133816144550074L;
        public AuthToken token;
        
        public Dump(final AuthToken token) {
            this.token = token;
        }
        
        @Override
        public Object call() throws Exception {
            assert Server.server != null;
            final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
            return metadataRepository.dumpMaps(this.token);
        }
    }
    
    public static class Authentication implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -44213816144550014L;
        String username;
        String password;
        String clientId;
        String type;
        
        public Authentication(final String username, final String password, final String clientId, final String type) {
            this.type = null;
            this.username = username;
            this.password = password;
            this.clientId = clientId;
            this.type = type;
        }
        
        @Override
        public Object call() throws Exception {
            assert Server.server != null;
            final HSecurityManager securityManager = HSecurityManager.get();
            return securityManager.authenticate(this.username, this.password, this.clientId, this.type);
        }
    }
    
    public static class RemoveDGInfoFromServerMetaObject implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -44213816144014L;
        public AuthToken token;
        public MetaInfo.MetaObject metaObject;
        
        public RemoveDGInfoFromServerMetaObject(final MetaInfo.MetaObject metaObject, final AuthToken token) {
            this.metaObject = metaObject;
            this.token = token;
        }
        
        @Override
        public Object call() throws Exception {
            return MetadataRepository.getINSTANCE().removeDGInfoFromServer(this.metaObject, this.token);
        }
    }
    
    private class PositionInfo
    {
        private final UUID serverID;
        private final UUID flowUuid;
        private final Position checkpoint;
        private final Event.EventAction commandAction;
        private final long commandTimestamp;
        
        public PositionInfo(final UUID serverID, final UUID flowUuid, final Position checkpoint, final Event.EventAction commandAction, final long commandTimestamp) {
            this.serverID = serverID;
            this.flowUuid = flowUuid;
            this.checkpoint = checkpoint;
            this.commandAction = commandAction;
            this.commandTimestamp = commandTimestamp;
        }
    }
}
