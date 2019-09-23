package com.datasphere.web.api;

import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.log4j.Logger;

import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.CompilationException;
import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.exceptions.InvalidFormatException;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class ClientOperations
{
    private static Logger logger;
    private ObjectMapper jsonMapper;
    private static MDRepository mdRepository;
    
    public static ClientOperations getInstance() {
        return SingletonHolder.INSTANCE;
    }
    
    private ClientOperations() {
        this.jsonMapper = ObjectMapperFactory.getInstance();
        ClientOperations.mdRepository = MetadataRepository.getINSTANCE();
    }
    
    public ObjectNode[] CRUDWorker(final QueryValidator clientInterface, final AuthToken token, final String namespace, final String appOrFlowName, final EntityType entityType, final CRUD operation, final ObjectNode data) throws Exception {
        Object resultAsJavaObject = null;
        if (operation.equals(CRUD.READ)) {
            resultAsJavaObject = this.read(entityType, data, token);
        }
        else {
            AuthToken requestToken = null;
            try {
                try {
                    requestToken = this.setupContextForCurrentOperation(operation, namespace, clientInterface, appOrFlowName, token);
                }
                catch (CompilationException ex) {
                    if (CRUD.CREATE == operation && EntityType.NAMESPACE == entityType) {
                        throw new CompilationException("No permission for namespace " + data.get("name"));
                    }
                    throw ex;
                }
                final Boolean cor = this.isUpdateOpertion(operation);
                if (operation.equals(CRUD.CREATE) || operation.equals(CRUD.UPDATE)) {
                    boolean thisIsAlterOperation = false;
                    if (operation.equals(CRUD.UPDATE)) {
                        final Object metaObject = this.read(entityType, data, token);
                        MetaInfo.Stream streamMetaObject = null;
                        if (metaObject instanceof MetaInfo.Stream) {
                            streamMetaObject = (MetaInfo.Stream)metaObject;
                        }
                        if (streamMetaObject != null) {
                            thisIsAlterOperation = isThisAlterOperation(ClientOperations.mdRepository, streamMetaObject, clientInterface, operation, entityType, cor, data, namespace, requestToken, token);
                            if (thisIsAlterOperation) {
                                streamMetaObject.getPartitioningFields().clear();
                                ClientOperations.mdRepository.updateMetaObject(streamMetaObject, token);
                                resultAsJavaObject = this.alter(clientInterface, operation, entityType, data, namespace, requestToken, token);
                            }
                        }
                    }
                    if (!thisIsAlterOperation) {
                        resultAsJavaObject = this.createOrUpdate(clientInterface, operation, entityType, cor, data, namespace, requestToken, token);
                    }
                }
                else {
                    assert operation.equals(CRUD.DELETE);
                    resultAsJavaObject = this.delete(clientInterface, entityType, namespace, data, requestToken);
                }
            }
            catch (Exception e) {
                throw e;
            }
            finally {
                this.destroyContextForCurrentOperation(clientInterface, requestToken);
            }
        }
        final ObjectNode[] resultAsJsonObject = this.formatResult(resultAsJavaObject);
        return resultAsJsonObject;
    }
    
    public static boolean isThisAlterOperation(final MDRepository mdRepository, final MetaInfo.Stream streamMetaObject, final QueryValidator clientInterface, final CRUD operation, final EntityType entityType, final Boolean cor, final ObjectNode data, final String namespace, final AuthToken requestToken, final AuthToken token) throws MetaDataRepositoryException, InvalidPropertiesFormatException {
        if (streamMetaObject == null || entityType != EntityType.STREAM || !cor) {
            return false;
        }
        final JsonNode jsonSNode = data.get("partitioningFields");
        assert jsonSNode.isArray();
        final String[] streamPartitionFields = ClientCreateOperation.arrayNodeToStringArray(data.get("partitioningFields"), entityType, "partitioningFields");
        final String dataType = data.get("dataType").asText(" ");
        final String type_fullName = ClientCreateOperation.getFullName(dataType);
        final MetaInfo.Type typeMetaObject = (MetaInfo.Type)mdRepository.getMetaObjectByName(EntityType.TYPE, Utility.splitDomain(type_fullName), Utility.splitName(type_fullName), null, token);
        final boolean persist = data.get("persist").asBoolean(false);
        StreamPersistencePolicy spp;
        if (persist) {
            String propertySet = null;
            if (data.has("propertySet")) {
                propertySet = data.get("propertySet").asText();
                if (propertySet != null && !propertySet.isEmpty()) {
                    propertySet = ClientCreateOperation.getFullName(propertySet);
                }
            }
            if (propertySet == null) {
                propertySet = "default";
            }
            spp = new StreamPersistencePolicy(propertySet);
        }
        else {
            spp = new StreamPersistencePolicy(null);
        }
        final String sppNamespace = Utility.splitDomain(spp.getFullyQualifiedNameOfPropertyset());
        final String sppName = Utility.splitName(spp.getFullyQualifiedNameOfPropertyset());
        MetaInfo.PropertySet kpset = null;
        if (persist) {
            kpset = (MetaInfo.PropertySet)mdRepository.getMetaObjectByName(EntityType.PROPERTYSET, sppNamespace, sppName, null, HSecurityManager.TOKEN);
        }
        final UUID typeFromJson = typeMetaObject.getUuid();
        boolean canBeAlter = false;
        if (streamMetaObject.dataType.equals((Object)typeFromJson)) {
            canBeAlter = true;
        }
        if (canBeAlter) {
            final EqualsBuilder notEqualsBuilder = new EqualsBuilder();
            if (kpset != null) {
                notEqualsBuilder.append((Object)streamMetaObject.pset, (Object)kpset.getFullName());
            }
            else {
                notEqualsBuilder.append((Object)streamMetaObject.pset, (Object)null);
            }
            notEqualsBuilder.append((Object)streamMetaObject.partitioningFields, (Object)Arrays.asList(streamPartitionFields));
            return !notEqualsBuilder.isEquals();
        }
        return false;
    }
    
    AuthToken setupContextForCurrentOperation(final CRUD operation, final String namespace, final QueryValidator clientInterface, final String appOrFlowName, final AuthToken token) throws InvalidPropertiesFormatException, MetaDataRepositoryException {
        if (namespace == null) {
            throw new InvalidPropertiesFormatException("Namespace can't be null for operation: " + operation.toString());
        }
        final Context ctx = new Context(token);
        ctx.setIsUIContext(true);
        ctx.useNamespace(namespace);
        clientInterface.setUpdateMode(true);
        if (appOrFlowName != null && !appOrFlowName.isEmpty()) {
            MetaInfo.MetaObject appOrFlow = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, namespace, appOrFlowName, null, token);
            if (appOrFlow == null) {
                appOrFlow = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.FLOW, namespace, appOrFlowName, null, token);
                if (appOrFlow == null) {
                    throw new NullPointerException("No Application or Flow found with name : " + appOrFlowName + " in Namespace: " + namespace);
                }
                ctx.setCurFlow((MetaInfo.Flow)appOrFlow);
                ctx.setCurApp(appOrFlow.getCurrentApp());
            }
            else {
                ctx.setCurApp((MetaInfo.Flow)appOrFlow);
            }
        }
        else if (ClientOperations.logger.isDebugEnabled()) {
            ClientOperations.logger.debug((Object)("Operation: " + operation + ", Namespace: " + namespace));
        }
        final AuthToken requestToken = new AuthToken();
        clientInterface.addContext(requestToken, ctx);
        return requestToken;
    }
    
    private Object read(final EntityType entityType, final ObjectNode data, final AuthToken token) throws InvalidPropertiesFormatException, MetaDataRepositoryException, IllegalArgumentException {
        if (entityType == null) {
            throw new InvalidPropertiesFormatException("Entity type can't be NULL for current operation: " + CRUD.READ.toString());
        }
        final JsonNode idNode = data.get("id");
        final JsonNode appNode = data.get("app");
        Object resultAsJavaObject;
        if (idNode != null) {
            final String id = idNode.asText(" ");
            if (id == null || id.isEmpty()) {
                throw new InvalidFormatException("Name of metadata object should conform to the format: Namespace.Name");
            }
            resultAsJavaObject = getMetaObjectFromFQN(id, token);
        }
        else if (appNode != null) {
            final String app = appNode.asText(" ");
            if (app == null || app.isEmpty()) {
                throw new InvalidFormatException("Name of metadata object should conform to the format: Namespace.Name");
            }
            final String requestedNamespace = app.split("\\.")[0];
            final String requestedEntityType_asString = app.split("\\.")[1];
            final String requestedAppName = app.split("\\.")[2];
            final EntityType requestedEntityType = EntityType.valueOf(requestedEntityType_asString);
            resultAsJavaObject = ClientOperations.mdRepository.getByEntityTypeInApplication(requestedEntityType, requestedNamespace, requestedAppName, token);
        }
        else {
            resultAsJavaObject = ClientOperations.mdRepository.getByEntityType(entityType, token);
            if (entityType.equals(EntityType.PROPERTYTEMPLATE)) {
                final Set<MetaInfo.PropertyTemplateInfo> ptiList = (Set<MetaInfo.PropertyTemplateInfo>)resultAsJavaObject;
                for (final MetaInfo.PropertyTemplateInfo pt : ptiList) {
                    if (pt.adapterVersion != null && !pt.adapterVersion.equals("0.0.0")) {
                        pt.setName(pt.name + "_" + pt.adapterVersion);
                    }
                }
            }
        }
        return resultAsJavaObject;
    }
    
    private Object createOrUpdate(final QueryValidator clientInterface, final CRUD operation, final EntityType entityType, final Boolean cor, final ObjectNode data, final String namespace, final AuthToken requestToken, final AuthToken token) throws Exception {
        final ClientCreateOperation cco = new ClientCreateOperation();
        return cco.createOrUpdate(clientInterface, entityType, cor, data, namespace, requestToken, token);
    }
    
    private Object alter(final QueryValidator clientInterface, final CRUD operation, final EntityType entityType, final ObjectNode data, final String namespace, final AuthToken requestToken, final AuthToken token) throws Exception {
        final ClientCreateOperation cco = new ClientCreateOperation();
        return cco.alter(clientInterface, entityType, data, namespace, requestToken, token);
    }
    
    private Object delete(final QueryValidator clientInterface, final EntityType entityType, final String namespace, final ObjectNode data, final AuthToken requestToken) throws MetaDataRepositoryException {
        final String objectName = data.get("name").asText(" ");
        final boolean doCascade = true;
        final boolean doForce = false;
        final Context ctx = clientInterface.getContext(requestToken);
        final Object resultAsJavaObject = ClientOperations.mdRepository.getMetaObjectByName(entityType, namespace, objectName, null, ctx.getAuthToken());
        clientInterface.DropStatement(requestToken, entityType.toString(), objectName, doCascade, doForce);
        return resultAsJavaObject;
    }
    
    private void destroyContextForCurrentOperation(final QueryValidator clientInterface, final AuthToken requestToken) {
        if (requestToken != null && clientInterface != null) {
            clientInterface.removeContext(requestToken);
        }
    }
    
    public ObjectNode[] formatResult(final Object resultAsJavaObject) throws Exception {
        ObjectNode[] resultAsJsonObject;
        if (resultAsJavaObject == null) {
            resultAsJsonObject = new ObjectNode[] { this.jsonMapper.createObjectNode().putNull("noField") };
        }
        else if (resultAsJavaObject instanceof Set) {
            final Set<MetaInfo.MetaObject> allMetaObjectsAsSet = (Set<MetaInfo.MetaObject>)resultAsJavaObject;
            Utility.removeInternalApplicationsWithTypes(allMetaObjectsAsSet, true);
            resultAsJsonObject = new ObjectNode[allMetaObjectsAsSet.size()];
            int cc = 0;
            final Iterator<MetaInfo.MetaObject> itr = allMetaObjectsAsSet.iterator();
            while (itr.hasNext()) {
                resultAsJsonObject[cc++] = itr.next().getJsonForClient();
            }
        }
        else {
            resultAsJsonObject = new ObjectNode[] { ((MetaInfo.MetaObject)resultAsJavaObject).getJsonForClient() };
        }
        return resultAsJsonObject;
    }
    
    public boolean isUpdateOpertion(final CRUD operation) {
        if (operation.equals(CRUD.CREATE)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }
    
    public static MetaInfo.MetaObject getMetaObjectFromFQN(final String id, final AuthToken token) throws MetaDataRepositoryException {
        final String requestedNamespace = id.split("\\.")[0];
        final String requestedEntityType_asString = id.split("\\.")[1];
        final String requestedObjectname = id.split("\\.")[2];
        final EntityType requestedEntityType = EntityType.valueOf(requestedEntityType_asString);
        final MetaInfo.MetaObject resultAsJavaObject = ClientOperations.mdRepository.getMetaObjectByName(requestedEntityType, requestedNamespace, requestedObjectname, null, token);
        return resultAsJavaObject;
    }
    
    static {
        ClientOperations.logger = Logger.getLogger((Class)ClientOperations.class);
        ClientOperations.mdRepository = null;
    }
    
    private static class SingletonHolder
    {
        private static final ClientOperations INSTANCE;
        
        static {
            INSTANCE = new ClientOperations();
        }
    }
    
    public enum CRUD
    {
        CREATE('c'), 
        READ('r'), 
        UPDATE('u'), 
        DELETE('d');
        
        char op;
        
        private CRUD(final char cc) {
            this.op = cc;
        }
    }
}
