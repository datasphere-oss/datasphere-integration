package com.datasphere.web.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasphere.appmanager.FlowUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.MetaInfoStatus;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class ApplicationHelper
{
    private MetadataRepository _mdRpository;
    
    public ApplicationHelper() {
        this._mdRpository = MetadataRepository.getINSTANCE();
    }
    
    public Map<String, String> getApplicationsStatus(final AuthToken token, final String[] applicationIDs) throws Exception {
        final HashMap<String, String> statuses = new HashMap<String, String>();
        for (final String applicationID : applicationIDs) {
            final String[] applicationIdParts = applicationID.split("\\.");
            final String requestedNamespace = applicationIdParts[0];
            final String requestedEntityType_asString = applicationIdParts[1];
            final String requestedAppName = applicationIdParts[2];
            if (!requestedEntityType_asString.equals("APPLICATION")) {
                throw new RuntimeException("Type " + requestedEntityType_asString + "not supported.");
            }
            final MetaInfo.Flow flow = (MetaInfo.Flow)this._mdRpository.getMetaObjectByName(EntityType.APPLICATION, requestedNamespace, requestedAppName, null, token);
            if (flow == null) {
                throw new RuntimeException("Application " + requestedAppName + "is not found.");
            }
            statuses.put(flow.getFQN(), this.GetRealApplicationStatus(flow));
        }
        return statuses;
    }
    
    public Map<String, String> getAllApplicationStatuses(final AuthToken token) throws Exception {
        final HashMap<String, String> statuses = new HashMap<String, String>();
        final Set<MetaInfo.Flow> flows = (Set<MetaInfo.Flow>)this._mdRpository.getByEntityType(EntityType.APPLICATION, token);
        for (final MetaInfo.Flow flow : flows) {
            final MetaInfoStatus metaInfoStatus = flow.getMetaInfoStatus();
            if (!metaInfoStatus.isAdhoc() && !metaInfoStatus.isAnonymous()) {
                statuses.put(flow.getFQN(), this.GetRealApplicationStatus(flow));
            }
        }
        return statuses;
    }
    
    public Set<ObjectNode> getApplicationComponents(final AuthToken token, final String applicationOrFlowID, final Boolean deep) throws Exception {
        final Set<ObjectNode> components = new HashSet<ObjectNode>();
        final Collection<UUID> componentIDs = this.getComponentUUIDs(token, applicationOrFlowID, deep);
        for (final UUID uuid : componentIDs) {
            final MetaInfo.MetaObject metaObject = this._mdRpository.getMetaObjectByUUID(uuid, token);
            if (metaObject != null && !metaObject.getMetaInfoStatus().isDropped()) {
                components.add(metaObject.getJsonForClient());
            }
        }
        return components;
    }
    
    public Set<ObjectNode> getApplicationComponentsByType(final AuthToken token, final String applicationID, final String type, final Boolean deep) throws Exception {
        final Set<ObjectNode> components = new HashSet<ObjectNode>();
        final Collection<UUID> componentIDs = this.getComponentUUIDs(token, applicationID, deep);
        for (final UUID uuid : componentIDs) {
            final MetaInfo.MetaObject metaObject = this._mdRpository.getMetaObjectByUUID(uuid, token);
            if (metaObject != null && metaObject.getType().toString().equals(type)) {
                components.add(metaObject.getJsonForClient());
            }
        }
        return components;
    }
    
    public Map<EntityType, Integer> getApplicationRollup(final AuthToken token, final String applicationID) throws Exception {
        final String requestedNamespace = applicationID.split("\\.")[0];
        final String requestedEntityType_asString = applicationID.split("\\.")[1];
        final String requestedAppName = applicationID.split("\\.")[2];
        Set<UUID> components = new HashSet<UUID>();
        MetaInfo.Flow flow;
        if (requestedEntityType_asString.equals(EntityType.APPLICATION.toString())) {
            flow = (MetaInfo.Flow)this._mdRpository.getMetaObjectByName(EntityType.APPLICATION, requestedNamespace, requestedAppName, null, token);
            components = flow.getDeepDependencies();
        }
        else {
            if (!requestedEntityType_asString.equals(EntityType.FLOW.toString())) {
                throw new RuntimeException("" + requestedAppName + " is not an application or flow.");
            }
            flow = (MetaInfo.Flow)this._mdRpository.getMetaObjectByName(EntityType.FLOW, requestedNamespace, requestedAppName, null, token);
            final List<UUID> componentsList = flow.getDependencies();
            components = new HashSet<UUID>(components);
        }
        if (flow == null) {
            throw new RuntimeException("Application or Flow " + requestedAppName + " not found.");
        }
        return this.getRollup(components, token);
    }
    
    public Map<String, Map<EntityType, Integer>> getApplicationsRollup(final AuthToken token, final String[] applicationIDs) throws Exception {
        final HashMap<String, Map<EntityType, Integer>> rollups = new HashMap<String, Map<EntityType, Integer>>();
        for (final String applicationID : applicationIDs) {
            rollups.put(applicationID, this.getApplicationRollup(token, applicationID));
        }
        return rollups;
    }
    
    private Collection<UUID> getComponentUUIDs(final AuthToken token, final String applicationID, final Boolean deep) throws MetaDataRepositoryException {
        final String[] applicationIdParts = applicationID.split("\\.");
        final String requestedNamespace = applicationIdParts[0];
        final String requestedEntityType_asString = applicationIdParts[1];
        final String requestedAppName = applicationIdParts[2];
        final EntityType entityType = EntityType.valueOf(requestedEntityType_asString);
        if (entityType != EntityType.APPLICATION && entityType != EntityType.FLOW) {
            throw new RuntimeException(requestedAppName + " is not an application or flow.");
        }
        final MetaInfo.Flow flow = (MetaInfo.Flow)this._mdRpository.getMetaObjectByName(entityType, requestedNamespace, requestedAppName, null, token);
        if (flow == null) {
            throw new RuntimeException("Application or Flow " + requestedAppName + " not found.");
        }
        return (Collection<UUID>)(((boolean)deep) ? flow.getDeepDependencies() : flow.getDependencies());
    }
    
    private Map<EntityType, Integer> getRollup(final Set<UUID> uuids, final AuthToken token) throws MetaDataRepositoryException {
        final Map<EntityType, Integer> rollup = new HashMap<EntityType, Integer>();
        for (final UUID uuid : uuids) {
            final MetaInfo.MetaObject metaObject = this._mdRpository.getMetaObjectByUUID(uuid, token);
            if (metaObject != null) {
                if (rollup.containsKey(metaObject.getType())) {
                    Integer count = rollup.get(metaObject.getType());
                    ++count;
                    rollup.put(metaObject.getType(), count);
                }
                else {
                    rollup.put(metaObject.getType(), 1);
                }
            }
        }
        return rollup;
    }
    
    private String GetRealApplicationStatus(final MetaInfo.Flow flow) {
        try {
            return flow.getMetaInfoStatus().isValid() ? FlowUtil.getCurrentStatus(flow.getUuid()).getStatus().toString() : "INVALID";
        }
        catch (MetaDataRepositoryException e) {
            e.printStackTrace();
            return "INVALID";
        }
    }
}
