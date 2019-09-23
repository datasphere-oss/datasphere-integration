package com.datasphere.utility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.exception.ServerException;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.DeploymentStrategy;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class DeployUtility
{
    private static Logger logger;
    
    public static List<UUID> canAnyFlowOrSubFlowBeDeployed(final MetaInfo.Flow flow, final UUID serverId) throws Exception {
        final List<UUID> flowsThatCanBeDeployed = new ArrayList<UUID>();
        boolean canFlowBeDeployed = canFlowBeDeployed(flow, serverId, null);
        if (canFlowBeDeployed) {
            flowsThatCanBeDeployed.add(flow.getUuid());
        }
        for (final UUID id : Flow.getLatestVersions(flow.getObjects(EntityType.FLOW), EntityType.FLOW)) {
            final MetaInfo.Flow o = Server.server.getObjectInfo(id, EntityType.FLOW);
            canFlowBeDeployed = canFlowBeDeployed(o, serverId, flow);
            if (canFlowBeDeployed) {
                flowsThatCanBeDeployed.add(id);
            }
        }
        return flowsThatCanBeDeployed;
    }
    
    public static boolean canFlowBeDeployed(final MetaInfo.Flow flow, final UUID serverId, final MetaInfo.Flow parent) throws Exception {
        final String apps = System.getProperty("com.datasphere.config.doNotDeployApps");
        final List<String> doNotDeployAppList = new ArrayList<String>();
        if (apps != null) {
            doNotDeployAppList.addAll(Arrays.asList(apps.split(",")));
        }
        if (doNotDeployAppList.contains(flow.getName()) || doNotDeployAppList.contains("*")) {
            return false;
        }
        final MetaInfo.Flow.Detail detail = haveDeploymentDetail(flow, parent);
        if (detail == null) {
            DeployUtility.logger.info((Object)("Flow " + flow.uuid + " cannot be deployed because deployment detail not found for flow"));
            return false;
        }
        if (checkStrategy(detail, serverId)) {
            DeployUtility.logger.info((Object)("Flow " + flow.uuid + " can be deployed on the server " + serverId));
            return true;
        }
        DeployUtility.logger.info((Object)("Flow " + flow.uuid + " cannot be deployed on the server " + serverId));
        return false;
    }
    
    public static boolean inDeploymentGroup(final UUID deploymentGroupID, final UUID serverId) {
        final List<UUID> deploymentGroups = (List<UUID>)HazelcastSingleton.get().getMap("#serverToDeploymentGroup").get((Object)serverId);
        DeployUtility.logger.info((Object)("List of DGs server " + serverId + " belongs to " + deploymentGroups + " . DG being looked for " + deploymentGroupID));
        return deploymentGroups != null && deploymentGroups.contains(deploymentGroupID);
    }
    
    protected static boolean checkStrategy(final MetaInfo.Flow.Detail d, final UUID serverId) throws Exception {
        DeployUtility.logger.info((Object)("Checking strategy if Flow " + d.flow + " can be deployed on the server " + serverId));
        if (!inDeploymentGroup(d.deploymentGroup, serverId)) {
            DeployUtility.logger.info((Object)("Flow " + d.flow + " cannot be deployed on the server " + serverId + " is not in the deployment group"));
            return false;
        }
        final MetaInfo.DeploymentGroup dg = getDeploymentGroupByID(d.deploymentGroup);
        if (d.strategy == DeploymentStrategy.ON_ALL) {
            DeployUtility.logger.info((Object)("Flow " + d.flow + " can be deployed on the server " + serverId + " because deployment strategy for the flow is ON_ALL"));
            return true;
        }
        final Long val = dg.groupMembers.get(serverId);
        if (val == null) {
            throw new RuntimeException("Flow " + d.flow + " cannot be deployed because server " + serverId + " is not in <" + dg.name + "> deployment group");
        }
        final long idInGroup = val;
        DeployUtility.logger.info((Object)("Found server " + serverId + " is in DG " + dg.name + " with id " + idInGroup));
        for (final long id : dg.groupMembers.values()) {
            DeployUtility.logger.info((Object)("checking " + idInGroup + " against " + id));
            if (idInGroup > id) {
                DeployUtility.logger.info((Object)("Flow " + d.flow + " cannot be deployed because the server " + serverId + " is not in lowest id in the deployment group"));
                return false;
            }
        }
        return true;
    }
    
    public static MetaInfo.DeploymentGroup getDeploymentGroupByID(final UUID uuid) throws ServerException, MetaDataRepositoryException {
        final MDRepository MDRepository = MetadataRepository.getINSTANCE();
        final MetaInfo.MetaObject o = MDRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
        if (o != null && o.type == EntityType.DG) {
            return (MetaInfo.DeploymentGroup)o;
        }
        throw new ServerException("cannot find metadata for " + EntityType.DG.name() + " " + uuid);
    }
    
    public static boolean hasRequiredServers(final List<MetaInfo.Flow> flows, final List<UUID> serverIds, final MetaInfo.Flow parent) throws Exception {
        if (serverIds.size() == 0) {
            return false;
        }
        final Set<UUID> deploymentGroups = new HashSet<UUID>();
        MetaInfo.Flow.Detail detail = haveDeploymentDetail(parent, null);
        if (detail == null) {
            throw new Exception("No deployment plan found");
        }
        deploymentGroups.add(detail.deploymentGroup);
        for (final MetaInfo.Flow flow : flows) {
            detail = haveDeploymentDetail(flow, parent);
            deploymentGroups.add(detail.deploymentGroup);
        }
        for (final UUID deploymentGroup : deploymentGroups) {
            int count = 0;
            for (final UUID serverId : serverIds) {
                if (inDeploymentGroup(deploymentGroup, serverId)) {
                    ++count;
                }
            }
            final MetaInfo.DeploymentGroup dg = Server.getServer().getDeploymentGroupByID(deploymentGroup);
            if (count < dg.getMinimumRequiredServers()) {
                DeployUtility.logger.info((Object)("Not enough servers in DG " + dg.name + " Required minimum " + dg.getMinimumRequiredServers() + " Actual: " + count));
                return false;
            }
        }
        DeployUtility.logger.info((Object)"All DGs have minimum number of servers to continue");
        return true;
    }
    
    public static MetaInfo.Flow.Detail haveDeploymentDetail(final MetaInfo.Flow o, final MetaInfo.Flow parent) {
        if (parent != null) {
            for (final MetaInfo.Flow.Detail d : parent.deploymentPlan) {
                if (o.uuid.equals((Object)d.flow)) {
                    return d;
                }
            }
            return parent.deploymentPlan.get(0);
        }
        if (o.deploymentPlan != null) {
            return o.deploymentPlan.get(0);
        }
        return null;
    }
    
    public static void removeServerFromDeploymentGroup(final UUID serverId) throws MetaDataRepositoryException {
        final List<UUID> deploymentGroups = (List<UUID>)HazelcastSingleton.get().getMap("#serverToDeploymentGroup").get((Object)serverId);
        if (deploymentGroups == null) {
            return;
        }
        MetaInfo.DeploymentGroup removedServerDeploymentGroup = null;
        final MDRepository MDRepository = MetadataRepository.getINSTANCE();
        for (final UUID deploymentGroup : deploymentGroups) {
            removedServerDeploymentGroup = (MetaInfo.DeploymentGroup)MDRepository.getMetaObjectByUUID(deploymentGroup, HSecurityManager.TOKEN);
            removedServerDeploymentGroup.removeMember(serverId);
            if (DeployUtility.logger.isInfoEnabled()) {
                DeployUtility.logger.info((Object)("Node " + serverId + " is removed from deployment group " + removedServerDeploymentGroup));
            }
            MDRepository.putMetaObject(removedServerDeploymentGroup, HSecurityManager.TOKEN);
        }
        deploymentGroups.remove(serverId);
    }
    
    public static Map<UUID, List<UUID>> getServerFlowDeploymentMap(final MetaInfo.Flow flow, final List<UUID> serverIds, final Map<UUID, List<UUID>> managedNodes, final Map<UUID, Long> appDeploymentMap) throws Exception {
        final Map<UUID, List<UUID>> serverFlowMap = new HashMap<UUID, List<UUID>>();
        List<UUID> servers = canFlowBeDeployed(flow, null, serverIds, managedNodes, appDeploymentMap);
        addToServerFlowMap(flow, serverFlowMap, servers);
        for (final UUID id : Flow.getLatestVersions(flow.getObjects(EntityType.FLOW), EntityType.FLOW)) {
            final MetaInfo.Flow o = Server.server.getObjectInfo(id, EntityType.FLOW);
            servers = canFlowBeDeployed(o, flow, serverIds, managedNodes, appDeploymentMap);
            addToServerFlowMap(o, serverFlowMap, servers);
        }
        return serverFlowMap;
    }
    
    private static void addToServerFlowMap(final MetaInfo.Flow flow, final Map<UUID, List<UUID>> serverFlowMap, final List<UUID> servers) {
        for (final UUID server : servers) {
            List<UUID> flows = serverFlowMap.get(server);
            if (flows == null) {
                flows = new ArrayList<UUID>();
            }
            flows.add(flow.getUuid());
            serverFlowMap.put(server, flows);
        }
    }
    
    public static List<UUID> canFlowBeDeployed(final MetaInfo.Flow flow, final MetaInfo.Flow parent, final List<UUID> serverIds, final Map<UUID, List<UUID>> managedNodes, final Map<UUID, Long> appDeploymentMap) throws Exception {
        final List<UUID> serverList = new ArrayList<UUID>();
        final String apps = System.getProperty("com.datasphere.config.doNotDeployApps");
        final List<String> doNotDeployAppList = new ArrayList<String>();
        if (apps != null) {
            doNotDeployAppList.addAll(Arrays.asList(apps.split(",")));
        }
        if (doNotDeployAppList.contains(flow.getName()) || doNotDeployAppList.contains("*")) {
            return serverList;
        }
        final MetaInfo.Flow.Detail detail = haveDeploymentDetail(flow, parent);
        if (detail == null) {
            DeployUtility.logger.info((Object)("Flow " + flow.uuid + " cannot be deployed because deployment detail not found for flow"));
            return serverList;
        }
        final MetaInfo.DeploymentGroup dg = getDeploymentGroupByID(detail.deploymentGroup);
        if (detail.strategy == DeploymentStrategy.ON_ALL) {
            return getServersForDeploymentAll(flow, dg, serverIds, managedNodes);
        }
        return getServerForDeploymentAny(flow, dg, appDeploymentMap, serverIds, managedNodes);
    }
    
    private static List<UUID> getServersForDeploymentAll(final MetaInfo.Flow flow, final MetaInfo.DeploymentGroup dg, final List<UUID> serverIds, final Map<UUID, List<UUID>> managedNodes) {
        final List<UUID> servers = new ArrayList<UUID>();
        for (final UUID server : serverIds) {
            if (dg.groupMembers.containsKey(server) && (managedNodes.get(server) == null || !managedNodes.get(server).contains(flow.uuid))) {
                servers.add(server);
            }
        }
        return servers;
    }
    
    private static List<UUID> getServerForDeploymentAny(final MetaInfo.Flow flow, final MetaInfo.DeploymentGroup dg, final Map<UUID, Long> appDeploymentMap, final List<UUID> serverIds, final Map<UUID, List<UUID>> managedNodes) {
        for (final List<UUID> deployedFlows : managedNodes.values()) {
            for (final UUID deployedFlow : deployedFlows) {
                if (deployedFlow.equals((Object)flow.getUuid())) {
                    return new ArrayList<UUID>();
                }
            }
        }
        if (!System.getProperty("com.datasphere.config.deployOnFirst", "false").equalsIgnoreCase("true")) {
            UUID leastBusyServer = null;
            Long appCountOnLeastBusyServer = Long.MAX_VALUE;
            for (final UUID server : dg.groupMembers.keySet()) {
                if (serverIds.contains(server)) {
                    final Long appCount = appDeploymentMap.get(server);
                    if ((appCount != null && appCount >= appCountOnLeastBusyServer) || appCount >= dg.getMaxApps()) {
                        continue;
                    }
                    leastBusyServer = server;
                    appCountOnLeastBusyServer = appCount;
                }
            }
            final List<UUID> servers = new ArrayList<UUID>();
            if (leastBusyServer != null) {
                servers.add(leastBusyServer);
            }
            return servers;
        }
        final UUID oldestServerToDeployOnAny = getOldestServerToDeployOnAny(dg, serverIds, appDeploymentMap);
        if (oldestServerToDeployOnAny == null) {
            return new ArrayList<UUID>();
        }
        final List<UUID> oldestServerList = new ArrayList<UUID>();
        oldestServerList.add(oldestServerToDeployOnAny);
        return oldestServerList;
    }
    
    private static UUID getOldestServerToDeployOnAny(final MetaInfo.DeploymentGroup dg, final List<UUID> serversToDeployOn, final Map<UUID, Long> appDeploymentMap) {
        if (dg.groupMembers.isEmpty() || serversToDeployOn.isEmpty()) {
            return null;
        }
        long smallestId = 2147483647L;
        UUID firstServer = null;
        for (final Map.Entry<UUID, Long> entry : dg.groupMembers.entrySet()) {
            if (entry.getValue() < smallestId && serversToDeployOn.contains(entry.getKey())) {
                final Long appCount = appDeploymentMap.get(entry.getKey());
                if (appCount != null && appCount >= dg.getMaxApps()) {
                    continue;
                }
                firstServer = entry.getKey();
                smallestId = entry.getValue();
            }
        }
        return firstServer;
    }
    
    static {
        DeployUtility.logger = Logger.getLogger((Class)DeployUtility.class);
    }
}
