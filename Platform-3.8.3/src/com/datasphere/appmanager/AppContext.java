package com.datasphere.appmanager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.Event;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.WASecurityManager;
import com.datasphere.utility.DeployUtility;
import com.datasphere.utility.WaitUtility;
import com.datasphere.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Member;

public class AppContext implements Serializable
{
    private static final long serialVersionUID = -5924386859283245529L;
    private static Logger logger;
    private final UUID appId;
    private long epochNumber;
    private final Set<ExceptionEvent> exceptionEvents;
    private final Map<UUID, List<UUID>> managedNodes;
    private Set<UUID> waitingNodes;
    private final WaitUtility waitUtility;
    
    public AppContext(final UUID appId) throws MetaDataRepositoryException {
        this.exceptionEvents = new HashSet<ExceptionEvent>();
        this.managedNodes = new HashMap<UUID, List<UUID>>();
        this.waitingNodes = null;
        this.waitUtility = new WaitUtility();
        this.appId = appId;
        this.epochNumber = System.currentTimeMillis();
        this.updateStatus(MetaInfo.StatusInfo.Status.CREATED);
    }
    
    public Long getEpochNumber() {
        return this.epochNumber;
    }
    
    public synchronized Long incrementAndGetEpochNumber() {
        final long currentTimeMillis = System.currentTimeMillis();
        if (this.epochNumber < currentTimeMillis) {
            this.epochNumber = currentTimeMillis;
        }
        else if (this.epochNumber == currentTimeMillis) {
            this.epochNumber = currentTimeMillis + 1L;
        }
        else {
            ++this.epochNumber;
        }
        return this.epochNumber;
    }
    
    public void close() {
    }
    
    public MetaInfo.StatusInfo.Status getDesiredAppStatus() throws MetaDataRepositoryException {
        final MDRepository metadataRepository = MetadataRepository.getINSTANCE();
        final MetaInfo.Flow flow = (MetaInfo.Flow)metadataRepository.getMetaObjectByUUID(this.appId, WASecurityManager.TOKEN);
        return flow.getFlowStatus();
    }
    
    public List<UUID> getManagedNodes() {
        return new ArrayList<UUID>(this.managedNodes.keySet());
    }
    
    public void clearExceptions() {
        this.exceptionEvents.clear();
    }
    
    public MetaInfo.StatusInfo.Status getCurrentStatus() throws MetaDataRepositoryException {
        return FlowUtil.getCurrentStatus(this.appId).getStatus();
    }
    
    public void updateStatus(final MetaInfo.StatusInfo.Status newState) throws MetaDataRepositoryException {
        FlowUtil.updateFlowActualStatus(this.appId, newState);
    }
    
    public MetaInfo.Flow getFlow() throws MetaDataRepositoryException {
        return FlowUtil.getFlow(this.appId);
    }
    
    public UUID getAppId() {
        return this.appId;
    }
    
    public String getAppName() throws MetaDataRepositoryException {
        return this.getFlow().getName();
    }
    
    public void addManagedNode(final UUID serverId, final List<UUID> flows) {
        if (this.managedNodes.get(serverId) == null) {
            this.managedNodes.put(serverId, flows);
        }
        else {
            this.managedNodes.get(serverId).addAll(flows);
        }
    }
    
    public boolean removedManagedNode(final UUID serverId) {
        return this.managedNodes.remove(serverId) != null;
    }
    
    public boolean removedManagedNodes(final List<UUID> serverIds) {
        boolean result = false;
        for (final UUID serverId : serverIds) {
            if (this.removedManagedNode(serverId)) {
                result = true;
            }
        }
        return result;
    }
    
    public void updateDesiredStatus(final MetaInfo.StatusInfo.Status status) throws Exception {
        DesiredStateManager.updateFlowDesiredStatus(status, this.getAppId());
    }
    
    public void execute(final ActionType action, final boolean incrementEpoch) throws MetaDataRepositoryException {
        this.execute(action, null, incrementEpoch);
    }
    
    public void execute(final ActionType action, final List<Property> params, final boolean incrementEpoch) throws MetaDataRepositoryException {
        this.execute(action, this.getFlow(), params, incrementEpoch);
    }
    
    public void execute(final ActionType action, final Long epoch, final List<UUID> servers) throws MetaDataRepositoryException {
        final MetaInfo.Flow flow = this.getFlow();
        AppContext.logger.info((Object)("Executing action " + action + " for flow " + flow.getFullName() + " with Epoch " + epoch + " on nodes: " + this.getManagedNodes()));
        this.execute(new Context.ChangeFlowState(action, this.getFlow(), servers, (UUID)WASecurityManager.TOKEN, null, epoch), servers);
    }
    
    public void execute(final ActionType action, final boolean incrementEpoch, final List<UUID> servers) throws MetaDataRepositoryException {
        final long epoch = incrementEpoch ? this.incrementAndGetEpochNumber() : this.getEpochNumber();
        final MetaInfo.Flow flow = this.getFlow();
        AppContext.logger.info((Object)("Executing action " + action + " for flow " + flow.getFullName() + " with Epoch " + epoch + " on nodes: " + this.getManagedNodes()));
        this.execute(new Context.ChangeFlowState(action, this.getFlow(), servers, (UUID)WASecurityManager.TOKEN, null, epoch), servers);
    }
    
    public void execute(final ActionType action, final MetaInfo.Flow flow, final List<Property> params, final boolean incrementEpoch) throws MetaDataRepositoryException {
        final long epoch = incrementEpoch ? this.incrementAndGetEpochNumber() : this.getEpochNumber();
        AppContext.logger.info((Object)("Executing action " + action + " for flow " + flow.getFullName() + " with Epoch " + epoch + " on nodes: " + this.getManagedNodes()));
        this.execute(new Context.ChangeFlowState(action, flow, this.getManagedNodes(), (UUID)WASecurityManager.TOKEN, params, epoch), this.getManagedNodes());
    }
    
    public void execute(final ActionType action, final boolean incrementEpoch, final List<UUID> servers, final List<UUID> flows) throws MetaDataRepositoryException {
        final long epoch = incrementEpoch ? this.incrementAndGetEpochNumber() : this.getEpochNumber();
        AppContext.logger.info((Object)("Executing action " + action + " for flow " + this.getFlow().getFullName() + " with Epoch " + epoch));
        this.execute(new Context.ChangeFlowState(action, this.getFlow(), flows, (UUID)WASecurityManager.TOKEN, null, epoch), servers);
    }
    
    public void execute(final ActionType action, final ArrayList<UUID> servers, final List<Property> params, final boolean incrementEpoch) throws MetaDataRepositoryException {
        final long epoch = incrementEpoch ? this.incrementAndGetEpochNumber() : this.getEpochNumber();
        AppContext.logger.info((Object)("Executing action " + action + " for flow " + this.getFlow().getFullName() + " with Epoch " + epoch));
        this.execute(new Context.ChangeFlowState(action, this.getFlow(), servers, (UUID)WASecurityManager.TOKEN, params, epoch), servers);
    }
    
    public void resetCaches() {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final IMap<String, Long> cacheStatus = hz.getMap("#cacheStatus-" + this.appId);
        cacheStatus.clear();
        hz.getMap("#cacheNodeSnapshotMap-" + this.appId).clear();
        hz.getMap("#cacheSnapshotIdQueue-" + this.appId).clear();
    }
    
    private void execute(final Callable<Context.Result> action, final List<UUID> serverIds) {
        if (AppContext.logger.isInfoEnabled()) {
            AppContext.logger.info((Object)("executing action " + ((Context.ChangeFlowState)action).getAction() + " on servers " + serverIds));
        }
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        final Map<UUID, Member> valid = new HashMap<UUID, Member>();
        for (final Member member : srvs) {
            valid.put(new UUID(member.getUuid()), member);
        }
        final Set<Member> executeSrvs = new HashSet<Member>();
        final Set<UUID> executeAgents = new HashSet<UUID>();
        for (final UUID serverId : serverIds) {
            final Member member2 = valid.get(serverId);
            if (member2 != null) {
                executeSrvs.add(member2);
            }
            else {
                executeAgents.add(serverId);
            }
        }
        if (!executeSrvs.isEmpty()) {
            DistributedExecutionManager.exec(hz, action, executeSrvs);
        }
        if (!executeAgents.isEmpty()) {
            DistributedExecutionManager.sendMessage(hz, executeAgents, action);
        }
        if (AppContext.logger.isInfoEnabled()) {
            AppContext.logger.info((Object)("Finished executing action " + ((Context.ChangeFlowState)action).getAction() + " on servers " + serverIds));
        }
    }
    
    public void addExceptionEvent(final ExceptionEvent exceptionEvent) {
        this.exceptionEvents.add(exceptionEvent);
    }
    
    public Set<ExceptionEvent> getExceptionEvents() {
        return this.exceptionEvents;
    }
    
    public void waitForSubscriptionsCompleted() throws InterruptedException, MetaDataRepositoryException {
        int count = 0;
        AppContext.logger.info((Object)("Waiting for subscriptions for the application " + this.getFlow().getFullName()));
        while (count < 10) {
            try {
                this.execute(ActionType.VERIFY_START, false);
                AppContext.logger.info((Object)("Waiting for all the subscriptions completed for " + this.getFlow().getFullName()));
                return;
            }
            catch (Throwable e) {
                AppContext.logger.info((Object)("Failed to verify start application " + this.getFlow().getFullName() + " with exception " + e.getMessage() + " retrying..." + count));
                Thread.sleep(1000L);
                ++count;
                continue;
            }
        }
        throw new RuntimeException("Failed to verify application start");
    }
    
    @Override
    public String toString() {
        try {
            return "AppId: " + this.appId + " AppName: " + this.getFlow().getName() + " Epoch Number: " + this.epochNumber + " Current Status: " + this.getCurrentStatus() + " desired status " + this.getDesiredAppStatus() + " exceptions : " + this.exceptionEvents + " managed nodes " + this.managedNodes;
        }
        catch (MetaDataRepositoryException e) {
            return "AppId " + this.appId;
        }
    }
    
    public Map<UUID, List<UUID>> deployFlowOnNodes(final List<UUID> serverIds) throws Exception {
        final Map<UUID, Long> appDeploymentMap = new HashMap<UUID, Long>();
        for (final UUID serverId : serverIds) {
            appDeploymentMap.put(serverId, new Long(0L));
        }
        final IMap<UUID, AppContext> managedApplications = HazelcastSingleton.get().getMap("#AppManagerManagedApplications");
        for (final AppContext appContext : managedApplications.values()) {
            if (appContext.getFlow().getNsName().equals("Global")) {
                continue;
            }
            for (final UUID managedServerId : appContext.getManagedNodes()) {
                Long appCount = appDeploymentMap.get(managedServerId);
                if (appCount != null) {
                    ++appCount;
                    appDeploymentMap.put(managedServerId, appCount);
                }
            }
        }
        final Map<UUID, List<UUID>> serverFlowMap = DeployUtility.getServerFlowDeploymentMap(this.getFlow(), serverIds, this.managedNodes, appDeploymentMap);
        for (final Map.Entry<UUID, List<UUID>> entry : serverFlowMap.entrySet()) {
            final List<UUID> flowsToDeploy = new ArrayList<UUID>(entry.getValue());
            if (this.managedNodes.get(entry.getKey()) != null) {
                flowsToDeploy.removeAll(this.managedNodes.get(entry.getKey()));
            }
            if (!flowsToDeploy.isEmpty()) {
                final List<UUID> servers = new ArrayList<UUID>();
                servers.add(entry.getKey());
                this.execute(ActionType.DEPLOY, true, servers, flowsToDeploy);
                this.addManagedNode(entry.getKey(), flowsToDeploy);
            }
        }
        return serverFlowMap;
    }
    
    public boolean hasEnoughServers() throws Exception {
        final ArrayList<UUID> nodes = new ArrayList<UUID>(this.managedNodes.keySet());
        return DeployUtility.hasRequiredServers(this.getFlow().getSubFlows(), nodes, this.getFlow());
    }
    
    public void stopCaches() throws MetaDataRepositoryException {
        this.execute(ActionType.STOP_CACHES, false);
    }
    
    public int removedAndCheckWaitStatusForCommandEvents(final Event.EventAction eventAction, final Long commandTimestamp, final UUID nodeId) {
        return this.waitUtility.stopWaitingOn(eventAction, commandTimestamp, nodeId);
    }
    
    public int removedAndCheckWaitStatus(final UUID nodeId) {
        this.waitingNodes.remove(nodeId);
        if (this.waitingNodes.isEmpty()) {
            this.waitingNodes = null;
            return 0;
        }
        return this.waitingNodes.size();
    }
    
    public void startCaches() throws MetaDataRepositoryException {
        AppContext.logger.info((Object)("Starting cache " + this.getFlow().getFullName()));
        try {
            this.execute(ActionType.START_CACHES, true);
        }
        catch (Exception e) {
            this.execute(ActionType.STOP_CACHES, true);
            throw e;
        }
        this.startWaitingForResponses(this.managedNodes.keySet());
        AppContext.logger.info((Object)("Starting cache load for " + this.getFlow().getFullName()));
    }
    
    public void startWaitingForResponses(final Set<UUID> nodes) {
        if (this.waitingNodes != null) {
            throw new IllegalStateException("WaitingNodes is not null");
        }
        this.waitingNodes = new HashSet<UUID>(nodes);
    }
    
    public void startWaitingForCommandNotifications(final Event.EventAction eventAction, final Long commandTimestamp, final Set<UUID> nodes) {
        this.waitUtility.startWaiting(eventAction, commandTimestamp, nodes);
    }
    
    public void addEventToDeferredQueue(final Event event) {
        this.getQueue().add(event);
    }
    
    public Event getNextEventFromDeferredQueue() {
        return (Event)this.getQueue().poll();
    }
    
    private IQueue<Event> getQueue() {
        return HazelcastSingleton.get().getQueue(this.appId.getUUIDString());
    }
    
    static {
        AppContext.logger = Logger.getLogger((Class)AppContext.class);
    }
}
