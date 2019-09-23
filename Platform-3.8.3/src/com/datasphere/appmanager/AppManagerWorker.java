package com.datasphere.appmanager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.datasphere.appmanager.event.ApiCallEvent;
import com.datasphere.appmanager.event.AppManagerEvent;
import com.datasphere.appmanager.event.CommandConfirmationEvent;
import com.datasphere.appmanager.event.Event;
import com.datasphere.appmanager.event.MembershipEvent;
import com.datasphere.appmanager.event.NodeEvent;
import com.datasphere.appmanager.event.NodeEventCallback;
import com.datasphere.exception.ActionNotFoundWarning;
import com.datasphere.exception.Warning;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.WASecurityManager;
import com.datasphere.utility.DeployUtility;
import com.datasphere.uuid.UUID;

class AppManagerWorker extends Thread implements NodeEventCallback
{
    public static final String DEPLOYMENT_PLAN = "DEPLOYMENT_PLAN_PARAM";
    private static Logger logger;
    MDRepository metadataRepository;
    private final BaseServer srv;
    private IMap<UUID, AppContext> managedApplications;
    private ISet<UUID> managedNodes;
    private ISet<UUID> managedAgents;
    public volatile boolean hasLock;
    public volatile boolean running;
    private final NodeMembershipListener nodeMembershipListener;
    private final EventQueueManager eventQueueManager;
    private final CheckpointScheduler checkpointManager;
    
    public AppManagerWorker(final BaseServer srv) throws Exception {
        super("AppManagerWorkerThread");
        this.metadataRepository = MetadataRepository.getINSTANCE();
        this.hasLock = false;
        this.running = true;
        this.managedApplications = HazelcastSingleton.get().getMap("#AppManagerManagedApplications");
        this.managedNodes = HazelcastSingleton.get().getSet("#AppManagerManagedNodes");
        this.managedAgents = HazelcastSingleton.get().getSet("#AppManagerManagedAgents");
        this.srv = srv;
        this.nodeMembershipListener = new NodeMembershipListener(this);
        this.eventQueueManager = EventQueueManager.get();
        this.checkpointManager = new CheckpointScheduler();
    }
    
    public void stopAppManager() {
        this.running = false;
        this.interrupt();
        try {
            this.join();
        }
        catch (InterruptedException e) {
            AppManagerWorker.logger.warn(("Got exception while waiting for AppManager thread to exit " + e.getMessage()));
        }
    }
    
    @Override
    public void run() {
        this.eventQueueManager.addMembershipEventToQueue(Event.EventAction.NODE_ADDED, this.srv.getServerID());
        final ILock lock = HazelcastSingleton.get().getLock("#AppManagerLeaderLock");
        try {
            lock.lock();
            if (!this.running) {
                return;
            }
            this.hasLock = true;
            AppManagerWorker.logger.info("Got the appManager lock");
            if (this.managedApplications.isEmpty()) {
                this.loadApplicationsToManage();
                for (final Map.Entry<UUID, AppContext> application : this.managedApplications.entrySet()) {
                    try {
                        this.bringAppToDesiredStateLocal(application.getKey(), application.getValue());
                    }
                    catch (Exception e) {
                        AppManagerWorker.logger.error(("got exception " + e.getMessage() + " in AppManager "));
                    }
                }
            }
            final List<UUID> servers = this.nodeMembershipListener.getAllServers();
            for (final UUID managedNode : this.managedNodes) {
                if (servers.contains(managedNode)) {
                    continue;
                }
                this.eventQueueManager.addMembershipEventToQueue(Event.EventAction.NODE_DELETED, managedNode);
            }
            AppManagerServerLocation.setAppManagerServerId(this.srv.getServerID());
            this.eventQueueManager.addItemListener(this);
            while (this.running && lock.isLocked()) {
                Thread.sleep(5000L);
            }
        }
        catch (Exception e2) {
            AppManagerWorker.logger.error(("got exception " + e2.getMessage() + " in AppManager "));
        }
        finally {
            this.cleanup();
            if (lock.isLocked()) {
                AppManagerWorker.logger.info("Releasing appManager lock");
                lock.unlock();
            }
        }
    }
    
    private void cleanup() {
        if (this.hasLock) {
            AppManagerServerLocation.removeAppManagerServerId(this.srv.getServerID());
            this.eventQueueManager.removeItemListener();
            this.hasLock = false;
        }
    }
    
    private void handleMembershipEvent(final MembershipEvent event) {
        if (event.getEventAction() == Event.EventAction.NODE_DELETED) {
            try {
                DeployUtility.removeServerFromDeploymentGroup(event.getMember());
            }
            catch (MetaDataRepositoryException e) {
                AppManagerWorker.logger.error(("Failed to handle event " + event + " with exception " + e.getMessage() + " when removing server from deployment groups"));
            }
            this.managedNodes.remove(event.getMember());
        }
        if (event.getEventAction() == Event.EventAction.NODE_ADDED) {
            this.managedNodes.add(event.getMember());
        }
        if (event.getEventAction() == Event.EventAction.AGENT_NODE_ADDED) {
            this.managedAgents.add(event.getMember());
        }
        if (event.getEventAction() == Event.EventAction.AGENT_NODE_DELETED) {
            if (this.managedAgents.contains(event.getMember())) {
                AppManagerWorker.logger.info(("Removing agent " + event.getMember()));
            }
            this.managedAgents.remove(event.getMember());
        }
        for (final Map.Entry<UUID, AppContext> application : this.managedApplications.entrySet()) {
            try {
                this.runFlow(event, application.getKey(), event.getEventAction());
            }
            catch (Throwable e2) {
                AppManagerWorker.logger.error(("Failed to handle event " + event + " with exception " + e2.getMessage()));
            }
        }
    }
    
    private void handleNodeEvent(final NodeEvent event) {
        final UUID flowId = event.getFlowId();
        try {
            this.runFlow(event, flowId, event.getEventAction());
        }
        catch (Throwable e) {
            AppManagerWorker.logger.error(("Failed to handle event [ " + event + " ] with exception " + e.getMessage()), e);
        }
    }
    
    private void handleCommandConfirmationEvent(final CommandConfirmationEvent event) {
        final UUID flowId = event.getFlowId();
        try {
            this.runFlow(event, flowId, event.getEventAction());
        }
        catch (Throwable e) {
            AppManagerWorker.logger.warn(("Failed to handle event [ " + event + " ] with exception " + e.getMessage()));
        }
    }
    
    private ExceptionEvent getExceptionEvent(final UUID flowId, final Throwable exception) {
        final ExceptionEvent ee = new ExceptionEvent();
        ee.setAppid(flowId);
        ee.setType(ExceptionType.UnknownException);
        ee.setMessage(exception.getMessage());
        return ee;
    }
    
    private String getFlowName(final UUID flowId) {
        try {
            final MetaInfo.Flow flow = FlowUtil.getFlow(flowId);
            return flow.getName();
        }
        catch (MetaDataRepositoryException e) {
            return "UNKNOWN";
        }
    }
    
    private void handleApiEvent(final ApiCallEvent event) {
        String exception = null;
        final ActionType newEventAction = event.getActionType();
        final UUID flowId = event.getFlowId();
        if (event.getEventAction() == Event.EventAction.API_STATUS) {
            this.sendStatusNotification(event.getRequestId(), flowId);
            return;
        }
        AppManagerWorker.logger.info(("Dequeued new state change request for application " + flowId + " to move to new state " + newEventAction));
        try {
            this.runFlow(event, flowId, event.getEventAction());
            if (event.getEventAction() == Event.EventAction.API_UNDEPLOY) {
                this.managedApplications.remove(flowId);
            }
        }
        catch (Throwable e) {
            AppManagerWorker.logger.error(("Got exception " + e.getMessage() + " for application " + flowId), e);
            exception = e.getMessage();
        }
        finally {
            if (event instanceof ApiCallEvent) {
                this.sendCompletionNotification(event.getRequestId(), newEventAction, exception);
            }
        }
    }
    
    private void runFlow(final Event event, final UUID flowId, final Event.EventAction eventAction) throws Exception {
        if (eventAction == Event.EventAction.AGENT_NODE_ADDED || eventAction == Event.EventAction.AGENT_NODE_DELETED) {
            return;
        }
        AppContext appContext = (AppContext)this.managedApplications.get(flowId);
        AppManagerWorker.logger.info(("Handle event " + event + " for flow " + flowId));
        if (appContext == null) {
            if (eventAction == Event.EventAction.API_DEPLOY) {
                appContext = new AppContext(flowId);
            }
            else {
                final MetaInfo.Flow flow = FlowUtil.getFlow(flowId);
                if (flow != null) {
                    throw new Warning("Application " + flow.getName() + " is not deployed. Please deploy the application first");
                }
                throw new Warning("Application does not exist for flowId " + flowId);
            }
        }
        try {
            final ArrayList<UUID> managedNodesLocal = new ArrayList<UUID>((Collection<? extends UUID>)this.managedNodes);
            if (eventAction.sendToAgents()) {
                managedNodesLocal.addAll((Collection<? extends UUID>)this.managedAgents);
            }
            ApplicationStateMachine.runStateFlow(eventAction, appContext, event, managedNodesLocal);
        }
        catch (Exception e) {
            if (event instanceof ApiCallEvent || e instanceof ActionNotFoundWarning) {
                throw e;
            }
            this.handleEventHandlingFailure(event, appContext, e);
        }
        this.managedApplications.put(flowId, appContext);
    }
    
    private void handleEventHandlingFailure(final Event event, final AppContext appContext, final Throwable e) {
        final UUID flowId = appContext.getAppId();
        if (event != null) {
            AppManagerWorker.logger.warn(("Failed to handle event [ " + event + " ] with exception " + e.getMessage() + "...Putting Flow " + this.getFlowName(flowId) + " in CRASH state"));
        }
        else {
            AppManagerWorker.logger.warn(("Handling exception " + e.getMessage() + "...Putting Flow " + this.getFlowName(flowId) + " in CRASH state"));
        }
        if (event != null) {
            if (event.getEventAction() == Event.EventAction.NODE_ERROR) {
                return;
            }
        }
        try {
            final AppManagerEvent appManagerEvent = new AppManagerEvent(flowId, Event.EventAction.SOFT_ERROR, this.getExceptionEvent(flowId, e));
            final ArrayList<UUID> managedNodesLocal = new ArrayList<UUID>((Collection<? extends UUID>)this.managedNodes);
            ApplicationStateMachine.runStateFlow(Event.EventAction.SOFT_ERROR, appContext, appManagerEvent, managedNodesLocal);
        }
        catch (Exception e2) {
            AppManagerWorker.logger.warn(("Failed to put flow " + this.getFlowName(flowId) + " in CRASH state with exception " + e.getMessage()));
        }
    }
    
    private void bringAppToDesiredStateLocal(final UUID appID, final AppContext appContext) throws Exception {
        final AppManagerEvent appManagerEvent = new AppManagerEvent(appID, Event.EventAction.SOFT_DEPLOY);
        this.runFlow(appManagerEvent, appID, Event.EventAction.SOFT_DEPLOY);
    }
    
    private void sendCompletionNotification(final UUID requestId, final ActionType newEventAction, final String exception) {
        if (requestId != null) {
            if (AppManagerWorker.logger.isInfoEnabled()) {
                AppManagerWorker.logger.info(("Sending completion notification for executing action " + newEventAction + " for requestId " + requestId));
            }
            final ITopic<ChangeApplicationStateResponse> appManagerResponseTopic = (ITopic<ChangeApplicationStateResponse>)HazelcastSingleton.get().getTopic("#AppManagerResponse");
            if (exception != null) {
                final ChangeApplicationStateResponse response = new ChangeApplicationStateResponse(requestId, ChangeApplicationStateResponse.RESULT.FAILURE, exception);
                appManagerResponseTopic.publish(response);
                if (AppManagerWorker.logger.isInfoEnabled()) {
                    AppManagerWorker.logger.info(("Sending failure for executing action " + newEventAction + " for requestId " + requestId + " with exception " + exception));
                }
            }
            else {
                final ChangeApplicationStateResponse response = new ChangeApplicationStateResponse(requestId, ChangeApplicationStateResponse.RESULT.SUCCESS, null);
                appManagerResponseTopic.publish(response);
                if (AppManagerWorker.logger.isInfoEnabled()) {
                    AppManagerWorker.logger.info(("Sending success for executing action " + newEventAction + " for requestId " + requestId));
                }
            }
        }
    }
    
    private void sendStatusNotification(final UUID requestId, final UUID flowId) {
        final AppContext appContext = (AppContext)this.managedApplications.get(flowId);
        final ITopic<ChangeApplicationStateResponse> appManagerResponseTopic = HazelcastSingleton.get().getTopic("#AppManagerResponse");
        try {
            if (AppManagerWorker.logger.isInfoEnabled()) {
                AppManagerWorker.logger.info(("Sending completion notification for executing action status for requestId " + requestId));
            }
            if (appContext == null) {
                final MetaInfo.Flow flow = FlowUtil.getFlow(flowId);
                if (flow == null) {
                    throw new Exception("Invalid flowId  " + flowId);
                }
                final ApplicationStatusResponse response = new ApplicationStatusResponse(requestId, ChangeApplicationStateResponse.RESULT.SUCCESS, null, MetaInfo.StatusInfo.Status.CREATED, null);
                appManagerResponseTopic.publish(response);
            }
            else {
                final ApplicationStatusResponse response2 = new ApplicationStatusResponse(requestId, ChangeApplicationStateResponse.RESULT.SUCCESS, null, appContext.getCurrentStatus(), appContext.getExceptionEvents());
                appManagerResponseTopic.publish(response2);
            }
        }
        catch (Throwable e) {
            final ApplicationStatusResponse response = new ApplicationStatusResponse(requestId, ChangeApplicationStateResponse.RESULT.FAILURE, e.getMessage(), null, null);
            appManagerResponseTopic.publish(response);
        }
    }
    
    public boolean isLeader() {
        return this.hasLock;
    }
    
    public void loadApplicationsToManage() throws Exception {
        final MDRepository md = this.metadataRepository;
        if (md == null) {
            return;
        }
        final Set<MetaInfo.Namespace> namespaces = (Set<MetaInfo.Namespace>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.NAMESPACE, WASecurityManager.TOKEN);
        for (final MetaInfo.Namespace app : namespaces) {
            if (app.name.equalsIgnoreCase("Global")) {
                this.loadApplicationForNamespace(app);
            }
        }
        for (final MetaInfo.Namespace app : namespaces) {
            if (!app.name.equalsIgnoreCase("Global")) {
                this.loadApplicationForNamespace(app);
            }
        }
    }
    
    private void loadApplicationForNamespace(final MetaInfo.Namespace app) throws MetaDataRepositoryException {
        if (AppManagerWorker.logger.isInfoEnabled()) {
            AppManagerWorker.logger.info(("Loading objects for namespace: " + app.name));
        }
        final Set<MetaInfo.MetaObject> objs = MetadataRepository.getINSTANCE().getByNameSpace(app.name, WASecurityManager.TOKEN);
        if (objs != null) {
            for (final MetaInfo.MetaObject obj : objs) {
                if (obj.type == EntityType.APPLICATION) {
                    if (DesiredStateManager.getDesiredAppStatus(obj.getUuid()) != MetaInfo.StatusInfo.Status.CREATED) {
                        this.managedApplications.put(obj.getUuid(), new AppContext(obj.getUuid()));
                    }
                    else {
                        FlowUtil.updateFlowActualStatus(obj.getUuid(), MetaInfo.StatusInfo.Status.CREATED);
                    }
                }
            }
        }
    }
    
    @Override
    public void call(final Event event) {
        try {
            if (event instanceof ApiCallEvent) {
                this.handleApiEvent((ApiCallEvent)event);
            }
            else if (event instanceof NodeEvent) {
                this.handleNodeEvent((NodeEvent)event);
            }
            else if (event instanceof MembershipEvent) {
                this.handleMembershipEvent((MembershipEvent)event);
            }
            else {
                if (!(event instanceof CommandConfirmationEvent)) {
                    throw new RuntimeException("App Manager worker received an unsupported event type: " + event);
                }
                this.handleCommandConfirmationEvent((CommandConfirmationEvent)event);
            }
        }
        catch (Exception e) {
            AppManagerWorker.logger.error(("got exception " + e.getMessage() + " in AppManager "));
        }
    }
    
    public void scheduleCheckpoint(final UUID flowId) {
        this.checkpointManager.add(flowId, 0L);
    }
    
    public void scheduleCheckpoint(final UUID flowId, final long delayAdvance) {
        this.checkpointManager.add(flowId, delayAdvance);
    }
    
    public void unscheduleCheckpoint(final UUID flowId) {
        this.checkpointManager.remove(flowId);
    }
    
    static {
        AppManagerWorker.logger = Logger.getLogger((Class)AppManagerWorker.class);
    }
    
    private class CheckpointScheduler
    {
        private ScheduledExecutorService appCheckpointExecutor;
        private final Map<UUID, ScheduledFuture> futures;
        
        public CheckpointScheduler() {
            this.appCheckpointExecutor = new ScheduledThreadPoolExecutor(1);
            this.futures = new HashMap<UUID, ScheduledFuture>();
        }
        
        public synchronized void add(final UUID flowId, final long delayAdvance) {
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                AppManagerWorker.logger;
                Logger.getLogger("Recovery").debug(("Checkpoint Manager adding future for app " + flowId + " @" + System.currentTimeMillis()));
            }
            MetaInfo.Flow flowInfo = null;
            try {
                flowInfo = FlowUtil.getFlow(flowId);
            }
            catch (MetaDataRepositoryException e) {
                throw new Warning("Checkpoint Manager ignoring app " + flowInfo.name + " because the application is unavailable or does not exist for flowId " + flowId);
            }
            if (flowInfo.recoveryType == 0) {
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    AppManagerWorker.logger;
                    Logger.getLogger("Recovery").debug(("Checkpoint Manager ignoring app " + flowInfo.name + " because recovery is not specified"));
                }
                return;
            }
            if (flowInfo.recoveryPeriod <= 0L) {
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    AppManagerWorker.logger;
                    Logger.getLogger("Recovery").debug(("Checkpoint Manager ignoring app " + flowInfo.name + " because even though recovery is specified, the recovery period is " + flowInfo.recoveryPeriod + " which is not allowed"));
                }
                return;
            }
            final Runnable appCheckpointRunnable = new Runnable() {
                @Override
                public void run() {
                    UUID appManagerId = null;
                    int attemptCount = 0;
                    while (attemptCount < 20) {
                        appManagerId = AppManagerServerLocation.getAppManagerServerId();
                        if (appManagerId != null) {
                            break;
                        }
                        try {
                            Thread.sleep(200L);
                            ++attemptCount;
                            continue;
                        }
                        catch (InterruptedException e2) {}
                        break;
                    }
                    if (appManagerId == null) {
                        throw new Warning("Checkpoint Manager cannot take actio because no AppManager found in the cluster");
                    }
                    try {
                        if (Logger.getLogger("Recovery").isDebugEnabled()) {
                            AppManagerWorker.logger;
                            Logger.getLogger("Recovery").debug(("Checkpoint Manager initiating a CHECKPOINT for " + flowId));
                        }
                        final Map<String, Object> params = new HashMap<String, Object>();
                        final UUID requestId = new UUID(System.currentTimeMillis());
                        Server.server.getAppManager().changeApplicationState(ActionType.CHECKPOINT, flowId, params, requestId);
                    }
                    catch (Exception e) {
                        Logger.getLogger("Recovery").warn(("Checkpoint Manager  failed to initiate checkpoint for " + flowId + ": " + e.getMessage()));
                    }
                }
            };
            final long delay = flowInfo.recoveryPeriod - delayAdvance;
            final ScheduledFuture<?> future = this.appCheckpointExecutor.schedule(appCheckpointRunnable, delay, TimeUnit.SECONDS);
            this.futures.put(flowId, future);
            Logger.getLogger("Recovery").debug(("Checkpoint Manager DONE ADDING future for app " + flowId + ".@" + System.currentTimeMillis()));
        }
        
        public synchronized void remove(final UUID flowId) {
            if (!this.futures.containsKey(flowId)) {
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug(("Ignoring request to stop managing checkpoints for flow " + flowId + " because no checkpoint process exists. @" + System.currentTimeMillis()));
                }
                return;
            }
            final boolean canceled = this.futures.remove(flowId).cancel(false);
            if (!canceled) {
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug(("Attempted to stop managing checkpoints for flow " + flowId + " but cancellation failed."));
                }
                return;
            }
            if (Logger.getLogger("Recovery").isDebugEnabled()) {
                Logger.getLogger("Recovery").debug(("Cancelled future for flow " + flowId));
            }
        }
    }
}
