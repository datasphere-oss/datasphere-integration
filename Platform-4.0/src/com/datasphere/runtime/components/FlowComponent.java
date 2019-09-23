package com.datasphere.runtime.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.DisapproveQuiesceException;
import com.datasphere.appmanager.NodeManager;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.ReportStats;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public abstract class FlowComponent extends MonitorableComponent implements IFlowComponent
{
    private static Logger logger;
    private final BaseServer srv;
    private final MetaInfo.MetaObject info;
    protected volatile boolean paused;
    private Flow flow;
    private NodeManager nodeManager;
    public final Queue<CommandEvent> injectedCommandEvents;
    public HashMap<AuthToken, ReportStats.BaseReportStats> compStats;
    public Publisher dataSource;
    protected int inDegreeCluster;
    protected int inDegreeNode;
    Boolean recoveryEnabled;
    List<AuthToken> authTokenList;
    
    public void addSessionToReport(final AuthToken token) {
    }
    
    public ReportStats.BaseReportStats removeSessionToReport(final AuthToken token, final boolean computeRetVal) {
        return null;
    }
    
    public FlowComponent(final BaseServer srv, final MetaInfo.MetaObject info) {
        this.paused = false;
        this.injectedCommandEvents = new ConcurrentLinkedQueue<CommandEvent>();
        this.compStats = new LinkedHashMap<AuthToken, ReportStats.BaseReportStats>();
        this.inDegreeCluster = -88;
        this.inDegreeNode = -99;
        this.recoveryEnabled = null;
        this.authTokenList = new ArrayList<AuthToken>();
        this.srv = srv;
        this.info = info;
    }
    
    @Override
    public void setFlow(final Flow f) {
        this.flow = f;
        if (f != null) {
            this.inDegreeNode = f.getTopLevelFlow().getInDegrees(this.getMetaID());
        }
    }
    
    @Override
    public Flow getFlow() {
        return this.flow;
    }
    
    @Override
    public Flow getTopLevelFlow() {
        if (this.flow == null) {
            return null;
        }
        if (this.flow.getFlow() == null) {
            return this.flow;
        }
        return this.flow.getTopLevelFlow();
    }
    
    @Override
    public boolean recoveryIsEnabled() {
        if (this.recoveryEnabled == null) {
            final Flow f = this.getTopLevelFlow();
            this.recoveryEnabled = (f != null && f.recoveryIsEnabled());
        }
        return this.recoveryEnabled;
    }
    
    public boolean isPaused() {
        return this.paused;
    }
    
    public void setPaused(final boolean paused) {
        this.paused = paused;
    }
    
    public boolean approveQuiesce() throws DisapproveQuiesceException {
        return true;
    }
    
    public boolean isFlowInError() {
        if (this.nodeManager == null) {
            final Flow topLevelflow = this.getTopLevelFlow();
            if (topLevelflow == null) {
                return false;
            }
            this.nodeManager = topLevelflow.getNodeManager();
            if (this.nodeManager == null) {
                return false;
            }
        }
        return this.nodeManager.isError();
    }
    
    @Deprecated
    public abstract void close() throws Exception;
    
    public abstract void stop() throws Exception;
    
    public void flush() throws Exception {
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)(this.getMetaName() + " does not support flushing; doing nothing."));
        }
    }
    
    public void publish(final ITaskEvent event) throws Exception {
        FlowComponent.logger.debug((Object)(this.getMetaName() + " does not support publishing; doing nothing."));
    }
    
    public void injectCommandEvent(final CommandEvent commandEvent) throws Exception {
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)(this.getMetaName() + " is injecting " + commandEvent.getClass() + "  "));
        }
        commandEvent.performCommand(this);
    }
    
    public int getInDegree() {
        return this.inDegreeNode;
    }
    
    public MetaInfo.MetaObject getMetaInfo() {
        return this.info;
    }
    
    public MetaInfo.MetaObjectInfo getMetaObjectInfo() {
        return this.getMetaInfo().makeMetaObjectInfo();
    }
    
    public EntityType getMetaType() {
        return this.getMetaInfo().getType();
    }
    
    @Override
    public UUID getMetaID() {
        return this.getMetaInfo().getUuid();
    }
    
    public String getMetaName() {
        return this.getMetaInfo().getName();
    }
    
    public String getMetaNsName() {
        return this.getMetaInfo().getNsName();
    }
    
    public String getMetaFullName() {
        return this.getMetaInfo().getFullName();
    }
    
    public String getMetaUri() {
        return this.getMetaInfo().getUri();
    }
    
    public List<UUID> getMetaDependencies() {
        return this.getMetaInfo().getDependencies();
    }
    
    public String metaToString() {
        return this.getMetaInfo().metaToString();
    }
    
    public Position getCheckpoint() {
        return null;
    }
    
    @Override
    public BaseServer srv() {
        return this.srv;
    }
    
    public void notifyAppMgr(final EntityType entityType, final String entityName, final UUID entityId, final Exception exception, final String relatedActivity, final Object... relatedObjects) {
        FlowComponent.logger.warn((Object)("received exception from :" + entityType + ", of exception type : " + exception.getClass().getCanonicalName()));
        final ExceptionEvent ee = new ExceptionEvent();
        final Flow topLevelFlow = this.getTopLevelFlow();
        if (topLevelFlow == null) {
            return;
        }
        final MetaInfo.Flow app = (MetaInfo.Flow)topLevelFlow.getMetaInfo();
        ee.setAppid(app.uuid);
        ee.setType(ExceptionType.getExceptionType(exception));
        ee.setEntityType(entityType);
        ee.setClassName(exception.getClass().getName());
        ee.setMessage(exception.getMessage());
        ee.entityName = entityName;
        ee.entityId = entityId;
        ee.relatedActivity = relatedActivity;
        ee.setRelatedObjects(relatedObjects);
        ee.setAction(this.getUserRequestedActionForException(exception, ee.getType(), app.getEhandlers()));
        if (FlowComponent.logger.isInfoEnabled()) {
            FlowComponent.logger.info((Object)("exception event created :" + ee));
        }
        this.publishException(ee);
        if (this.getFlow().getNodeManager() != null) {
            this.getFlow().getNodeManager().recvExceptionEvent(ee);
        }
        else if (this.getFlow().getNodeManager() != null) {
            this.getFlow().getNodeManager().recvExceptionEvent(ee);
        }
        else if (this.getTopLevelFlow().getNodeManager() != null) {
            this.getTopLevelFlow().getNodeManager().recvExceptionEvent(ee);
        }
        else {
            FlowComponent.logger.warn((Object)"Failed to get app manager, so NOT notifying exception. ");
        }
    }
    
    public void notifyAppMgrForStopApplication(final EntityType entityType, final String entityName, final UUID entityId, final String relatedActivity, final Object... relatedObjects) {
        final Flow topLevelFlow = this.getTopLevelFlow();
        if (topLevelFlow == null) {
            return;
        }
        final MetaInfo.Flow app = (MetaInfo.Flow)topLevelFlow.getMetaInfo();
        final UUID appUUID = app.uuid;
        if (this.getFlow().getNodeManager() != null) {
            this.getFlow().getNodeManager().recvStopSignal(appUUID, entityType, entityName);
        }
        else if (this.getTopLevelFlow().getNodeManager() != null) {
            this.getTopLevelFlow().getNodeManager().recvStopSignal(appUUID, entityType, entityName);
        }
        else {
            FlowComponent.logger.warn((Object)"Failed to get app manager, so NOT stopping application. ");
        }
    }
    
    public ActionType getUserRequestedActionForException(final Throwable ex, final ExceptionType eType, final Map<String, Object> ehandlers) {
        if (ehandlers == null || ehandlers.isEmpty()) {
            return this.getDefaultAction(ex);
        }
        for (final String exceptionType : ehandlers.keySet()) {
            if (eType.name().equalsIgnoreCase(exceptionType)) {
                if (((String)ehandlers.get(exceptionType)).equalsIgnoreCase("stop")) {
                    return ActionType.STOP;
                }
                if (((String)ehandlers.get(exceptionType)).equalsIgnoreCase("crash")) {
                    return ActionType.CRASH;
                }
                return ActionType.IGNORE;
            }
        }
        return this.getDefaultAction(ex);
    }
    
    private ActionType getDefaultAction(final Throwable ex) {
        return ActionType.CRASH;
    }
    
    public boolean startReportCollection(final AuthToken clToken) {
        if (this.authTokenList.contains(clToken)) {
            return false;
        }
        this.authTokenList.add(clToken);
        return true;
    }
    
    protected void publishException(final ExceptionEvent event) {
        try {
            if (FlowComponent.logger.isDebugEnabled()) {
                FlowComponent.logger.debug((Object)"publishing exception to exceptionStream.");
            }
            final Stream exceptionStream = this.srv.getExceptionStream();
            if (exceptionStream != null) {
                final Channel channel = exceptionStream.getChannel();
                if (channel != null) {
                    if (FlowComponent.logger.isDebugEnabled()) {
                        FlowComponent.logger.debug((Object)("channel name :" + channel.getSubscribersCount() + ", channel:" + channel));
                    }
                    final List<DARecord> jsonBatch = new ArrayList<DARecord>();
                    jsonBatch.add(new DARecord((Object)event));
                    channel.publish((ITaskEvent)TaskEvent.createStreamEvent(jsonBatch));
                    FlowComponent.logger.warn((Object)("channel to publish exceptions is not null and published to channel : " + channel.getSubscribersCount()));
                }
                else {
                    FlowComponent.logger.warn((Object)"channel to publish exceptions is null.");
                }
            }
        }
        catch (Exception ex) {
            FlowComponent.logger.error((Object)"Problem publishing exception event", (Throwable)ex);
        }
    }
    
    public void recordLagMarker(final LagMarker lagMarker) {
        lagMarker.recordLag(this.getMetaFullName() + "#" + this.getMetaInfo().getType().name() + "#" + Server.server.ServerInfo.getName(), HazelcastSingleton.get().getCluster().getClusterTime());
    }
    
    public void lagRecordInfo(final MetaInfo.MetaObject metaInfo, final ITaskEvent event) {
        this.lagRecordInfo(metaInfo, ((TaskEvent)event).getLagMarker());
    }
    
    public void lagRecordInfo(final MetaInfo.MetaObject metaInfo, final LagMarker lagMarker) {
        if (FlowComponent.logger.isDebugEnabled()) {
            FlowComponent.logger.debug((Object)("Lag record received for the " + metaInfo.getFullName() + " \n " + lagMarker));
        }
    }
    
    public boolean shouldMarkerBePassedAlong(final ITaskEvent event) {
        if (!event.isLagRecord()) {
            return false;
        }
        final TaskEvent taskEvent = (TaskEvent)event;
        final LagMarker lagMarker = taskEvent.getLagMarker();
        return this.lagMarkerWithinTheCorrectApp(lagMarker);
    }
    
    private boolean lagMarkerWithinTheCorrectApp(final LagMarker lagMarker) {
        return lagMarker != null && lagMarker.getApplicationUUID().equals((Object)this.getTopLevelFlow().getMetaID());
    }
    
    static {
        FlowComponent.logger = Logger.getLogger((Class)FlowComponent.class);
    }
}
