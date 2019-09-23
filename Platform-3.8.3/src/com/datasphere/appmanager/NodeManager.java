package com.datasphere.appmanager;

import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.event.Event;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.WaitUtility;
import com.datasphere.uuid.UUID;

public class NodeManager
{
    private static Logger logger;
    private final Flow application;
    private final EventQueueManager eventQueueManager;
    private final UUID serverId;
    public Long epochNumber;
    private boolean error;
    private final UUID flowId;
    private final PathManager pendingCheckpoint;
    WaitUtility waitUtility;
    
    public NodeManager(final Flow flow, final BaseServer srv) throws Exception {
        this.pendingCheckpoint = new PathManager();
        this.waitUtility = new WaitUtility();
        this.application = flow;
        this.flowId = this.application.getMetaID();
        NodeManager.logger.info((Object)("NodeManager starting for application " + this.application.getMetaName()));
        this.serverId = srv.getServerID();
        this.eventQueueManager = EventQueueManager.get();
        this.registerAppManager();
        this.epochNumber = null;
        this.error = false;
    }
    
    private void registerAppManager() {
    }
    
    public synchronized void close() {
        NodeManager.logger.info((Object)("AppManager Stopping for application " + this.application.getMetaName()));
    }
    
    public synchronized void flowDeployed(final Long epochNumber) {
        this.eventQueueManager.sendNodeEvent(this.serverId, Event.EventAction.NODE_CACHE_DEPLOYED, this.flowId);
        this.epochNumber = epochNumber;
        this.error = false;
        NodeManager.logger.info((Object)("Sent NODE_CACHE_DEPLOYED for flow " + this.application.getMetaFullName() + ". New Epoch Number " + epochNumber));
    }
    
    public synchronized void flowStarted(final Long epochNumber) {
        this.eventQueueManager.sendNodeEvent(this.serverId, Event.EventAction.NODE_RUNNING, this.flowId);
        this.epochNumber = epochNumber;
        this.error = false;
        NodeManager.logger.info((Object)("Sent NODE_RUNNING for flow " + this.application.getMetaFullName() + ". New Epoch Number " + epochNumber));
    }
    
    public synchronized void flowStopped(final Long epochNumber) {
        this.epochNumber = epochNumber;
        this.error = false;
        NodeManager.logger.info((Object)("Sent NODE_APP_STOPPED (STOPPED) for flow " + this.application.getMetaFullName() + ". New Epoch Number " + epochNumber));
    }
    
    public synchronized void notifyNodeProcessedCommand(final Event.EventAction eventAction, final Long commandTimestamp) {
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("Node has finished processing command " + eventAction + " at time " + commandTimestamp));
        }
        if (eventAction == Event.EventAction.NODE_APP_CHECKPOINTED || eventAction == Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED) {
            final Position p = this.pendingCheckpoint.toPosition();
            StatusDataStore.getInstance().putPendingAppCheckpoint(this.flowId, p, commandTimestamp);
            this.pendingCheckpoint.clear();
        }
        this.eventQueueManager.sendCommandConfirmationEvent(this.serverId, eventAction, this.flowId, commandTimestamp);
        this.error = false;
    }
    
    public synchronized void recvExceptionEvent(final ExceptionEvent ee) {
        NodeManager.logger.warn((Object)("Exception event is received from component " + ee.getEntityType().name() + " - " + ee));
        ee.setEpochNumber(this.epochNumber);
        if (ee.getAction().equals(ActionType.CRASH) || ee.getAction().equals(ActionType.STOP)) {
            this.eventQueueManager.sendNodeErrorEvent(this.serverId, Event.EventAction.NODE_ERROR, ee, this.flowId);
            this.error = true;
        }
    }
    
    public synchronized void recvStopSignal(final UUID appUUID, final EntityType entityType, final String entityName) {
        NodeManager.logger.info((Object)("Receiving Application Stop Signal from " + entityType.name() + " - " + entityName + " for app UUID: " + appUUID));
        this.eventQueueManager.sendNodeEvent(this.serverId, Event.EventAction.API_STOP, this.flowId);
    }
    
    public boolean isError() {
        return this.error;
    }
    
    public synchronized void waitForCommand(final Long commandTimestamp, final Set<UUID> waitSet, final Event.EventAction commandType) {
        if ((commandType == Event.EventAction.NODE_APP_CHECKPOINTED || commandType == Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED) && !this.pendingCheckpoint.isEmpty()) {
            if (Logger.getLogger("Commands").isInfoEnabled()) {
                Logger.getLogger("Commands").info((Object)("Application " + this.application.getMetaName() + " is clearing old checkpoint state in preparation of a new checkpoint (this is not a problem, it probably occured after a crash and redeployment)"));
            }
            this.pendingCheckpoint.clear();
        }
        this.waitUtility.startWaiting(commandType, commandTimestamp, waitSet);
        if (NodeManager.logger.isDebugEnabled()) {
            NodeManager.logger.debug((Object)("NodeManager WAITING on " + waitSet.size() + " components to " + commandType + ":"));
            for (final UUID u : waitSet) {
                try {
                    final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(u, HSecurityManager.TOKEN);
                    NodeManager.logger.debug((Object)("   * " + u + " ~ " + mo.getName()));
                }
                catch (MetaDataRepositoryException ex) {}
            }
        }
    }
    
    public synchronized void notifyComponentProcessedCommand(final long commandTimestamp, final UUID compUuid, final Event.EventAction commandType) {
        if (!this.isWaitingOn(commandType, commandTimestamp, compUuid)) {
            if (Logger.getLogger("Commands").isInfoEnabled()) {
                Logger.getLogger("Commands").info((Object)("Unexpected cannot process " + commandType + " at time " + commandTimestamp + " on component " + compUuid + " because this node is not waiting on that!"));
            }
            return;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            try {
                final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(compUuid, HSecurityManager.TOKEN);
                Logger.getLogger("Commands").debug((Object)("NodeManager notified of expected " + commandType + " processed by expected component " + mo.getName()));
            }
            catch (MetaDataRepositoryException e) {
                Logger.getLogger("Commands").debug((Object)("[Error printing information] NodeManager notified of expected " + commandType + " processed by expected component " + compUuid));
            }
        }
        final int stillWaitingCount = this.waitUtility.stopWaitingOn(commandType, commandTimestamp, compUuid);
        if (stillWaitingCount == 0) {
            this.notifyNodeProcessedCommand(commandType, commandTimestamp);
        }
    }
    
    public void stashPendingCheckpoint(final Position checkpoint) {
        this.pendingCheckpoint.mergeLowerPositions(checkpoint);
    }
    
    public boolean isWaitingOn(final Event.EventAction commandType, final long commandTimestamp, final UUID metaID) {
        return this.waitUtility.isWaitingOn(commandType, commandTimestamp, metaID);
    }
    
    public boolean isWaitingOn(final long commandTimestamp) {
        return this.waitUtility.isWaitingOn(commandTimestamp);
    }
    
    public void clearWaiting() {
        this.waitUtility.clear();
    }
    
    static {
        NodeManager.logger = Logger.getLogger((Class)NodeManager.class);
    }
}
