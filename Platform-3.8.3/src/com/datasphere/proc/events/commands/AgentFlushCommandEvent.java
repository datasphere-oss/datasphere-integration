package com.datasphere.proc.events.commands;

import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.appmanager.EventQueueManager;
import com.datasphere.appmanager.event.Event;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.recovery.PathManager;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.uuid.UUID;

public class AgentFlushCommandEvent extends FlushCommandEvent
{
    protected UUID agentUuid;
    private PathManager pendingCheckpoint;
    private Set<UUID> agentComponentsExpected;
    
    public AgentFlushCommandEvent(final UUID agentUuid, final UUID appUuid, final long commandTimestamp, final Set<UUID> agentComponentsExpected) {
        super(appUuid, commandTimestamp);
        this.pendingCheckpoint = new PathManager();
        this.agentUuid = agentUuid;
        this.agentComponentsExpected = agentComponentsExpected;
    }
    
    @Override
    public boolean performCommand(final FlowComponent component) throws Exception {
        if (BaseServer.baseServer.isServer()) {
            if (Logger.getLogger("Commands").isTraceEnabled()) {
                Logger.getLogger("Commands").trace((Object)("Processing agent flush for component, on a Server: " + component.getMetaName() + "..."));
            }
            return super.performCommand(component);
        }
        if (!this.agentComponentsExpected.remove(component.getMetaID())) {
            Logger.getLogger("Commands").warn((Object)("Agent will not perform flush command on " + component.getMetaName() + " because it is not in the list of expected agent components"));
            return false;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Taking checkpoint on an agent for component " + component.getMetaName() + "..."));
        }
        component.flush();
        if (this.agentComponentsExpected.isEmpty()) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)"Agent checkpoint complete, writing to pending table.");
            }
            StatusDataStore.getInstance().putPendingAppCheckpoint(this.appUuid, this.pendingCheckpoint.toPosition(), this.commandTimestamp);
            EventQueueManager.get().sendCommandConfirmationEvent(this.agentUuid, Event.EventAction.NODE_APP_QUIESCE_FLUSHED, this.appUuid, this.commandTimestamp);
            this.pendingCheckpoint = null;
        }
        component.publish((ITaskEvent)this);
        return true;
    }
    
    @Override
    public boolean performCommandForStream(final FlowComponent component, final long linkUuid) throws Exception {
        if (BaseServer.baseServer.isServer()) {
            if (Logger.getLogger("Commands").isTraceEnabled()) {
                Logger.getLogger("Commands").trace((Object)("Checkpoint command from Agent now processing on a Server: " + component.getMetaName() + "..."));
            }
            return super.performCommandForStream(component, linkUuid);
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Agent (stream) taking checkpoint for component " + component.getMetaName() + "..."));
        }
        component.flush();
        return true;
    }
}
