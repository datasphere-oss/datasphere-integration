package com.datasphere.proc.events.commands;

import com.datasphere.uuid.*;
import com.datasphere.runtime.components.*;
import org.apache.log4j.*;

import com.datasphere.runtime.containers.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.recovery.*;

public class CheckpointCommandEvent extends CommandEvent
{
    public CheckpointCommandEvent(final UUID appUuid, final long commandTimestamp) {
        super(appUuid, commandTimestamp);
    }
    
    @Override
    public boolean performCommand(final FlowComponent component) throws Exception {
        if (!component.getTopLevelFlow().getNodeManager().isWaitingOn(this.commandTimestamp)) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)("Dropping apparently anachronistic Command for time " + component + " at component " + component.getMetaName()));
            }
            return false;
        }
        if (!component.getTopLevelFlow().getNodeManager().isWaitingOn(Event.EventAction.NODE_APP_CHECKPOINTED, this.commandTimestamp, component.getMetaID())) {
            throw new Exception("Unexpected cannot checkpoint " + component.getMetaName() + " at time " + this.commandTimestamp + " because the node is not waiting on that!");
        }
        final UUID componentAppUuid = component.getTopLevelFlow().getMetaID();
        if (componentAppUuid != null && !componentAppUuid.equals((Object)this.appUuid)) {
            return false;
        }
        if (CommandEvent.shouldWait(component.getMetaID().toString(), component.getInDegree(), component, this)) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)("Waiting for other inputs before checkpointing component " + component.getMetaName()));
            }
            return false;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Not waiting on checkpointing component " + component.getMetaName()));
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Taking checkpoint for component " + component.getMetaName() + "..."));
        }
        component.getTopLevelFlow().getNodeManager().stashPendingCheckpoint(component.getCheckpoint());
        component.getTopLevelFlow().getNodeManager().notifyComponentProcessedCommand(this.commandTimestamp, component.getMetaID(), Event.EventAction.NODE_APP_CHECKPOINTED);
        component.publish((ITaskEvent)this);
        return true;
    }
    
    @Override
    public boolean performCommandForStream(final FlowComponent component, final long linkUuid) throws Exception {
        final UUID componentAppUuid = component.getTopLevelFlow().getMetaID();
        if (componentAppUuid != null && !componentAppUuid.equals((Object)this.appUuid)) {
            Logger.getLogger("Commands").info((Object)("CheckpointCommandEvent for (stream) " + this.appUuid + " cannot be applied to component app " + componentAppUuid));
            return false;
        }
        final String metaId = component.getMetaID().toString() + "-" + linkUuid;
        if (CommandEvent.shouldWait(metaId, component.getInDegree(), component, this)) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)("Waiting (stream) before checkpointing component " + component.getMetaName() + "/" + metaId + "..."));
            }
            return false;
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("Taking checkpoint for (stream) component " + component.getMetaName() + "/" + metaId + "..."));
        }
        final Position componentCheckpoint = component.getCheckpoint();
        component.getTopLevelFlow().getNodeManager().stashPendingCheckpoint(componentCheckpoint);
        return true;
    }
}
