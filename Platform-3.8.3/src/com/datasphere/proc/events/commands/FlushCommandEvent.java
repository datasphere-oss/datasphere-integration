package com.datasphere.proc.events.commands;

import com.datasphere.uuid.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.components.*;
import org.apache.log4j.*;

import com.datasphere.runtime.containers.*;

public class FlushCommandEvent extends CommandEvent
{
    public FlushCommandEvent(final UUID appUuid, final long commandTimestamp) {
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
        if (!component.getTopLevelFlow().getNodeManager().isWaitingOn(Event.EventAction.NODE_APP_QUIESCE_FLUSHED, this.commandTimestamp, component.getMetaID())) {
            if (Logger.getLogger("Commands").isInfoEnabled()) {
                Logger.getLogger("Commands").info((Object)("Unexpected cannot flush " + component.getMetaName() + " for command timestamp " + this.commandTimestamp + " because the node is not waiting on that!"));
            }
            return false;
        }
        if (CommandEvent.shouldWait(component.getMetaID().toString(), component.getInDegree(), component, this)) {
            if (Logger.getLogger("Commands").isInfoEnabled()) {
                Logger.getLogger("Commands").info((Object)("Waiting for more inputs before flushing component " + component.getMetaName()));
            }
            return false;
        }
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("Flushing component " + component.getMetaName()));
        }
        final UUID componentAppUuid = component.getTopLevelFlow().getMetaID();
        if (componentAppUuid != null && !componentAppUuid.equals((Object)this.appUuid)) {
            Logger.getLogger("Commands").info((Object)("No flushing " + component.getMetaName() + " because it is not in the same application as the quiesce command (" + this.appUuid + ")"));
            return false;
        }
        component.flush();
        component.getTopLevelFlow().getNodeManager().notifyComponentProcessedCommand(this.commandTimestamp, component.getMetaID(), Event.EventAction.NODE_APP_QUIESCE_FLUSHED);
        component.publish((ITaskEvent)this);
        return true;
    }
    
    @Override
    public boolean performCommandForStream(final FlowComponent component, final long linkUuid) throws Exception {
        if (CommandEvent.shouldWait(component.getMetaID().toString() + "-" + new Long(linkUuid).toString(), component.getInDegree(), component, this)) {
            if (Logger.getLogger("Commands").isInfoEnabled()) {
                Logger.getLogger("Commands").info((Object)("Waiting for more inputs before flushing [stream] component " + component.getMetaName()));
            }
            return false;
        }
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("Flushing [stream] component " + component.getMetaName()));
        }
        final UUID componentAppUuid = component.getTopLevelFlow().getMetaID();
        if (componentAppUuid != null && !componentAppUuid.equals((Object)this.appUuid)) {
            Logger.getLogger("Commands").info((Object)("FlushCommandEvent [stream] for " + this.appUuid + " cannot be applied to component app " + componentAppUuid));
            return false;
        }
        component.flush();
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("<<< [stream] Done flushing component " + component.getMetaName()));
        }
        return true;
    }
}
