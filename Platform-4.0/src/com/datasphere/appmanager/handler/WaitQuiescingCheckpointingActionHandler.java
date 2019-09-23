package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.*;
import com.datasphere.persistence.*;

public class WaitQuiescingCheckpointingActionHandler implements ActionHandler
{
    private static Logger logger;
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("App Manager is handling a checkpoint confirmation: " + event));
        }
        final CommandConfirmationEvent commandConfirmationEvent = (CommandConfirmationEvent)event;
        final int waitCount = appContext.removedAndCheckWaitStatusForCommandEvents(commandConfirmationEvent.getEventAction(), commandConfirmationEvent.getCommandTimestamp(), commandConfirmationEvent.getServerId());
        if (waitCount > 0) {
            if (Logger.getLogger("Commands").isDebugEnabled()) {
                Logger.getLogger("Commands").debug((Object)("App Manager handled " + event.getEventAction() + " but still waiting on " + waitCount + " more nodes"));
            }
            return appContext.getCurrentStatus();
        }
        final boolean success = StatusDataStore.getInstance().promotePendingAppCheckpoint(appContext.getAppId(), appContext.getFlow().getUri(), commandConfirmationEvent.getCommandTimestamp());
        if (!success) {
            WaitQuiescingCheckpointingActionHandler.logger.warn((Object)("Recorded an entire app checkpoint for " + appContext.getAppName() + " but failed to promote it to a full checkpoint!"));
        }
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("!! App Manager will now stop having finalized the quiescent checkpoint: " + event));
        }
        appContext.execute(ActionType.STOP, true);
        appContext.execute(ActionType.START_CACHES, true);
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("!! App Manager completed QUIESCE: " + event));
        }
        appContext.updateDesiredStatus(MetaInfo.StatusInfo.Status.QUIESCED);
        return MetaInfo.StatusInfo.Status.QUIESCED;
    }
    
    static {
        WaitQuiescingCheckpointingActionHandler.logger = Logger.getLogger((Class)AppCheckpointPersistenceLayer.class);
    }
}
