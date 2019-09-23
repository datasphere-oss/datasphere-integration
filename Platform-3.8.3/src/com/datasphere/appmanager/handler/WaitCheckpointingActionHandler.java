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

public class WaitCheckpointingActionHandler implements ActionHandler
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
        if (appContext.getCurrentStatus() != MetaInfo.StatusInfo.Status.RUNNING) {
            WaitCheckpointingActionHandler.logger.info((Object)("Cannot finalize checkpoint for " + appContext.getAppName() + "@" + commandConfirmationEvent.getCommandTimestamp() + " because app must be RUNNING but is " + appContext.getCurrentStatus()));
            return appContext.getCurrentStatus();
        }
        final boolean success = StatusDataStore.getInstance().promotePendingAppCheckpoint(appContext.getAppId(), appContext.getFlow().getUri(), commandConfirmationEvent.getCommandTimestamp());
        if (!success) {
            WaitCheckpointingActionHandler.logger.warn((Object)("Recorded an entire app checkpoint for " + appContext.getAppName() + "@" + commandConfirmationEvent.getCommandTimestamp() + " but failed to promote it to a full checkpoint!"));
        }
        final long delayAdvance = (System.currentTimeMillis() - commandConfirmationEvent.getCommandTimestamp()) / 1000L;
        Server.server.getAppManager().scheduleCheckpoint(appContext.getAppId(), delayAdvance);
        if (Logger.getLogger("Commands").isDebugEnabled()) {
            Logger.getLogger("Commands").debug((Object)("App Manager handled a checkpoint confirmation " + appContext.getAppName() + "@" + commandConfirmationEvent.getCommandTimestamp() + " CHECKPOINT COMPLETE!"));
        }
        return appContext.getCurrentStatus();
    }
    
    static {
        WaitCheckpointingActionHandler.logger = Logger.getLogger((Class)AppCheckpointPersistenceLayer.class);
    }
}
