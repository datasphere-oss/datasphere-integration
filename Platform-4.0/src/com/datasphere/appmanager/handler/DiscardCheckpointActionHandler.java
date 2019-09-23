package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.metaRepository.*;
import com.datasphere.persistence.*;

public class DiscardCheckpointActionHandler implements ActionHandler
{
    private static Logger logger;
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final CommandConfirmationEvent commandConfirmationEvent = (CommandConfirmationEvent)event;
        final boolean success = StatusDataStore.getInstance().clearPendingAppCheckpoint(appContext.getAppId(), commandConfirmationEvent.getCommandTimestamp());
        if (!success) {
            DiscardCheckpointActionHandler.logger.warn((Object)("Recorded an entire app checkpoint for " + appContext.getAppName() + " but then tried to discard it and failed !"));
        }
        return appContext.getCurrentStatus();
    }
    
    static {
        DiscardCheckpointActionHandler.logger = Logger.getLogger((Class)AppCheckpointPersistenceLayer.class);
    }
}
