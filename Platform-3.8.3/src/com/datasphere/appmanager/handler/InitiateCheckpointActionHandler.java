package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class InitiateCheckpointActionHandler extends BaseActionHandler
{
    private static Logger logger;
    
    public InitiateCheckpointActionHandler(final boolean isApi) {
        super(isApi);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final long commandTimestamp = System.currentTimeMillis();
        final List<Property> params = new ArrayList<Property>();
        params.add(new Property("COMMAND_TIMESTAMP", commandTimestamp));
        if (Logger.getLogger("Commands").isInfoEnabled()) {
            Logger.getLogger("Commands").info((Object)("Starting a checkpoint for app " + appContext.getAppName() + " at timestamp " + commandTimestamp));
        }
        appContext.startWaitingForCommandNotifications(Event.EventAction.NODE_APP_CHECKPOINTED, commandTimestamp, new HashSet<UUID>(managedNodes));
        appContext.execute(ActionType.CHECKPOINT, managedNodes, params, false);
        return appContext.getCurrentStatus();
    }
    
    static {
        InitiateCheckpointActionHandler.logger = Logger.getLogger((Class)InitiateCheckpointActionHandler.class);
    }
}
