package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import org.apache.log4j.*;

import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class WaitQuiescingFlushingActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final CommandConfirmationEvent commandConfirmationEvent = (CommandConfirmationEvent)event;
        final int stillWaitingCount = appContext.removedAndCheckWaitStatus(commandConfirmationEvent.getServerId());
        if (stillWaitingCount > 0) {
            if (Logger.getLogger("AppManager").isDebugEnabled()) {
                Logger.getLogger("AppManager").debug((Object)("App Manager handled " + event.getEventAction() + " but still waiting on more nodes"));
            }
            return appContext.getCurrentStatus();
        }
        long commandTimestamp = System.currentTimeMillis();
        List<Property> params = null;
        if (event != null && event instanceof ApiCallEvent) {
            final ApiCallEvent apiCallEvent = (ApiCallEvent)event;
            final Object value = apiCallEvent.getParams().get("COMMAND_TIMESTAMP");
            if (value != null && value instanceof Long) {
                commandTimestamp = (long)value;
            }
        }
        params = new ArrayList<Property>();
        params.add(new Property("COMMAND_TIMESTAMP", commandTimestamp));
        appContext.startWaitingForCommandNotifications(Event.EventAction.NODE_APP_QUIESCE_CHECKPOINTED, commandTimestamp, new HashSet<UUID>(managedNodes));
        appContext.execute(ActionType.QUIESCE_CHECKPOINT, managedNodes, params, false);
        return appContext.getCurrentStatus();
    }
}
