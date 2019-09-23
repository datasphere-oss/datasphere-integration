package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;

public class ErrorRunningActionHandler implements ActionHandler
{
    private static Logger logger;
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final ExceptionEvent exceptionEvent = ((NodeEvent)event).getExceptionEvent();
        if (exceptionEvent.getEpochNumber().compareTo(appContext.getEpochNumber()) == 0) {
            appContext.execute(ActionType.STOP, false);
            appContext.execute(ActionType.STOP_CACHES, false);
            appContext.addExceptionEvent(exceptionEvent);
            return MetaInfo.StatusInfo.Status.CRASH;
        }
        ErrorRunningActionHandler.logger.error((Object)("Ignoring Error event because epoch in exception event " + exceptionEvent.getEpochNumber() + " does not match epoch in appContext " + appContext.getEpochNumber()));
        return appContext.getCurrentStatus();
    }
    
    static {
        ErrorRunningActionHandler.logger = Logger.getLogger((Class)ErrorRunningActionHandler.class);
    }
}
