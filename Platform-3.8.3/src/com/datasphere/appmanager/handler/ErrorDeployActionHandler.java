package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;

public class ErrorDeployActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final NodeEvent nodeEvent = (NodeEvent)event;
        final ExceptionEvent exceptionEvent = ((NodeEvent)event).getExceptionEvent();
        if (exceptionEvent.getEpochNumber().equals(appContext.getEpochNumber())) {
            appContext.execute(ActionType.STOP_CACHES, false);
            appContext.addExceptionEvent(exceptionEvent);
            return MetaInfo.StatusInfo.Status.CRASH;
        }
        return appContext.getCurrentStatus();
    }
}
