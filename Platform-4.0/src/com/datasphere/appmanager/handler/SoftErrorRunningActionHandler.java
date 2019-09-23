package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;

public class SoftErrorRunningActionHandler implements ActionHandler
{
    private static Logger logger;
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final ExceptionEvent exceptionEvent = ((AppManagerEvent)event).getExceptionEvent();
        appContext.execute(ActionType.STOP, false);
        appContext.execute(ActionType.STOP_CACHES, false);
        appContext.addExceptionEvent(exceptionEvent);
        return MetaInfo.StatusInfo.Status.CRASH;
    }
    
    static {
        SoftErrorRunningActionHandler.logger = Logger.getLogger((Class)SoftErrorRunningActionHandler.class);
    }
}
