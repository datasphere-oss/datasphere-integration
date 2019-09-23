package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;

public class WaitDeployingActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final NodeEvent nodeEvent = (NodeEvent)event;
        if (appContext.removedAndCheckWaitStatus(nodeEvent.getServerId()) != 0) {
            return appContext.getCurrentStatus();
        }
        final MetaInfo.StatusInfo.Status currentStatus = appContext.getDesiredAppStatus();
        if (currentStatus == MetaInfo.StatusInfo.Status.QUIESCED) {
            return MetaInfo.StatusInfo.Status.QUIESCED;
        }
        return MetaInfo.StatusInfo.Status.DEPLOYED;
    }
}
