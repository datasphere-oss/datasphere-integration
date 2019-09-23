package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;

public class NodeRemoveTransientStateActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final MembershipEvent nodeEvent = (MembershipEvent)event;
        MetaInfo.StatusInfo.Status returnStatus = appContext.getCurrentStatus();
        if (appContext.removedAndCheckWaitStatus(nodeEvent.getMember()) == 0) {
            returnStatus = MetaInfo.StatusInfo.Status.DEPLOYED;
        }
        appContext.addEventToDeferredQueue(event);
        return returnStatus;
    }
}
