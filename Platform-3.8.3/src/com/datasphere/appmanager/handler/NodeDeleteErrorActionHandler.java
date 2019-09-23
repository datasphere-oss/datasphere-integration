package com.datasphere.appmanager.handler;

import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;

public class NodeDeleteErrorActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        if (event instanceof MembershipEvent) {
            final MembershipEvent nodeEvent = (MembershipEvent)event;
            appContext.removedManagedNode(nodeEvent.getMember());
            return appContext.getCurrentStatus();
        }
        throw new IllegalStateException("Unexpected event type found");
    }
}
