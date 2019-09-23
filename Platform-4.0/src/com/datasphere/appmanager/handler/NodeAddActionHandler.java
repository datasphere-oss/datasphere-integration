package com.datasphere.appmanager.handler;

import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.uuid.*;

import java.util.*;

public class NodeAddActionHandler
{
    public static Map handleAddNode(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        if (event instanceof MembershipEvent) {
            final MembershipEvent nodeEvent = (MembershipEvent)event;
            final List serverId = new ArrayList();
            serverId.add(nodeEvent.getMember());
            return appContext.deployFlowOnNodes(managedNodes);
        }
        throw new IllegalStateException("Unexpected event type found");
    }
}
