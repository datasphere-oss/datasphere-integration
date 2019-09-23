package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class NodeDeleteRunningActionHandler implements ActionHandler
{
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        if (!(event instanceof MembershipEvent)) {
            throw new IllegalStateException("Unexpected event type found");
        }
        final MembershipEvent nodeEvent = (MembershipEvent)event;
        if (!appContext.removedManagedNode(nodeEvent.getMember())) {
            return appContext.getCurrentStatus();
        }
        appContext.execute(ActionType.STOP, false);
        final Map<UUID, List<UUID>> result = appContext.deployFlowOnNodes(managedNodes);
        if (!appContext.hasEnoughServers()) {
            return MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS;
        }
        appContext.execute(ActionType.STOP_CACHES, false);
        appContext.startCaches();
        return MetaInfo.StatusInfo.Status.DEPLOYING;
    }
}
