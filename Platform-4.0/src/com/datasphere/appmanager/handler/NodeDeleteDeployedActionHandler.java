package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;

import java.util.*;

public class NodeDeleteDeployedActionHandler implements ActionHandler
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
        appContext.stopCaches();
        final Map<UUID, List<UUID>> result = appContext.deployFlowOnNodes(managedNodes);
        if (!appContext.hasEnoughServers()) {
            return MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS;
        }
        appContext.startCaches();
        return MetaInfo.StatusInfo.Status.DEPLOYING;
    }
}
