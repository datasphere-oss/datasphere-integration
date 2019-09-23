package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;
import java.util.*;

public class NESNodeUpdateActionHandler implements ActionHandler
{
    private static Logger logger;
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final Map<UUID, List<UUID>> result = appContext.deployFlowOnNodes(managedNodes);
        if (!appContext.hasEnoughServers()) {
            return MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS;
        }
        appContext.startCaches();
        return MetaInfo.StatusInfo.Status.DEPLOYING;
    }
    
    static {
        NESNodeUpdateActionHandler.logger = Logger.getLogger((Class)NESNodeUpdateActionHandler.class);
    }
}
