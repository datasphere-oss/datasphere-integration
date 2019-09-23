package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.meta.*;
import java.util.*;

public class NodeAddDeployedActionHandler extends BaseActionHandler
{
    public NodeAddDeployedActionHandler() {
        super(false);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final Map result = NodeAddActionHandler.handleAddNode(appContext, event, managedNodes);
        if (!result.isEmpty()) {
            appContext.stopCaches();
            appContext.startCaches();
            return MetaInfo.StatusInfo.Status.DEPLOYING;
        }
        return appContext.getCurrentStatus();
    }
}
