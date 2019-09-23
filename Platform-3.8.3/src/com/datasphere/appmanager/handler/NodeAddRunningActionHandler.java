package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class NodeAddRunningActionHandler extends BaseActionHandler
{
    public NodeAddRunningActionHandler() {
        super(false);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final Map result = NodeAddActionHandler.handleAddNode(appContext, event, managedNodes);
        if (!result.isEmpty()) {
            appContext.execute(ActionType.STOP, false);
            appContext.execute(ActionType.STOP_CACHES, false);
            appContext.startCaches();
            return MetaInfo.StatusInfo.Status.DEPLOYING;
        }
        return appContext.getCurrentStatus();
    }
}
