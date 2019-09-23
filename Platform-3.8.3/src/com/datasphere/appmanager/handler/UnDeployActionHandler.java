package com.datasphere.appmanager.handler;

import org.apache.log4j.*;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.runtime.*;
import java.util.*;

public class UnDeployActionHandler extends BaseActionHandler
{
    private static Logger logger;
    private final boolean isFromError;
    
    public UnDeployActionHandler(final boolean isFromError, final boolean isApi) {
        super(isApi);
        this.isFromError = isFromError;
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        if (this.isApi()) {
            appContext.updateDesiredStatus(MetaInfo.StatusInfo.Status.CREATED);
        }
        final MetaInfo.Flow flow = appContext.getFlow();
        UnDeployActionHandler.logger.info((Object)("Undeploying the application for application " + flow.getFullName()));
        final List<UUID> serverIds = appContext.getManagedNodes();
        if (!this.isFromError) {
            appContext.execute(ActionType.STOP_CACHES, false);
        }
        appContext.execute(ActionType.UNDEPLOY, false);
        this.updateFlowDeploymentPlan(null, appContext.getAppId());
        appContext.clearExceptions();
        return MetaInfo.StatusInfo.Status.CREATED;
    }
    
    static {
        UnDeployActionHandler.logger = Logger.getLogger((Class)UnDeployActionHandler.class);
    }
}
