package com.datasphere.appmanager.handler;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.appmanager.*;
import com.datasphere.appmanager.event.*;
import com.datasphere.exception.*;
import com.datasphere.runtime.*;

import java.util.*;

public class DeployActionHandler extends BaseActionHandler
{
    public DeployActionHandler(final boolean isApi) {
        super(isApi);
    }
    
    @Override
    public MetaInfo.StatusInfo.Status handle(final AppContext appContext, final Event event, final ArrayList<UUID> managedNodes) throws Exception {
        final UUID appId = appContext.getAppId();
        appContext.resetCaches();
        final MetaInfo.StatusInfo.Status currentStatus = appContext.getDesiredAppStatus();
        if (this.isApi()) {
            appContext.updateDesiredStatus(MetaInfo.StatusInfo.Status.DEPLOYED);
            final Object deploymentPlan = ((ApiCallEvent)event).getParams().get("DEPLOYMENTPLAN");
            if (deploymentPlan != null) {
                this.updateFlowDeploymentPlan((List<MetaInfo.Flow.Detail>)deploymentPlan, appId);
            }
        }
        try {
            final Map<UUID, List<UUID>> result = appContext.deployFlowOnNodes(managedNodes);
            if (!appContext.hasEnoughServers()) {
                if (this.isApi()) {
                    throw new Warning("Not enough Servers in the deployment group to complete the deployment. It is possible that every node in deployment group is already at maximum capacity based on limit applications setting for the deployment group or the number of nodes in deployment group does not meet mininum number of required servers");
                }
                return MetaInfo.StatusInfo.Status.NOT_ENOUGH_SERVERS;
            }
            else {
                if (!result.isEmpty()) {
                    appContext.execute(ActionType.STOP_CACHES, false);
                    appContext.startCaches();
                    return MetaInfo.StatusInfo.Status.DEPLOYING;
                }
                return MetaInfo.StatusInfo.Status.DEPLOYED;
            }
        }
        catch (Throwable e) {
            try {
                appContext.updateDesiredStatus(currentStatus);
                FlowUtil.updateFlowDeploymentPlan(null, appId);
                appContext.execute(ActionType.UNDEPLOY, false);
            }
            catch (Throwable t) {}
            throw e;
        }
    }
}
