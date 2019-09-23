package com.datasphere.appmanager.handler;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.uuid.*;
import com.datasphere.appmanager.*;
import com.datasphere.metaRepository.*;

public abstract class BaseActionHandler implements ActionHandler
{
    private static Logger logger;
    private final boolean isApi;
    
    public BaseActionHandler(final boolean isApi) {
        this.isApi = isApi;
    }
    
    public void updateFlowDeploymentPlan(final List<MetaInfo.Flow.Detail> deploymentPlan, final UUID flowId) throws MetaDataRepositoryException {
        FlowUtil.updateFlowDeploymentPlan(deploymentPlan, flowId);
    }
    
    public MetaInfo.Flow getFlow(final UUID appId) throws MetaDataRepositoryException {
        return FlowUtil.getFlow(appId);
    }
    
    public boolean isApi() {
        return this.isApi;
    }
    
    static {
        BaseActionHandler.logger = Logger.getLogger((Class)BaseActionHandler.class);
    }
}
