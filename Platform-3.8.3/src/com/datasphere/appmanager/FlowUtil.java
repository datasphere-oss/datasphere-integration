package com.datasphere.appmanager;

import com.datasphere.runtime.meta.*;
import com.datasphere.security.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.*;
import java.util.*;
import com.datasphere.metaRepository.*;

public class FlowUtil
{
    static MDRepository metadataRepository;
    
    public static void updateFlowDeploymentPlan(final List<MetaInfo.Flow.Detail> deploymentPlan, final UUID flowId) throws MetaDataRepositoryException {
        final MetaInfo.Flow flow = (MetaInfo.Flow)FlowUtil.metadataRepository.getMetaObjectByUUID(flowId, HSecurityManager.TOKEN);
        flow.setDeploymentPlan(deploymentPlan);
        FlowUtil.metadataRepository.updateMetaObject(flow, HSecurityManager.TOKEN);
    }
    
    public static MetaInfo.StatusInfo getCurrentStatus(final UUID appId) throws MetaDataRepositoryException {
        return FlowUtil.metadataRepository.getStatusInfo(appId, HSecurityManager.TOKEN);
    }
    
    public static MetaInfo.StatusInfo.Status getCurrentStatusFromAppManager(final UUID appId) throws Exception {
        final AppManagerRequestClient appManagerRequestClient = new AppManagerRequestClient(HSecurityManager.TOKEN);
        final ChangeApplicationStateResponse response = appManagerRequestClient.sendRequest(ActionType.STATUS, appId, null);
        appManagerRequestClient.close();
        return ((ApplicationStatusResponse)response).getStatus();
    }
    
    public static MetaInfo.StatusInfo.Status getCurrentStatusFromAppManager(final UUID appId, final AuthToken authToken) throws Exception {
        final AppManagerRequestClient appManagerRequestClient = new AppManagerRequestClient(authToken);
        final ChangeApplicationStateResponse response = appManagerRequestClient.sendRequest(ActionType.STATUS, appId, null);
        appManagerRequestClient.close();
        return ((ApplicationStatusResponse)response).getStatus();
    }
    
    public static Set<String> getCurrentErrorsFromAppManager(final UUID uuid, final AuthToken token) throws Exception {
        final AppManagerRequestClient appManagerRequestClient = new AppManagerRequestClient(token);
        final ChangeApplicationStateResponse response = appManagerRequestClient.sendRequest(ActionType.STATUS, uuid, null);
        appManagerRequestClient.close();
        final Set<String> errorString = new HashSet<String>();
        for (final ExceptionEvent e : ((ApplicationStatusResponse)response).getExceptionEvents()) {
            errorString.add(e.userString());
        }
        return errorString;
    }
    
    public static MetaInfo.StatusInfo.Status updateFlowActualStatus(final UUID appId, final MetaInfo.StatusInfo.Status newStatus) throws MetaDataRepositoryException {
        MetaInfo.StatusInfo status = FlowUtil.metadataRepository.getStatusInfo(appId, HSecurityManager.TOKEN);
        if (status == null) {
            status = new MetaInfo.StatusInfo();
            final MetaInfo.Flow flow = (MetaInfo.Flow)FlowUtil.metadataRepository.getMetaObjectByUUID(appId, HSecurityManager.TOKEN);
            status.construct(appId, newStatus, flow.getType(), flow.getName());
        }
        else {
            if (status.status != MetaInfo.StatusInfo.Status.DEPLOYING) {
                status.previousStatus = status.status;
            }
            status.status = newStatus;
        }
        FlowUtil.metadataRepository.putStatusInfo(status, HSecurityManager.TOKEN);
        return newStatus;
    }
    
    public static MetaInfo.Flow getFlow(final UUID flowId) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject object = FlowUtil.metadataRepository.getMetaObjectByUUID(flowId, HSecurityManager.TOKEN);
        if (object instanceof MetaInfo.Flow) {
            return (MetaInfo.Flow)object;
        }
        return null;
    }
    
    static {
        FlowUtil.metadataRepository = MetadataRepository.getINSTANCE();
    }
}
