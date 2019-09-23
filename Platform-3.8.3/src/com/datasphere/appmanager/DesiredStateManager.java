package com.datasphere.appmanager;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;

public class DesiredStateManager
{
    static MDRepository metadataRepository;
    
    public static MetaInfo.StatusInfo.Status getDesiredAppStatus(final UUID appId) throws MetaDataRepositoryException {
        final MetaInfo.Flow flow = (MetaInfo.Flow)DesiredStateManager.metadataRepository.getMetaObjectByUUID(appId, HSecurityManager.TOKEN);
        return flow.getFlowStatus();
    }
    
    public static void updateFlowDesiredStatus(final MetaInfo.StatusInfo.Status newStatus, final UUID flowId) throws Exception {
        final MetaInfo.Flow flow = (MetaInfo.Flow)DesiredStateManager.metadataRepository.getMetaObjectByUUID(flowId, HSecurityManager.TOKEN);
        flow.setFlowStatus(newStatus);
        DesiredStateManager.metadataRepository.updateMetaObject(flow, HSecurityManager.TOKEN);
    }
    
    static {
        DesiredStateManager.metadataRepository = MetadataRepository.getINSTANCE();
    }
}
