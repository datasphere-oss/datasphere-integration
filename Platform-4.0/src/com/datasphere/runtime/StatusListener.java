package com.datasphere.runtime;

import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import org.apache.log4j.*;
import com.hazelcast.core.*;
import com.datasphere.runtime.components.*;
import com.datasphere.security.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.metaRepository.*;

public class StatusListener implements HazelcastIMapListener<UUID, MetaInfo.StatusInfo>
{
    private static Logger logger;
    
    public void entryAdded(final EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent) {
        if (((MetaInfo.StatusInfo)entryEvent.getValue()).getType().equals(EntityType.APPLICATION)) {
            try {
                final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)entryEvent.getKey(), HSecurityManager.TOKEN);
                if (!mo.getName().contains("MonitoringSourceApp") && !mo.getName().contains("MonitoringProcessApp")) {
                    MonitorCollector.reportStateChange((UUID)entryEvent.getKey(), ((MetaInfo.StatusInfo)entryEvent.getValue()).serializeToHumanReadableString(System.currentTimeMillis()));
                }
            }
            catch (MetaDataRepositoryException e) {
                StatusListener.logger.warn((Object)e.getMessage(), (Throwable)e);
            }
        }
    }
    
    public void entryRemoved(final EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent) {
    }
    
    public void entryUpdated(final EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent) {
        if (((MetaInfo.StatusInfo)entryEvent.getValue()).getType().equals(EntityType.APPLICATION)) {
            try {
                final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)entryEvent.getKey(), HSecurityManager.TOKEN);
                if (!mo.getName().contains("MonitoringSourceApp") && !mo.getName().contains("MonitoringProcessApp") && ((MetaInfo.StatusInfo)entryEvent.getValue()).getPreviousStatus() != ((MetaInfo.StatusInfo)entryEvent.getValue()).getStatus()) {
                    MonitorCollector.reportStateChange((UUID)entryEvent.getKey(), ((MetaInfo.StatusInfo)entryEvent.getValue()).serializeToHumanReadableString(System.currentTimeMillis()));
                }
            }
            catch (MetaDataRepositoryException e) {
                StatusListener.logger.warn((Object)e.getMessage(), (Throwable)e);
            }
        }
    }
    
    static {
        StatusListener.logger = Logger.getLogger((Class)StatusListener.class);
    }
}
