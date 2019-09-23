package com.datasphere.upgrades;

import java.util.Map;
import java.util.Set;

import com.hazelcast.core.IMap;
import com.datasphere.metaRepository.CDCMetadataDBOperations;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.cdc.CDCMetadataExtension;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class CDCMetadata implements Upgrade
{
    private static String TO;
    
    @Override
    public void upgrade(final String currentVersion, final String upgradeToVersion) throws MetaDataRepositoryException {
        final String string;
        final String combo = string = currentVersion + CDCMetadata.TO + upgradeToVersion;
        switch (string) {
            case "3.7.3-3.7.5": {
                this.upgradeFrom373To375();
                break;
            }
        }
    }
    
    private void upgradeFrom373To375() throws MetaDataRepositoryException {
        final IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
        final CDCMetadataDBOperations cdcMetadataDBInstance = CDCMetadataDBOperations.getCDCMetadataDBInstance((Map<String, MetaInfo.Initializer>)startUpMap);
        final Set<CDCMetadataExtension> cdcData = cdcMetadataDBInstance.getAllCDCMetadata();
        for (final CDCMetadataExtension obj : cdcData) {
            final String parentComponent = obj.getParentComponent();
            final UUID parentComponentUUID = new UUID(parentComponent);
            final MetaInfo.Source mObject = (MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(parentComponentUUID, HSecurityManager.TOKEN);
            final String parentComponentName = mObject.getUri();
            obj.setParentComponent(parentComponentName);
            cdcMetadataDBInstance.store(obj);
        }
    }
    
    static {
        CDCMetadata.TO = "-";
    }
}
