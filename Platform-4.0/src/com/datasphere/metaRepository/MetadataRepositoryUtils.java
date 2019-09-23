package com.datasphere.metaRepository;

import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class MetadataRepositoryUtils
{
    private static Logger logger;
    
    public static MetaInfo.Flow getAppMetaObjectBelongsTo(final MetaInfo.MetaObject currentMetaObj) {
        if (currentMetaObj == null) {
            return null;
        }
        if (currentMetaObj.getType().equals(EntityType.APPLICATION)) {
            return (MetaInfo.Flow)currentMetaObj;
        }
        final Set<UUID> set = currentMetaObj.getReverseIndexObjectDependencies();
        if (set != null || set.size() == 0) {
            for (final UUID parentObjId : set) {
                try {
                    final MetaInfo.MetaObject parentObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(parentObjId, HSecurityManager.TOKEN);
                    if (parentObj.type == EntityType.APPLICATION && isAppContainsThisStream((MetaInfo.Flow)parentObj, currentMetaObj)) {
                        if (MetadataRepositoryUtils.logger.isInfoEnabled()) {
                            MetadataRepositoryUtils.logger.info((Object)(currentMetaObj.name + " " + currentMetaObj.getType().name() + " belongs to " + parentObj.name));
                        }
                        return (MetaInfo.Flow)parentObj;
                    }
                    if (parentObj.type != EntityType.FLOW) {
                        continue;
                    }
                    final MetaInfo.Flow result = getAppMetaObjectBelongsTo(parentObj);
                    if (result != null) {
                        return result;
                    }
                    continue;
                }
                catch (MetaDataRepositoryException e) {
                    MetadataRepositoryUtils.logger.error((Object)"error getting application metainfo object", (Throwable)e);
                }
            }
        }
        return null;
    }
    
    public static boolean isAppContainsThisStream(final MetaInfo.Flow app, final MetaInfo.MetaObject metaObj) {
        if (app == null || metaObj == null) {
            return false;
        }
        final Set<UUID> subSet = app.objects.get(metaObj.getType());
        if (subSet != null && subSet.contains(metaObj.uuid)) {
            return true;
        }
        final Set<UUID> flowSet = app.getObjects(EntityType.FLOW);
        if (flowSet != null) {
            for (final UUID flowId : flowSet) {
                MetaInfo.Flow flowObject = null;
                try {
                    flowObject = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowId, HSecurityManager.TOKEN);
                }
                catch (MetaDataRepositoryException e) {
                    MetadataRepositoryUtils.logger.error((Object)"error getting application metainfo object", (Throwable)e);
                }
                if (isAppContainsThisStream(flowObject, metaObj)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    static {
        MetadataRepositoryUtils.logger = Logger.getLogger((Class)MetadataRepositoryUtils.class);
    }
}
