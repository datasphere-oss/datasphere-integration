package com.datasphere.runtime.components;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import java.util.*;

public abstract class PropertyVariablePermissionChecker
{
    private static final Logger logger;
    
    public static boolean propertyVariableAccessChecker(final AuthToken token, String namespace, final Map<String, Object>... properties) throws MetaDataRepositoryException {
        for (final Map<String, Object> property : properties) {
            for (final Map.Entry entry : property.entrySet()) {
                if (entry.getValue() instanceof String && entry.getValue().toString().trim().startsWith("$")) {
                    String propEnvKey = entry.getValue().toString().trim().substring(1);
                    if (propEnvKey != null && !propEnvKey.trim().isEmpty() && propEnvKey.contains(".")) {
                        final String[] splitKeys = propEnvKey.split("\\.");
                        propEnvKey = splitKeys[1];
                        namespace = splitKeys[0];
                    }
                    final MetaInfo.PropertyVariable obj = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, propEnvKey, null, HSecurityManager.TOKEN);
                    if (obj == null) {
                        return true;
                    }
                    try {
                        final boolean check = PermissionUtility.checkPermission(obj, ObjectPermission.Action.deploy, token, true);
                        return check;
                    }
                    catch (MetaDataRepositoryException e) {
                        PropertyVariablePermissionChecker.logger.error((Object)("Security Exception while trying to access prop variable object" + e.getMessage()));
                    }
                }
            }
        }
        return true;
    }
    
    static {
        logger = Logger.getLogger((Class)PropertyVariablePermissionChecker.class);
    }
}
