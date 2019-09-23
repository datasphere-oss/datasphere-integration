package com.datasphere.metaRepository;

import org.apache.log4j.Logger;

import com.datasphere.exception.SecurityException;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class PermissionUtility
{
    private static final String colon = ":";
    private static Logger logger;
    
    private static ObjectPermission.ObjectType getPermissionObjectType(final EntityType eType) {
        switch (eType) {
            case UNKNOWN: {
                return ObjectPermission.ObjectType.unknown;
            }
            case APPLICATION: {
                return ObjectPermission.ObjectType.application;
            }
            case FLOW: {
                return ObjectPermission.ObjectType.flow;
            }
            case STREAM: {
                return ObjectPermission.ObjectType.stream;
            }
            case WINDOW: {
                return ObjectPermission.ObjectType.window;
            }
            case TYPE: {
                return ObjectPermission.ObjectType.type;
            }
            case CQ: {
                return ObjectPermission.ObjectType.cq;
            }
            case QUERY: {
                return ObjectPermission.ObjectType.query;
            }
            case SOURCE: {
                return ObjectPermission.ObjectType.source;
            }
            case TARGET: {
                return ObjectPermission.ObjectType.target;
            }
            case PROPERTYSET: {
                return ObjectPermission.ObjectType.propertyset;
            }
            case PROPERTYVARIABLE: {
                return ObjectPermission.ObjectType.propertyvariable;
            }
            case HDSTORE: {
                return ObjectPermission.ObjectType.hdstore;
            }
            case PROPERTYTEMPLATE: {
                return ObjectPermission.ObjectType.propertytemplate;
            }
            case CACHE: {
                return ObjectPermission.ObjectType.cache;
            }
            case ALERTSUBSCRIBER: {
                return ObjectPermission.ObjectType.alertsubscriber;
            }
            case SERVER: {
                return ObjectPermission.ObjectType.server;
            }
            case USER: {
                return ObjectPermission.ObjectType.user;
            }
            case ROLE: {
                return ObjectPermission.ObjectType.role;
            }
            case INITIALIZER: {
                return ObjectPermission.ObjectType.initializer;
            }
            case DG: {
                return ObjectPermission.ObjectType.deploymentgroup;
            }
            case VISUALIZATION: {
                return ObjectPermission.ObjectType.visualization;
            }
            case NAMESPACE: {
                return ObjectPermission.ObjectType.namespace;
            }
            case DASHBOARD: {
                return ObjectPermission.ObjectType.dashboard;
            }
            case PAGE: {
                return ObjectPermission.ObjectType.page;
            }
            case QUERYVISUALIZATION: {
                return ObjectPermission.ObjectType.queryvisualization;
            }
        }
        return null;
    }
    
    public static ObjectPermission getCreateOrUpdatePermission(final MetaInfo.MetaObject mObject, final MDCache cache) throws SecurityException {
        final String appName = mObject.nsName;
        if (PermissionUtility.logger.isDebugEnabled()) {
            PermissionUtility.logger.debug((Object)("Object key : " + NamePolicy.makeKey(mObject.nsName + ":" + mObject.getType() + ":" + mObject.name)));
        }
        ObjectPermission permission;
        if (cache.contains(MDConstants.typeOfRemove.BY_NAME, null, NamePolicy.makeKey(mObject.nsName + ":" + mObject.getType() + ":" + mObject.name))) {
            permission = new ObjectPermission(appName, ObjectPermission.Action.update, getPermissionObjectType(mObject.type), mObject.name);
        }
        else if (mObject.type == EntityType.NAMESPACE) {
            permission = new ObjectPermission("Global", ObjectPermission.Action.create, ObjectPermission.ObjectType.namespace, mObject.name);
        }
        else {
            permission = new ObjectPermission(appName, ObjectPermission.Action.create, getPermissionObjectType(mObject.type), mObject.name);
        }
        return permission;
    }
    
    public static boolean checkReadPermission(final MetaInfo.MetaObject mObject, final AuthToken token, final HSecurityManager sManager) throws MetaDataRepositoryException, SecurityException {
        MDConstants.checkNullParams("Can't pass NULL parameters while checking for READ permission on a MetaObject, \nMetaObject: " + mObject.getFullName() + "\nToken: " + token + "\nSecurity Manager: " + ((sManager == null) ? "Null" : "Present"), mObject, token, sManager);
        final ObjectPermission permission = new ObjectPermission(mObject.nsName, ObjectPermission.Action.read, getPermissionObjectType(mObject.type), mObject.name);
        return sManager.isAllowedAndNotDisallowed(token, permission);
    }
    
    public static boolean checkPermission(final MetaInfo.MetaObject obj, final ObjectPermission.Action action, final AuthToken token, final boolean shouldThrow) throws SecurityException, MetaDataRepositoryException {
        final String errString = "";
        MDConstants.checkNullParams("Can't pass NULL parameters while checking for permission on a MetaObject", obj, token, action);
        try {
            if ((obj.getMetaInfoStatus().isAnonymous() || obj.getMetaInfoStatus().isAdhoc()) && action.equals(ObjectPermission.Action.create)) {
                return true;
            }
            final HSecurityManager sm = HSecurityManager.get();
            if (sm == null) {
                return true;
            }
            ObjectPermission.ObjectType otype = getPermissionObjectType(obj.type);
            if (obj.type == EntityType.TARGET && ((MetaInfo.Target)obj).isSubscription()) {
                otype = ObjectPermission.ObjectType.subscription;
            }
            final ObjectPermission permission = new ObjectPermission(obj.nsName, action, otype, obj.name, ObjectPermission.PermissionType.allow);
            if (!sm.isDisallowed(token, permission)) {
                if (sm.isAllowed(token, permission)) {
                    return true;
                }
                final MetaInfo.Flow app = obj.getCurrentApp();
                if (app != null) {
                    final ObjectPermission appPermission = new ObjectPermission(app.nsName, action, getPermissionObjectType(app.type), app.name, ObjectPermission.PermissionType.allow);
                    if (sm.isAllowedAndNotDisallowed(token, appPermission)) {
                        return true;
                    }
                }
                final MetaInfo.Dashboard currentDashBoard = obj.getCurrentDashBoard();
                if (currentDashBoard != null) {
                    final ObjectPermission dashPermission = new ObjectPermission(currentDashBoard.nsName, action, getPermissionObjectType(EntityType.DASHBOARD), currentDashBoard.getName(), ObjectPermission.PermissionType.allow);
                    if (sm.isAllowedAndNotDisallowed(token, dashPermission)) {
                        return true;
                    }
                }
            }
        }
        catch (Exception e) {
            throw e;
        }
        if (shouldThrow) {
            throw new SecurityException("No permission to " + action + " " + obj.type.toString().toLowerCase() + " " + obj.nsName + "." + obj.name + " " + errString);
        }
        return false;
    }
    
    public static boolean checkPermission(final UUID uuid, final ObjectPermission.Action action, final AuthToken token, final boolean shouldThrow) throws SecurityException, MetaDataRepositoryException {
        final MDRepository mdr = MetadataRepository.getINSTANCE();
        final MetaInfo.MetaObject metaObject = mdr.getMetaObjectByUUID(uuid, token);
        return checkPermission(metaObject, action, token, shouldThrow);
    }
    
    static {
        PermissionUtility.logger = Logger.getLogger((Class)MDCache.class);
    }
}
