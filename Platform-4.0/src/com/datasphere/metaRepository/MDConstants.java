package com.datasphere.metaRepository;

import com.datasphere.runtime.components.*;
import com.datasphere.runtime.utils.*;
import com.datasphere.runtime.meta.*;

public class MDConstants
{
    public static final String status = "status";
    public static final String serversForDeployment = "serversForDeployment";
    public static final String ENCRYPTED_KEYWORDS_PROPERTYSET_NAME = "encryptedWords";
    public static final String ENCRYPTED_FLAG = "_encrypted";
    public static final String SUBSCRIPTION_PROP = "isSubscription";
    
    public static void checkNullParams(final String exceptionMessage, final Object... args) throws MetaDataRepositoryException {
        for (final Object arg : args) {
            if (arg == null) {
                throw new MetaDataRepositoryException(exceptionMessage);
            }
        }
    }
    
    public static String makeUrlWithoutVersion(final String namespace, final EntityType eType, final String name) {
        return NamePolicy.makeKey(namespace + ":" + eType + ":" + name);
    }
    
    public static String makeURLWithoutVersion(final MetaInfo.MetaObject mObject) {
        return NamePolicy.makeKey(mObject.nsName + ":" + mObject.type + ":" + mObject.name);
    }
    
    public static String makeURLWithVersion(final String nvUrl, final Integer version) {
        return NamePolicy.makeKey(nvUrl + ":" + (int)version);
    }
    
    public static String makeURLWithVersion(final EntityType eType, final String namespace, final String name, final Integer version) {
        return NamePolicy.makeKey(namespace + ":" + eType + ":" + name + ":" + (int)version);
    }
    
    public static String stripVersion(final String vUrl) {
        return vUrl.substring(0, vUrl.lastIndexOf(":"));
    }
    
    public enum typeOfPut
    {
        STATUS_INFO, 
        DEPLOYMENT_INFO, 
        SHOWSTREAMOBJECT, 
        METAOBJECT, 
        SERVERMETAOBJECT;
    }
    
    public enum typeOfGet
    {
        BY_UUID, 
        BY_NAME, 
        BY_ENTITY_TYPE, 
        BY_NAMESPACE, 
        BY_NAMESPACE_AND_ENTITY_TYPE, 
        BY_STATUSINFO, 
        BY_POSITIONINFO, 
        BY_SERVERFORDEPLOYMENT, 
        BY_ALL;
    }
    
    public enum typeOfRemove
    {
        BY_UUID, 
        BY_NAME, 
        STATUS_INFO, 
        DEPLOYMENT_INFO;
    }
    
    public enum mdcListener
    {
        server, 
        status, 
        showStream, 
        actions, 
        urlToMeta, 
        deploymentInfo;
    }
    
    public static class MapNames
    {
        public static final String PositionInfo = "#PositionInfo";
        public static final String ObjectVersions = "#objectVersions";
        public static final String UuidToUrl = "#uuidToUrl";
        public static final String UrlToMetaObject = "#urlToMetaObject";
        public static final String ETypeToMetaObject = "#eTypeToMetaObject";
        public static final String Status = "#status";
        public static final String ShowStream = "#showStream";
        public static final String DeploymentInfo = "deployedObjects";
        public static final String Servers = "#servers";
    }
}
