package com.datasphere.drop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.datasphere.historicalcache.Cache;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDClientOps;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.persistence.HStore;
import com.datasphere.proc.BaseProcess;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.ServerUpgradeUtility;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.tungsten.Tungsten;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class DropMetaObject
{
    public static EntityType[] sharedObject;
    private static HSecurityManager securityManager;
    private static Logger logger;
    private static MDRepository metaDataRepository;
    
    public static boolean isValidInput(final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) {
        return metaObject != null && dropRule != null && authToken != null;
    }
    
    public static boolean isNamespaceEmpty(final String nsName, final AuthToken token) throws MetaDataRepositoryException {
    		Iterator iter = HazelcastSingleton.get().getSet("#" + nsName).iterator();
        while (iter.hasNext()) {
        		final UUID uuid = (UUID)iter.next();
            final MetaInfo.MetaObject metaObjectInNamespace = getMetaObjectByUUID(uuid, token);
            if (!(metaObjectInNamespace instanceof MetaInfo.Role) && !metaObjectInNamespace.getMetaInfoStatus().isDropped()) {
                return false;
            }
        }
        return true;
    }
    
    private static String flagDropIfAnonymous(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        String str = "";
        if (metaObject == null) {
            return str;
        }
        if (metaObject.getMetaInfoStatus().isAnonymous()) {
            str += flagDropped(metaObject, authToken);
        }
        return str;
    }
    
    protected static String flagDropIfGenerated(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        String str = "";
        if (metaObject == null) {
            return str;
        }
        if (metaObject.getMetaInfoStatus().isGenerated()) {
            str += flagDropped(metaObject, authToken);
        }
        return str;
    }
    
    private static List<String> flagDropped(final Context ctx, final EntityType entityType, final Set<UUID> metaObjectUUIDList, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
        final List<String> str = new ArrayList<String>();
        for (final UUID objectUUID : metaObjectUUIDList) {
            final MetaInfo.MetaObject metaObject = getMetaObjectByUUID(objectUUID, authToken);
            try {
                checkPermissionToDrop(metaObject, authToken);
            }
            catch (Exception e) {
                str.add(e.getMessage());
                continue;
            }
            if (metaObject == null) {
                throw new MetaDataRepositoryException("Meta Object with ID: " + objectUUID + " is not available");
            }
            str.addAll(dropObject(ctx, metaObject, entityType, dropRule, authToken));
            final MetaInfo.MetaObject droppedObject = getMetaObjectByUUID(metaObject.getUuid(), authToken);
            if (droppedObject == null) {
                continue;
            }
            str.add(flagDropped(droppedObject, authToken));
        }
        return str;
    }
    
    private static boolean isPartOfApplicationOrFlowDeployed(final MetaInfo.Flow applicationOrFlow, final AuthToken authToken) throws MetaDataRepositoryException {
        for (final Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInApplication : applicationOrFlow.objects.entrySet()) {
            final Set<UUID> metaObjectUUIDList = metaObjectsInApplication.getValue();
            for (final UUID uuid : metaObjectUUIDList) {
                final MetaInfo.MetaObject metaObject = getMetaObjectByUUID(uuid, authToken);
                if (metaObject != null && isMetaObjectDeployed(metaObject, metaObjectsInApplication.getKey(), authToken)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static <E extends MetaInfo.MetaObject> List<E> getAllObjectsByNamespaceAndAuthOptimized(final String nsName, final AuthToken token, final boolean ignoresecurity) throws MetaDataRepositoryException {
        final List list = new ArrayList();
		Iterator iter = HazelcastSingleton.get().getSet("#" + nsName).iterator();
        while (iter.hasNext() ) {
        		final UUID uuid = (UUID)iter.next(); 
            list.add(getMetaObjectByUUID(uuid, token));
        }
        return (List<E>)list;
    }
    
    public static <E extends MetaInfo.Flow> Set<E> getApplicationByNamespaceAndAuth(final String nsName, final AuthToken token, final boolean ignoresecurity) throws MetaDataRepositoryException {
        final Set list = new LinkedHashSet();
        Iterator iter = HazelcastSingleton.get().getSet("#" + nsName).iterator();
        while (iter.hasNext() ) {
        		final UUID uuid = (UUID)iter.next(); 
            final MetaInfo.MetaObject x = getMetaObjectByUUID(uuid, token);
            if (x.getType() == EntityType.APPLICATION) {
                list.add(x);
            }
        }
        return (Set<E>)list;
    }
    
    public static List<String> dropObject(final Context ctx, final MetaInfo.MetaObject objectToDrop, final EntityType objectType, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
        final List<String> str = new ArrayList<String>();
        if (objectToDrop.getMetaInfoStatus().isDropped()) {
            return str;
        }
        switch (objectType) {
            case ALERTSUBSCRIBER: {
                break;
            }
            case APPLICATION: {
                str.addAll(DropApplication.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case CACHE: {
                str.addAll(DropCache.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case CQ: {
                str.addAll(DropCQ.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case DG: {
                break;
            }
            case FLOW: {
                str.addAll(DropFlow.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case INITIALIZER: {
                break;
            }
            case PROPERTYSET: {
                str.addAll(DropPropertySet.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case PROPERTYVARIABLE: {
                break;
            }
            case PROPERTYTEMPLATE: {
                break;
            }
            case ROLE: {
                str.addAll(DropRole.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case SERVER: {
                break;
            }
            case SOURCE: {
                str.addAll(DropSource.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case STREAM: {
                str.addAll(DropStream.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case STREAM_GENERATOR: {
                break;
            }
            case TARGET: {
                str.addAll(DropTarget.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case TYPE: {
                str.addAll(DropType.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case USER: {
                str.addAll(DropUser.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case VISUALIZATION: {
                str.addAll(DropVisualization.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case HDSTORE: {
                str.addAll(DropHDStore.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case WINDOW: {
                str.addAll(DropWindow.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case UNKNOWN: {
                DropMetaObject.logger.warn((Object)("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n"));
                str.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
                break;
            }
            case DASHBOARD: {
                str.addAll(DropDashboard.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            case WASTOREVIEW: {
                DropMetaObject.metaDataRepository.removeMetaObjectByUUID(objectToDrop.getUuid(), authToken);
                break;
            }
            case PAGE:
            case QUERYVISUALIZATION: {
                str.addAll(DropDefault.drop(ctx, objectToDrop, dropRule, authToken));
                break;
            }
            default: {
                str.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
                break;
            }
        }
        return str;
    }
    
    public static void checkPermissionToDrop(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.drop, authToken, true);
    }
    
    private static String flagDropped(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        if (metaObject.getMetaInfoStatus().isDropped()) {
            return "";
        }
        DropMetaObject.metaDataRepository.removeStatusInfo(metaObject.getUuid(), HSecurityManager.TOKEN);
        metaObject.getMetaInfoStatus().setDropped(true);
        removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
        DropMetaObject.metaDataRepository.updateMetaObject(metaObject, authToken);
        invalidateRelatedObjects(metaObject, authToken);
        if (metaObject.type == EntityType.CQ) {
            ((MetaInfo.CQ)metaObject).removeClass();
        }
        if (metaObject.type == EntityType.TYPE) {
            if (((MetaInfo.Type)metaObject).generated) {
                if (!metaObject.getMetaInfoStatus().isGenerated()) {
                    ((MetaInfo.Type)metaObject).removeClass();
                    return metaObject.type + " " + metaObject.name + " dropped successfully\n";
                }
                ((MetaInfo.Type)metaObject).removeClass();
            }
        }
        else if (metaObject.type.equals(EntityType.HDSTORE)) {
            ((MetaInfo.HDStore)metaObject).removeGeneratedClasses();
            return metaObject.type + " " + metaObject.name + " dropped successfully\n";
        }
        if (metaObject.type == EntityType.TYPE && !((MetaInfo.Type)metaObject).generated) {
            return "";
        }
        if (!metaObject.getMetaInfoStatus().isAnonymous()) {
            if (metaObject.type == EntityType.CACHE) {
                final MetaInfo.Cache cacheMetaObject = (MetaInfo.Cache)metaObject;
                if (cacheMetaObject.adapterClassName == null) {
                    return "EVENTTABLE " + metaObject.name + " dropped successfully\n";
                }
            }
            return metaObject.type + " " + metaObject.name + " dropped successfully\n";
        }
        return "";
    }
    
    public static void removeFromRecentVersion(final String namespaceName, final UUID objectName) {
        HazelcastSingleton.get().getSet("#" + namespaceName).remove((Object)objectName);
    }
    
    public static void addToRecentVersion(final String namespaceName, final UUID objectName) {
        HazelcastSingleton.get().getSet("#" + namespaceName).add((Object)objectName);
    }
    
    private static void invalidateRelatedObjects(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        if (metaObject == null) {
            return;
        }
        final Set<UUID> relatedObject = metaObject.getReverseIndexObjectDependencies();
        if (relatedObject == null || relatedObject.isEmpty()) {
            return;
        }
        for (final UUID uuid : relatedObject) {
            final MetaInfo.MetaObject relatedMetaObject = getMetaObjectByUUID(uuid, authToken);
            if (relatedMetaObject != null) {
                if (!relatedMetaObject.getMetaInfoStatus().isDropped() && relatedMetaObject.getMetaInfoStatus().isValid()) {
                    relatedMetaObject.getMetaInfoStatus().setValid(false);
                    DropMetaObject.metaDataRepository.updateMetaObject(relatedMetaObject, authToken);
                    if (DropMetaObject.logger.isDebugEnabled()) {
                        DropMetaObject.logger.debug((Object)("Related Object " + relatedMetaObject.name));
                    }
                }
                invalidateRelatedObjects(relatedMetaObject, authToken);
            }
            else {
                if (!DropMetaObject.logger.isInfoEnabled()) {
                    continue;
                }
                DropMetaObject.logger.info((Object)(metaObject.getFullName() + " is already dropped."));
            }
        }
    }
    
    private static void updateCurrentAppAndFlowPointers(final Context context) throws MetaDataRepositoryException {
        if (context == null) {
            return;
        }
        if (context.getCurApp() != null) {
            context.setCurApp(context.get(context.getCurApp().nsName + "." + context.getCurApp().name, EntityType.APPLICATION));
        }
        if (context.getCurFlow() != null) {
            context.setCurFlow(context.get(context.getCurFlow().nsName + "." + context.getCurFlow().name, EntityType.FLOW));
        }
    }
    
    private static List<MetaInfo.Flow> getAppsContainMetaObject(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        final List<MetaInfo.Flow> appsContainThisMetaObject = new ArrayList<MetaInfo.Flow>();
        if (metaObject.type == EntityType.APPLICATION) {
            appsContainThisMetaObject.add((MetaInfo.Flow)metaObject);
        }
        else if (metaObject.type == EntityType.NAMESPACE) {
            final List<MetaInfo.MetaObject> allObjectsInNamespace = getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
            if (allObjectsInNamespace != null) {
                for (final MetaInfo.MetaObject objectInNamespace : allObjectsInNamespace) {
                    if (objectInNamespace != null && objectInNamespace.type == EntityType.APPLICATION) {
                        appsContainThisMetaObject.add((MetaInfo.Flow)objectInNamespace);
                    }
                }
            }
        }
        else {
            final Set<UUID> dependencyList = metaObject.getReverseIndexObjectDependencies();
            for (final UUID dependency : dependencyList) {
                final MetaInfo.MetaObject depenedentObject = getMetaObjectByUUID(dependency, authToken);
                if (depenedentObject != null) {
                    if (depenedentObject.type != EntityType.APPLICATION) {
                        continue;
                    }
                    appsContainThisMetaObject.add((MetaInfo.Flow)depenedentObject);
                }
                else {
                    if (!DropMetaObject.logger.isInfoEnabled()) {
                        continue;
                    }
                    DropMetaObject.logger.info((Object)(metaObject.getFullName() + " is already dropped."));
                }
            }
        }
        return appsContainThisMetaObject;
    }
    
    private static boolean isMetaObjectDeployed(final MetaInfo.MetaObject metaObject, final EntityType entityType, final AuthToken authToken) throws MetaDataRepositoryException {
        List<MetaInfo.Flow> appsContainMetaObject = new ArrayList<MetaInfo.Flow>();
        appsContainMetaObject = getAppsContainMetaObject(metaObject, authToken);
        if (DropMetaObject.logger.isDebugEnabled()) {
            DropMetaObject.logger.debug((Object)("checkIfAppDeployed() => appsContainMetaObject " + appsContainMetaObject));
        }
        for (final MetaInfo.Flow app : appsContainMetaObject) {
            if (DropMetaObject.logger.isDebugEnabled()) {
                DropMetaObject.logger.debug((Object)("-->" + app.name));
                DropMetaObject.logger.debug((Object)("-->" + app.deploymentPlan));
            }
            if (app.deploymentPlan != null) {
                if (metaObject.type == EntityType.APPLICATION) {
                    throw new RuntimeException("Cannot remove, " + metaObject.name + " is deployed!! (Undeploy application first)");
                }
                throw new RuntimeException("Cannot remove, app/flow " + app.name + " that uses " + metaObject.name + " is deployed!! (Undeploy application first)");
            }
        }
        return false;
    }
    
    private static void checkAndUndeploy(final MetaInfo.MetaObject metaObject, final EntityType entityType, final AuthToken authToken) {
    }
    
    private static MetaInfo.MetaObject getMetaObjectByUUID(final UUID metaObjectUUID, final AuthToken authToken) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject metaObject = DropMetaObject.metaDataRepository.getMetaObjectByUUID(metaObjectUUID, authToken);
        return metaObject;
    }
    
    static boolean checkIfTargetHasDependencies(final MetaInfo.MetaObject inputOutputMetaObject, final MetaInfo.MetaObject connectedMetaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        final Set<UUID> reverseIndexObjectDependencies = connectedMetaObject.getReverseIndexObjectDependencies();
        boolean hasSourceDependencies = false;
        if (reverseIndexObjectDependencies == null) {
            return hasSourceDependencies;
        }
        for (final UUID uuid : reverseIndexObjectDependencies) {
            final MetaInfo.MetaObject reverseObj = getMetaObjectByUUID(uuid, authToken);
            if (reverseObj == null) {
                continue;
            }
            if (reverseObj.getMetaInfoStatus().isDropped()) {
                continue;
            }
            if (reverseObj.equals(inputOutputMetaObject)) {
                continue;
            }
            hasSourceDependencies = true;
        }
        return hasSourceDependencies;
    }
    
    static {
        DropMetaObject.sharedObject = new EntityType[] { EntityType.WINDOW, EntityType.CACHE, EntityType.STREAM, EntityType.TYPE, EntityType.HDSTORE };
        DropMetaObject.securityManager = HSecurityManager.get();
        DropMetaObject.logger = Logger.getLogger((Class)DropMetaObject.class);
        DropMetaObject.metaDataRepository = MetadataRepository.getINSTANCE();
    }
    
    public enum DropRule
    {
        CASCADE, 
        FORCE, 
        ALL, 
        NONE;
    }
    
    public static class DropRole
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) {
            final MetaInfo.Role roleObject = (MetaInfo.Role)metaObject;
            final List<String> resultString = new ArrayList<String>();
            try {
                DropMetaObject.securityManager.removeRole(roleObject, authToken);
            }
            catch (SecurityException errorMessage) {
                if (errorMessage != null) {
                    resultString.add(errorMessage.getMessage());
                    return resultString;
                }
            }
            catch (Exception ex) {}
            return resultString;
        }
    }
    
    public static class DropUser
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) {
            final MetaInfo.User userObject = (MetaInfo.User)metaObject;
            final List<String> resultString = new ArrayList<String>();
            try {
                if (userObject == null) {
                    DropMetaObject.securityManager.removeUser(Tungsten.currUserMetaInfo.getName(), null, authToken);
                }
                else {
                    DropMetaObject.securityManager.removeUser(Tungsten.currUserMetaInfo.getName(), userObject.getUserId(), authToken);
                }
            }
            catch (SecurityException errorMessage) {
                throw errorMessage;
            }
            catch (Exception ex) {}
            return resultString;
        }
    }
    
    public static class DropCache
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.Cache cacheMetaObject = (MetaInfo.Cache)metaObject;
            List<String> resultString = new ArrayList<String>();
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(cacheMetaObject, resultString, authToken);
            }
            else {
                if (isMetaObjectDeployed(cacheMetaObject, EntityType.CACHE, authToken)) {
                    return resultString;
                }
                resultString = dropNow(cacheMetaObject, resultString, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.Cache cacheMetaObject, final List<String> resultString, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(cacheMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(cacheMetaObject, authToken));
            resultString.add(flagDropIfAnonymous(getMetaObjectByUUID(cacheMetaObject.typename, authToken), authToken));
            Cache.drop(cacheMetaObject);
            return resultString;
        }
    }
    
    public static class DropCQ
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)metaObject;
            List<String> resultString = new ArrayList<String>();
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(ctx, cqMetaObject, resultString, dropRule, authToken);
            }
            else {
                if (isMetaObjectDeployed(cqMetaObject, EntityType.CQ, authToken)) {
                    return resultString;
                }
                resultString = dropNow(ctx, cqMetaObject, resultString, dropRule, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final Context ctx, final MetaInfo.CQ cqMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(cqMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(cqMetaObject, authToken));
            for (final UUID ds : cqMetaObject.plan.getDataSources()) {
                final MetaInfo.MetaObject cqSubtaskMetaObject = getMetaObjectByUUID(ds, authToken);
                if (cqSubtaskMetaObject != null && cqSubtaskMetaObject.getMetaInfoStatus().isAnonymous() && !(cqSubtaskMetaObject instanceof MetaInfo.Stream) && !(cqSubtaskMetaObject instanceof MetaInfo.WAStoreView) && !ServerUpgradeUtility.isUpgrading) {
                    resultString.add(flagDropped(cqSubtaskMetaObject, authToken));
                }
            }
            if (cqMetaObject.stream != null) {
                final MetaInfo.MetaObject cqOutputStreamMetaObject = getMetaObjectByUUID(cqMetaObject.stream, authToken);
                if (cqOutputStreamMetaObject != null && cqOutputStreamMetaObject.getMetaInfoStatus().isAnonymous() && cqOutputStreamMetaObject instanceof MetaInfo.Stream) {
                    if (DropMetaObject.logger.isDebugEnabled()) {
                        DropMetaObject.logger.debug((Object)("AGAIN " + cqOutputStreamMetaObject.getName() + " " + cqOutputStreamMetaObject.getMetaInfoStatus()));
                    }
                    if (!ServerUpgradeUtility.isUpgrading) {
                        resultString.addAll(DropStream.drop(ctx, cqOutputStreamMetaObject, dropRule, authToken));
                    }
                }
            }
            return resultString;
        }
    }
    
    public static class DropSource
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            final MetaInfo.Source sourceMetaObject = (MetaInfo.Source)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(ctx, sourceMetaObject, resultString, dropRule, authToken);
            }
            else {
                if (isMetaObjectDeployed(sourceMetaObject, EntityType.SOURCE, authToken)) {
                    return resultString;
                }
                resultString = dropNow(ctx, sourceMetaObject, resultString, dropRule, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final Context ctx, final MetaInfo.Source sourceMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(sourceMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            final boolean isDependent = checkIfStreamIsDependent(sourceMetaObject, authToken);
            if (!isDependent) {
                if (!sourceMetaObject.coDependentObjects.isEmpty()) {
                    for (final UUID uuid : sourceMetaObject.coDependentObjects) {
                        final MetaInfo.MetaObject obj = getMetaObjectByUUID(uuid, authToken);
                        if (obj.getMetaInfoStatus().isGenerated() && obj.getType() == EntityType.STREAM) {
                            if (DropMetaObject.logger.isDebugEnabled()) {
                                DropMetaObject.logger.debug((Object)("OBJECT IS GENERATED AND STREAM" + obj.getFullName()));
                            }
                            DropStream.drop(ctx, obj, dropRule, authToken);
                        }
                        else if (obj.getMetaInfoStatus().isAnonymous()) {
                            if (DropMetaObject.logger.isDebugEnabled()) {
                                DropMetaObject.logger.debug((Object)(obj.getType() + " OBJECT IS ANONYMOUS " + obj.getFullName()));
                            }
                            flagDropIfAnonymous(obj, authToken);
                        }
                        else {
                            if (!DropMetaObject.logger.isDebugEnabled()) {
                                continue;
                            }
                            DropMetaObject.logger.debug((Object)("Error case for " + obj + " %% " + obj.getMetaInfoStatus() + " %% " + obj.getType()));
                        }
                    }
                    MetadataRepository.getINSTANCE().removeMetaObjectByUUID(sourceMetaObject.outputStream, HSecurityManager.TOKEN);
                }
                else {
                    resultString.add(DropMetaObject.flagDropIfGenerated(getMetaObjectByUUID(sourceMetaObject.outputStream, authToken), authToken));
                }
            }
            try {
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(sourceMetaObject.adapterClassName);
                final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
                proc.onDrop(sourceMetaObject);
            }
            catch (Exception ex) {
                DropMetaObject.logger.error((Object)("failed to create an instance of adapter : " + sourceMetaObject.adapterClassName), (Throwable)ex);
            }
            resultString.add(flagDropped(sourceMetaObject, authToken));
            return resultString;
        }
        
        private static boolean checkIfStreamIsDependent(final MetaInfo.Source sourceMetaObject, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.MetaObject outputStream = getMetaObjectByUUID(sourceMetaObject.outputStream, authToken);
            if (outputStream != null) {
                final Set<UUID> objectsUsingSource = outputStream.getReverseIndexObjectDependencies();
                for (final UUID uuid : objectsUsingSource) {
                    final MetaInfo.MetaObject reverseObj = getMetaObjectByUUID(uuid, authToken);
                    if (reverseObj.type != EntityType.SOURCE) {
                        continue;
                    }
                    if (reverseObj.type == EntityType.SOURCE && !reverseObj.equals(sourceMetaObject)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
    
    public static class DropStream
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            final MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(streamMetaObject, resultString, dropRule, authToken);
            }
            else {
                if (isMetaObjectDeployed(streamMetaObject, EntityType.STREAM, authToken)) {
                    return resultString;
                }
                resultString = dropNow(streamMetaObject, resultString, dropRule, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.Stream streamMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(streamMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            if (streamMetaObject.pset != null) {
                try {
                    if (HazelcastSingleton.isClientMember()) {
                        final RemoteCall deleteTopic_executor = KafkaStreamUtils.getDeleteTopicExecutor(streamMetaObject);
                        DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Object>)deleteTopic_executor);
                    }
                    else {
                        KafkaStreamUtils.deleteTopic(streamMetaObject);
                    }
                }
                catch (Exception e) {
                    if (dropRule != DropRule.FORCE) {
                        throw new RuntimeException("Failed to delete kafka topics associated with stream: " + streamMetaObject.getFullName() + ", Reason: " + e.getMessage(), e);
                    }
                }
            }
            resultString.add(flagDropped(streamMetaObject, authToken));
            final MetaInfo.MetaObject mObject = getMetaObjectByUUID(streamMetaObject.dataType, authToken);
            if (mObject != null) {
                flagDropIfAnonymous(mObject, authToken);
                DropMetaObject.flagDropIfGenerated(mObject, authToken);
            }
            return resultString;
        }
    }
    
    public static class DropTarget
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            final MetaInfo.Target targetMetaObject = (MetaInfo.Target)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(targetMetaObject, resultString, authToken);
            }
            else {
                if (isMetaObjectDeployed(targetMetaObject, EntityType.TARGET, authToken)) {
                    return resultString;
                }
                resultString = dropNow(targetMetaObject, resultString, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.Target targetMetaObject, final List<String> resultString, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(targetMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(targetMetaObject, authToken));
            try {
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(targetMetaObject.adapterClassName);
                final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
                proc.onDrop(targetMetaObject);
            }
            catch (Exception ex) {
                DropMetaObject.logger.error((Object)("failed to create an instance of adapter : " + targetMetaObject.adapterClassName), (Throwable)ex);
            }
            final MetaInfo.MetaObject inputStream = getMetaObjectByUUID(targetMetaObject.inputStream, authToken);
            if (!DropMetaObject.checkIfTargetHasDependencies(targetMetaObject, inputStream, authToken) && !inputStream.getMetaInfoStatus().isGenerated()) {
                resultString.add(flagDropIfAnonymous(inputStream, authToken));
            }
            return resultString;
        }
    }
    
    public static class DropType
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            final MetaInfo.Type typeMetaObject = (MetaInfo.Type)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(typeMetaObject, resultString, authToken);
            }
            else {
                if (isMetaObjectDeployed(typeMetaObject, EntityType.TYPE, authToken)) {
                    return resultString;
                }
                resultString = dropNow(typeMetaObject, resultString, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.Type typeMetaObject, final List<String> resultString, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(typeMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(typeMetaObject, authToken));
            return resultString;
        }
    }
    
    public static class DropHDStore
    {
        public static List<String> removeIndex(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) {
            if (HazelcastSingleton.isClientMember()) {
                try {
                    final MDClientOps clientOps = MDClientOps.getINSTANCE();
                    final String hdStoreName = metaObject.getFullName();
                    final List<String> result = clientOps.drop(hdStoreName, EntityType.HDSTORE, dropRule, authToken);
                    return result;
                }
                catch (Exception exception) {
                    Throwable cause;
                    for (cause = exception; cause.getCause() != null; cause = cause.getCause()) {}
                    throw new RuntimeException(cause);
                }
            }
            return null;
        }
        
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            if (HazelcastSingleton.isClientMember()) {
                try {
                    final MDClientOps clientOps = MDClientOps.getINSTANCE();
                    final String hdStoreName = metaObject.getFullName();
                    final List<String> result = clientOps.drop(hdStoreName, EntityType.HDSTORE, dropRule, authToken);
                    updateCurrentAppAndFlowPointers(ctx);
                    return result;
                }
                catch (Exception exception) {
                    Throwable cause;
                    for (cause = exception; cause.getCause() != null; cause = cause.getCause()) {}
                    throw new RuntimeException(cause);
                }
            }
            final MetaInfo.HDStore hdStoreObject = (MetaInfo.HDStore)metaObject;
            List<String> resultString = new ArrayList<String>();
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(hdStoreObject, resultString, authToken);
            }
            else {
                if (isMetaObjectDeployed(hdStoreObject, EntityType.HDSTORE, authToken)) {
                    DropMetaObject.logger.warn((Object)("HDStore " + hdStoreObject.getName() + " is deployed so not dropping it"));
                    return resultString;
                }
                resultString = dropNow(hdStoreObject, resultString, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.HDStore hdStoreObject, final List<String> resultString, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(hdStoreObject, authToken);
            }
            catch (Exception e) {
                DropMetaObject.logger.warn((Object)("Failed to drop hdstore with exception " + e.getMessage()));
                resultString.add(e.getMessage());
                return resultString;
            }
            HStore.drop(hdStoreObject);
            resultString.add(flagDropped(hdStoreObject, authToken));
            final MetaInfo.Type hdContextType = (MetaInfo.Type)getMetaObjectByUUID(hdStoreObject.contextType, authToken);
            if (hdContextType.getMetaInfoStatus().isAnonymous()) {
                resultString.add(flagDropped(hdContextType, authToken));
            }
            if (hdStoreObject.eventTypes != null) {
                for (final UUID hdEventUUID : hdStoreObject.eventTypes) {
                    final MetaInfo.Type hdEventType = (MetaInfo.Type)getMetaObjectByUUID(hdEventUUID, authToken);
                    if (hdEventType.getMetaInfoStatus().isAnonymous()) {
                        resultString.add(flagDropped(hdEventType, authToken));
                    }
                }
            }
            return resultString;
        }
    }
    
    public static class DropWindow
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            final MetaInfo.Window windowMetaObject = (MetaInfo.Window)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                resultString = dropNow(windowMetaObject, resultString, authToken);
            }
            else {
                if (isMetaObjectDeployed(windowMetaObject, EntityType.WINDOW, authToken)) {
                    return resultString;
                }
                resultString = dropNow(windowMetaObject, resultString, authToken);
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final MetaInfo.Window windowMetaObject, final List<String> resultString, final AuthToken authToken) throws MetaDataRepositoryException {
            try {
                DropMetaObject.checkPermissionToDrop(windowMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(windowMetaObject, authToken));
            return resultString;
        }
    }
    
    public static class DropApplication
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
                return resultString;
            }
            final MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                DropMetaObject.checkPermissionToDrop(applicationMetaObject, authToken);
                resultString = dropNow(ctx, applicationMetaObject, resultString, dropRule, authToken);
            }
            else {
                if (isMetaObjectDeployed(applicationMetaObject, EntityType.APPLICATION, authToken)) {
                    return resultString;
                }
                if (dropRule == DropRule.NONE && !applicationMetaObject.getAllObjects().isEmpty()) {
                    throw new RuntimeException("Cannot remove, " + applicationMetaObject.name + " has objects inside, use CASCADE.");
                }
                if (dropRule == DropRule.NONE) {
                    DropMetaObject.checkPermissionToDrop(applicationMetaObject, authToken);
                    resultString.add(flagDropped(applicationMetaObject, authToken));
                }
                else if (dropRule == DropRule.CASCADE) {
                    DropMetaObject.checkPermissionToDrop(applicationMetaObject, authToken);
                    if (!isPartOfApplicationOrFlowDeployed(applicationMetaObject, authToken)) {
                        return resultString;
                    }
                    resultString = dropNow(ctx, applicationMetaObject, resultString, dropRule, authToken);
                }
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final Context ctx, final MetaInfo.Flow applicationMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final Set<UUID> allObjects = new LinkedHashSet<UUID>();
            final Set<UUID> allAnonObjects = new LinkedHashSet<UUID>();
            final List<MetaInfo.PropertySet> propertySetList = new ArrayList<MetaInfo.PropertySet>();
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInApplication : applicationMetaObject.objects.entrySet()) {
                final Set<UUID> metaObjectUUIDList = metaObjectsInApplication.getValue();
                allObjects.addAll(metaObjectUUIDList);
            }
            for (final UUID uuid : allObjects) {
                final MetaInfo.MetaObject object = getMetaObjectByUUID(uuid, authToken);
                if (object != null && !object.getMetaInfoStatus().isAnonymous()) {
                    DropMetaObject.logger.warn((Object)("DROPPING" + object.getFullName()));
                    if (object instanceof MetaInfo.PropertySet) {
                        propertySetList.add((MetaInfo.PropertySet)object);
                    }
                    else {
                        resultString.addAll(DropMetaObject.dropObject(ctx, object, object.getType(), dropRule, authToken));
                    }
                }
                else {
                    allAnonObjects.add(uuid);
                }
            }
            for (final UUID uuid : allAnonObjects) {
                final MetaInfo.MetaObject object = getMetaObjectByUUID(uuid, authToken);
                if (object != null && object.getMetaInfoStatus().isAnonymous()) {
                    resultString.add(flagDropped(object, authToken));
                }
            }
            resultString.add(flagDropped(getMetaObjectByUUID(applicationMetaObject.uuid, authToken), authToken));
            if (propertySetList.size() > 0) {
                for (int i = 0; i < propertySetList.size(); ++i) {
                    DropMetaObject.dropObject(ctx, propertySetList.get(i), propertySetList.get(i).getType(), dropRule, authToken);
                }
            }
            try {
                final RemoteCall dropAppCheckpoint = new StatusDataStore.DropAppCheckpoint(applicationMetaObject.uuid);
                final Boolean success = DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Boolean>)dropAppCheckpoint);
                if (!success) {
                    resultString.add("Failed to remove application checkpoint for " + applicationMetaObject.name);
                }
            }
            catch (Exception e) {
                resultString.add("Could not remove application checkpoint for " + applicationMetaObject.name + ": " + e.getMessage());
            }
            return resultString;
        }
    }
    
    public static class DropFlow
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            List<String> resultString = new ArrayList<String>();
            if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
                return resultString;
            }
            final MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                DropMetaObject.checkPermissionToDrop(flowMetaObject, authToken);
                resultString = dropNow(ctx, flowMetaObject, resultString, dropRule, authToken);
            }
            else {
                if (isMetaObjectDeployed(flowMetaObject, EntityType.FLOW, authToken)) {
                    return resultString;
                }
                if (dropRule == DropRule.NONE) {
                    try {
                        DropMetaObject.checkPermissionToDrop(flowMetaObject, authToken);
                    }
                    catch (Exception e) {
                        resultString.add(e.getMessage());
                        return resultString;
                    }
                    resultString.add(flagDropped(flowMetaObject, authToken));
                }
                else if (dropRule == DropRule.CASCADE) {
                    resultString = dropNow(ctx, flowMetaObject, resultString, dropRule, authToken);
                }
            }
            updateCurrentAppAndFlowPointers(ctx);
            return resultString;
        }
        
        private static List<String> dropNow(final Context ctx, final MetaInfo.Flow flowMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInFlow : flowMetaObject.objects.entrySet()) {
                final Set<UUID> metaObjectUUIDList = metaObjectsInFlow.getValue();
                resultString.addAll(flagDropped(ctx, metaObjectsInFlow.getKey(), metaObjectUUIDList, dropRule, authToken));
            }
            try {
                DropMetaObject.checkPermissionToDrop(flowMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(flowMetaObject, authToken));
            return resultString;
        }
    }
    
    public static class DropNamespace
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final List<String> resultString = new ArrayList<String>();
            if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
                return resultString;
            }
            final MetaInfo.Namespace namespaceMetaObject = (MetaInfo.Namespace)metaObject;
            if (dropRule == DropRule.FORCE || dropRule == DropRule.ALL) {
                return dropNow(ctx, metaObject, namespaceMetaObject, resultString, dropRule, authToken);
            }
            if (isMetaObjectDeployed(namespaceMetaObject, EntityType.NAMESPACE, authToken)) {
                return resultString;
            }
            return dropNow(ctx, metaObject, namespaceMetaObject, resultString, dropRule, authToken);
        }
        
        private static List<String> dropNow(final Context ctx, final MetaInfo.MetaObject metaObject, final MetaInfo.Namespace namespaceMetaObject, final List<String> resultString, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            if (DropMetaObject.isNamespaceEmpty(namespaceMetaObject.name, authToken)) {
                try {
                    DropMetaObject.checkPermissionToDrop(namespaceMetaObject, authToken);
                }
                catch (Exception e) {
                    resultString.add(e.getMessage());
                    return resultString;
                }
                resultString.add(flagDropped(namespaceMetaObject, authToken));
                final List<MetaInfo.MetaObject> allObjectsInNamespace = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(namespaceMetaObject.name, authToken, false);
                if (allObjectsInNamespace != null) {
                    for (final MetaInfo.MetaObject metaObjectInNamespace : allObjectsInNamespace) {
                        if (metaObjectInNamespace == null) {
                            continue;
                        }
                        if (metaObjectInNamespace.type != EntityType.ROLE) {
                            continue;
                        }
                        DropRole.drop(ctx, metaObjectInNamespace, dropRule, authToken);
                    }
                }
            }
            else {
                if (dropRule == DropRule.NONE) {
                    throw new Warning("Cannot drop namespace without cascade!");
                }
                final Set<MetaInfo.Flow> applicationList = DropMetaObject.getApplicationByNamespaceAndAuth(namespaceMetaObject.name, authToken, false);
                if (applicationList != null) {
                    for (final MetaInfo.Flow flow : applicationList) {
                        if (!flow.getMetaInfoStatus().isAnonymous()) {
                            resultString.addAll(DropApplication.drop(ctx, flow, dropRule, authToken));
                        }
                    }
                }
                List<MetaInfo.MetaObject> allObjectsInNamespace2 = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
                if (dropRule != DropRule.CASCADE && dropRule != DropRule.ALL && dropRule != DropRule.FORCE) {
                    resultString.add("There are objects in namespace " + metaObject.name + ". Use CASCADE\n");
                }
                else {
                    dropAllQueries(ctx, resultString, allObjectsInNamespace2, authToken);
                    allObjectsInNamespace2 = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
                    if (allObjectsInNamespace2 != null) {
                        for (final MetaInfo.MetaObject metaObjectInNamespace2 : allObjectsInNamespace2) {
                            if (metaObjectInNamespace2 == null) {
                                continue;
                            }
                            if (metaObjectInNamespace2.type == EntityType.ROLE) {
                                continue;
                            }
                            try {
                                DropMetaObject.checkPermissionToDrop(metaObjectInNamespace2, authToken);
                            }
                            catch (Exception e2) {
                                resultString.add(e2.getMessage());
                                continue;
                            }
                            resultString.addAll(DropMetaObject.dropObject(ctx, metaObjectInNamespace2, metaObjectInNamespace2.type, dropRule, authToken));
                        }
                    }
                    try {
                        DropMetaObject.checkPermissionToDrop(namespaceMetaObject, authToken);
                    }
                    catch (Exception e3) {
                        resultString.add(e3.getMessage());
                        return resultString;
                    }
                    resultString.add(flagDropped(namespaceMetaObject, authToken));
                }
                if (allObjectsInNamespace2 != null) {
                    for (final MetaInfo.MetaObject metaObjectInNamespace2 : allObjectsInNamespace2) {
                        if (metaObjectInNamespace2 == null) {
                            continue;
                        }
                        if (metaObjectInNamespace2.type != EntityType.ROLE) {
                            continue;
                        }
                        DropRole.drop(ctx, metaObjectInNamespace2, dropRule, authToken);
                    }
                }
            }
            updateCurrentAppAndFlowPointers(ctx);
            HazelcastSingleton.get().getSet("#" + namespaceMetaObject.name).destroy();
            return resultString;
        }
        
        private static void dropAllQueries(final Context context, final List<String> resultString, final List<MetaInfo.MetaObject> allObjectsInNamespace, final AuthToken authToken) throws MetaDataRepositoryException {
            if (allObjectsInNamespace == null) {
                return;
            }
            for (final MetaInfo.MetaObject metaObject : allObjectsInNamespace) {
                if (metaObject != null && metaObject.getType() == EntityType.QUERY) {
                    final MetaInfo.Query queryMetaObject = (MetaInfo.Query)metaObject;
                    if (queryMetaObject.appUUID == null) {
                        continue;
                    }
                    final MetaInfo.MetaObject obj;
                    if ((obj = getMetaObjectByUUID(queryMetaObject.appUUID, authToken)) != null) {
                        if (isMetaObjectDeployed(obj, EntityType.APPLICATION, authToken)) {
                            throw new RuntimeException("Cannot remove, " + queryMetaObject.name + " is deployed!! (Stop query first)");
                        }
                        continue;
                    }
                    else {
                        if (!DropMetaObject.logger.isInfoEnabled()) {
                            continue;
                        }
                        DropMetaObject.logger.info((Object)(metaObject + " has null uuid for application"));
                    }
                }
            }
            for (final MetaInfo.MetaObject metaObject : allObjectsInNamespace) {
                if (metaObject != null && metaObject.getType() == EntityType.QUERY) {
                    context.deleteQuery((MetaInfo.Query)metaObject, authToken);
                    resultString.add(metaObject.type + " " + metaObject.name + " dropped successfully\n");
                }
            }
        }
    }
    
    public static class DropVisualization
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.Visualization visualizationMetaObject = (MetaInfo.Visualization)metaObject;
            final List<String> resultString = new ArrayList<String>();
            try {
                DropMetaObject.checkPermissionToDrop(visualizationMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(visualizationMetaObject, authToken));
            return resultString;
        }
    }
    
    public static class DropDG
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final List<String> resultString = new ArrayList<String>();
            if (dropRule == DropRule.CASCADE || dropRule == DropRule.FORCE) {
                throw new Warning("Unsupported option '" + dropRule.toString() + "' for drop dg.");
            }
            if (metaObject.type == EntityType.DG && EntityType.DG.isNotVersionable()) {
                final MetaInfo.DeploymentGroup dd = (MetaInfo.DeploymentGroup)metaObject;
                DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.uuid, authToken);
                DropMetaObject.metaDataRepository.removeDGInfoFromServer(metaObject, authToken);
                resultString.add(metaObject.type + " " + metaObject.name + " dropped successfully\n");
            }
            return resultString;
        }
    }
    
    public static class DropSubscription
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.Target subscriptionMetaObject = (MetaInfo.Target)metaObject;
            if (!subscriptionMetaObject.isSubscription()) {
                return null;
            }
            final List<String> resultString = new ArrayList<String>();
            try {
                DropMetaObject.checkPermissionToDrop(subscriptionMetaObject, authToken);
            }
            catch (Exception e) {
                resultString.add(e.getMessage());
                return resultString;
            }
            resultString.add(flagDropped(subscriptionMetaObject, authToken));
            return resultString;
        }
    }
    
    public static class DropDashboard
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.Dashboard dashboardMetaObject = (MetaInfo.Dashboard)metaObject;
            final List<String> pages = dashboardMetaObject.getPages();
            final List<String> resultString = new ArrayList<String>();
            DropMetaObject.checkPermissionToDrop(dashboardMetaObject, authToken);
            if (pages != null) {
                for (final String page : pages) {
                    MetaInfo.Page pageMetaObject;
                    if (page.indexOf(".") == -1) {
                        pageMetaObject = (MetaInfo.Page)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.PAGE, metaObject.getNsName(), Utility.splitName(page), null, authToken);
                    }
                    else {
                        pageMetaObject = (MetaInfo.Page)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(page), Utility.splitName(page), null, authToken);
                    }
                    if (pageMetaObject == null) {
                        continue;
                    }
                    try {
                        DropMetaObject.checkPermissionToDrop(pageMetaObject, authToken);
                    }
                    catch (SecurityException e) {
                        resultString.add(e.getMessage());
                        continue;
                    }
                    final List<String> qvs = pageMetaObject.getQueryVisualizations();
                    if (qvs == null) {
                        continue;
                    }
                    for (final String qv : qvs) {
                        MetaInfo.QueryVisualization queryVisualizationMetaObject = null;
                        if (qv.indexOf(".") == -1) {
                            queryVisualizationMetaObject = (MetaInfo.QueryVisualization)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, metaObject.getNsName(), Utility.splitName(qv), null, authToken);
                        }
                        else {
                            queryVisualizationMetaObject = (MetaInfo.QueryVisualization)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(qv), Utility.splitName(qv), null, authToken);
                        }
                        if (queryVisualizationMetaObject != null) {
                            try {
                                DropMetaObject.checkPermissionToDrop(queryVisualizationMetaObject, authToken);
                            }
                            catch (SecurityException e2) {
                                resultString.add(e2.getMessage());
                                continue;
                            }
                            DropMetaObject.metaDataRepository.removeMetaObjectByUUID(queryVisualizationMetaObject.getUuid(), authToken);
                            resultString.add(queryVisualizationMetaObject.type + " " + queryVisualizationMetaObject.getFullName() + " dropped successfully\n");
                        }
                    }
                }
                for (final String page : pages) {
                    DropMetaObject.metaDataRepository.removeMetaObjectByName(EntityType.PAGE, dashboardMetaObject.getNsName(), page, null, authToken);
                    resultString.add(EntityType.PAGE + " " + page + " dropped successfully\n");
                }
            }
            if (dashboardMetaObject != null) {
                DropMetaObject.metaDataRepository.removeMetaObjectByUUID(dashboardMetaObject.getUuid(), authToken);
                resultString.add(dashboardMetaObject.type + " " + dashboardMetaObject.name + " dropped successfully\n");
            }
            return resultString;
        }
    }
    
    public static class DropPropertySet
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.PropertySet propertySetMetaObject = (MetaInfo.PropertySet)metaObject;
            final List<String> resultString = new ArrayList<String>();
            final boolean isDeployed = isMetaObjectDeployed(propertySetMetaObject, EntityType.PROPERTYSET, authToken);
            if (isDeployed) {
                return resultString;
            }
            if (propertySetMetaObject.getReverseIndexObjectDependencies() != null && !propertySetMetaObject.getReverseIndexObjectDependencies().isEmpty()) {
                for (final UUID userUUID : propertySetMetaObject.getReverseIndexObjectDependencies()) {
                    final MetaInfo.MetaObject dependentObject = getMetaObjectByUUID(userUUID, authToken);
                    if (dependentObject != null && dependentObject.getType() == EntityType.USER && dependentObject instanceof MetaInfo.User) {
                        final MetaInfo.User userMetaObject = (MetaInfo.User)dependentObject;
                        if (userMetaObject != null) {
                            throw new RuntimeException("User is using propertyset " + metaObject.getFullName() + " cannot drop.");
                        }
                        continue;
                    }
                }
            }
            DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
            resultString.add(propertySetMetaObject.getType() + " " + propertySetMetaObject.getFullName() + " dropped successfully\n");
            return resultString;
        }
    }
    
    public static class DropPropertyVariable
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final MetaInfo.PropertyVariable propertyVariableMetaObject = (MetaInfo.PropertyVariable)metaObject;
            final List<String> resultString = new ArrayList<String>();
            DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
            resultString.add(propertyVariableMetaObject.getType() + " " + propertyVariableMetaObject.getFullName() + " dropped successfully\n");
            return resultString;
        }
    }
    
    public static class DropDefault
    {
        public static List<String> drop(final Context ctx, final MetaInfo.MetaObject metaObject, final DropRule dropRule, final AuthToken authToken) throws MetaDataRepositoryException {
            final List<String> resultString = new ArrayList<String>();
            if (DropMetaObject.metaDataRepository.getMetaObjectByUUID(metaObject.getUuid(), authToken) != null) {
                DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
                resultString.add(metaObject.getType() + " " + metaObject.getFullName() + " dropped successfully\n");
            }
            return resultString;
        }
    }
}
