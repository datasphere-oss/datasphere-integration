package com.datasphere.metaRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.datasphere.exception.SecurityException;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ILock;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MessageListener;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.HazelcastIMapListener;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.compiler.Grammar;
import com.datasphere.runtime.compiler.Lexer;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class MetadataRepository implements MDRepository
{
    private final MDCache mdc;
    private final MetaDataDBOps mdDBOps;
    private final VersionManager versionManager;
    private final boolean doPersist;
    private static MetadataRepository INSTANCE;
    private static Logger logger;
    Comparator<UUID> timestampComparator;
    
    private MetadataRepository() {
        this.timestampComparator = new Comparator<UUID>() {
            @Override
            public int compare(final UUID o1, final UUID o2) {
                if (o1.time > o2.time) {
                    return 1;
                }
                return -1;
            }
        };
        this.mdc = MDCache.getInstance();
        this.versionManager = VersionManager.getInstance();
        final String persist = System.getProperty("com.datasphere.config.persist");
        if (persist != null) {
            if (!HazelcastSingleton.isClientMember() && persist.equalsIgnoreCase("true")) {
                this.doPersist = true;
                this.mdDBOps = MetaDataDBOps.getInstance();
            }
            else {
                this.doPersist = false;
                this.mdDBOps = null;
            }
        }
        else {
            this.doPersist = false;
            this.mdDBOps = null;
        }
    }
    
    @Override
    public void initialize() throws MetaDataRepositoryException {
        final HSecurityManager securityManager = HSecurityManager.get();
        if (this.doPersist) {
            if (HazelcastSingleton.get().getAtomicLong("#MDInitialized").get() == 1L) {
                return;
            }
            final ILock lock = HazelcastSingleton.get().getLock("MetaRepoLock");
            lock.lock();
            try {
                if (HazelcastSingleton.get().getAtomicLong("#MDInitialized").get() == 1L) {
                    return;
                }
                this.loadCacheFromDb();
                HazelcastSingleton.get().getAtomicLong("#MDInitialized").set(1L);
            }
            finally {
                lock.unlock();
            }
        }
    }
    
    private void loadCacheFromDb() throws MetaDataRepositoryException {
        final List<MetaInfo.MetaObject> allObjects = (List<MetaInfo.MetaObject>)this.mdDBOps.get(null, null, null, null, null, MDConstants.typeOfGet.BY_ALL);
        for (final MetaInfo.MetaObject metaObject : allObjects) {
            if (MetadataRepository.logger.isInfoEnabled()) {
                MetadataRepository.logger.info((Object)("Putting MetaObject with URI: " + metaObject.getUri() + " from Database"));
            }
            if (this.versionManager.put(metaObject)) {
                this.mdc.put(metaObject);
                final MetaInfo.StatusInfo status = new MetaInfo.StatusInfo(metaObject.getUuid(), MetaInfo.StatusInfo.Status.CREATED, metaObject.getType(), metaObject.getFullName());
                this.putStatusInfo(status, HSecurityManager.TOKEN);
                if (MetadataRepository.logger.isDebugEnabled()) {
                    MetadataRepository.logger.debug((Object)("Putting MetaObject with URI: " + metaObject.getUri() + " from Database"));
                }
                if (!MetadataRepository.logger.isDebugEnabled()) {
                    continue;
                }
                MetadataRepository.logger.debug((Object)metaObject.getUri());
            }
            else {
                MetadataRepository.logger.error((Object)"Version Manager couldn't put the object");
            }
        }
    }
    
    public static MetadataRepository getINSTANCE() {
        return MetadataRepository.INSTANCE;
    }
    
    public static void shutdown() {
        final MetadataRepository metadataRepository = getINSTANCE();
        final String persist = System.getProperty("com.datasphere.config.persist");
        assert persist != null;
        if (!HazelcastSingleton.isClientMember() && persist.equalsIgnoreCase("true")) {
            metadataRepository.shutDownMetaDataDB();
        }
        metadataRepository.shutDownMDCache();
    }
    
    public boolean shutDownMDCache() {
        return this.mdc.shutDown();
    }
    
    public void shutDownMetaDataDB() {
        this.mdDBOps.shutdown();
    }
    
    @Override
    public void putShowStream(final MetaInfo.ShowStream showStream, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't pass NULL parameters for Action:putShowStream(...) in Meta Data Repository", showStream, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().putShowStream(showStream, token);
        }
        else {
            this.mdc.put(showStream);
        }
    }
    
    @Override
    public void putStatusInfo(final MetaInfo.StatusInfo statusInfo, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't pass NULL parameters for Action:putStatusInfo(...) in Meta Data Repository", statusInfo, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().putStatusInfo(statusInfo, token);
        }
        else {
            this.mdc.put(statusInfo);
        }
    }
    
    @Override
    public void putDeploymentInfo(final Pair<UUID, UUID> deploymentInfo, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't pass NULL parameters for Action:putDeploymentInfo(...) in Meta Data Repository", deploymentInfo, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().putDeploymentInfo(deploymentInfo, token);
        }
        else {
            this.mdc.put(deploymentInfo);
        }
    }
    
    @Override
    public void putServer(final MetaInfo.Server server, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't pass NULL parameters for Action:putDeploymentInfo(...) in Meta Data Repository", server, token);
        this.mdc.put(server);
    }
    
    @Override
    public void putMetaObject(final MetaInfo.MetaObject metaObject, final AuthToken token) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().putMetaObject(metaObject, token);
        }
        else {
            if (metaObject.namespaceId.equals((Object)MetaInfo.GlobalUUID) && !metaObject.type.isGlobal() && !token.equals((Object)HSecurityManager.TOKEN)) {
                throw new SecurityException("Unable to create object " + metaObject + " in Meta Data Repository ");
            }
            if (!PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.create, token, true)) {
                throw new SecurityException("Oops, you don't have permission to store " + metaObject + " in Meta Data Repository ");
            }
            if (this.versionManager.put(metaObject)) {
                if (metaObject.getType().isStoreable() && this.doPersist && !metaObject.getMetaInfoStatus().isAdhoc()) {
                    final boolean stored = this.mdDBOps.store(metaObject);
                    if (stored) {
                        this.mdc.put(metaObject);
                    }
                }
                else {
                    this.mdc.put(metaObject);
                }
                this.addToRecentVersion(metaObject.nsName, metaObject.uuid);
            }
            else if (MetadataRepository.logger.isDebugEnabled()) {
                MetadataRepository.logger.debug((Object)"Some unexpected problem with Version Manager");
            }
        }
    }
    
    @Override
    public MetaInfo.StatusInfo getStatusInfo(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Either UUID or Auth Token is NULL", uuid, token);
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getStatusInfo(uuid, token);
        }
        return (MetaInfo.StatusInfo)this.mdc.get(null, uuid, null, "status", null, MDConstants.typeOfGet.BY_UUID);
    }
    
    @Override
    public Collection<UUID> getServersForDeployment(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Either UUID or Auth Token is NULL", uuid, token);
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getServersForDeployment(uuid, token);
        }
        return (Collection<UUID>)this.mdc.get(null, uuid, null, "serversForDeployment", null, MDConstants.typeOfGet.BY_UUID);
    }
    
    @Override
    public MetaInfo.MetaObject getServer(final String name, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't get Server information when Server Name or Authentication Token is NULL\nServer Name: " + name + "\nToken: " + token, name, token);
        Set<MetaInfo.Server> allServers;
        if (HazelcastSingleton.isClientMember()) {
            allServers = (Set<MetaInfo.Server>)MDClientOps.getINSTANCE().getByEntityType(EntityType.SERVER, token);
        }
        else {
            allServers = (Set<MetaInfo.Server>)this.getByEntityType(EntityType.SERVER, token);
        }
        for (final MetaInfo.MetaObject candidate : allServers) {
            if (name.equalsIgnoreCase(candidate.name)) {
                return candidate;
            }
        }
        return null;
    }
    
    @Override
    public Integer getMaxClassId() {
        return (this.mdDBOps == null) ? -1 : this.mdDBOps.loadMaxClassId();
    }
    
    @Override
    public MetaInfo.MetaObject getMetaObjectByUUID(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Either UUID or Auth Token is NULL", uuid, token);
        if (HazelcastSingleton.isClientMember()) {
            final MDClientOps mdClientOps = MDClientOps.getINSTANCE();
            return mdClientOps.getMetaObjectByUUID(uuid, token);
        }
        final MetaInfo.MetaObject result = (MetaInfo.MetaObject)this.mdc.get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_UUID);
        if (result != null) {
            if (result instanceof MetaInfo.MetaObject) {
                if (!PermissionUtility.checkPermission(result, ObjectPermission.Action.read, token, true)) {
                    return null;
                }
            }
            else {
                MetadataRepository.logger.error((Object)("Unexpected Object type : " + result.getClass()));
            }
        }
        return result;
    }
    
    @Override
    public MetaInfo.MetaObject getMetaObjectByName(final EntityType entityType, final String namespace, final String name, final Integer version, final AuthToken token) throws MetaDataRepositoryException, SecurityException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getMetaObjectByName(entityType, namespace, name, version, token);
        }
        if (entityType == null) {
            MDConstants.checkNullParams("Unable to get MetaObject by Name, required fields: Name of Object  and Authentication TokenName of the Object: " + name + ", Auth Token: " + name, token);
            final MetaInfo.MetaObject object = this.getByUnqualifiedName(null, name, null, token);
            if (object == null) {
                return null;
            }
            return object.getMetaInfoStatus().isDropped() ? null : object;
        }
        else {
            MDConstants.checkNullParams("Unable to get MetaObject by Name, required fields: Entity Type, Namespace, Name of Object  and Authentication TokenEntity Type: " + entityType + ", Namespace : " + namespace + ", Name of the Object: " + name + ", Auth Token: " + token, namespace, name, token);
            final MetaInfo.MetaObject object = this.getByFullyQualifiedName(entityType, namespace, name, null, token);
            if (object == null) {
                return null;
            }
            return object.getMetaInfoStatus().isDropped() ? null : object;
        }
    }
    
    private MetaInfo.MetaObject getByUnqualifiedName(final String namespace, final String name, final Integer version, final AuthToken token) throws MetaDataRepositoryException {
        final Set<MetaInfo.Server> allServers = (Set<MetaInfo.Server>)this.getByEntityType(EntityType.SERVER, token);
        for (final MetaInfo.MetaObject candidate : allServers) {
            if (name.equalsIgnoreCase(candidate.name)) {
                return candidate;
            }
        }
        final Set<MetaInfo.MetaObject> allApps = (Set<MetaInfo.MetaObject>)this.getByEntityType(EntityType.APPLICATION, token);
        for (final MetaInfo.MetaObject application : allApps) {
            final EntityType[] array;
            final EntityType[] orderedTypes = array = new EntityType[] { EntityType.APPLICATION, EntityType.FLOW, EntityType.SERVER, EntityType.SOURCE, EntityType.STREAM, EntityType.CQ, EntityType.TARGET, EntityType.HDSTORE, EntityType.WINDOW };
            for (final EntityType et : array) {
                final MetaInfo.MetaObject metaObject = this.getByFullyQualifiedName(et, application.getName(), name, null, token);
                if (metaObject != null) {
                    return metaObject;
                }
            }
        }
        for (final MetaInfo.MetaObject metaObject2 : allApps) {
            final MetaInfo.MetaObject candidate2 = this.getMetaObjectByUUID(metaObject2.getUuid(), token);
            if (name.equalsIgnoreCase(candidate2.name)) {
                return candidate2;
            }
        }
        final Set<MetaInfo.MetaObject> allUsers = (Set<MetaInfo.MetaObject>)this.getByEntityType(EntityType.USER, token);
        final Iterator<MetaInfo.MetaObject> iterator4 = allUsers.iterator();
        while (iterator4.hasNext()) {
            final MetaInfo.MetaObject candidate2 = iterator4.next();
            if (candidate2 != null && name.equalsIgnoreCase(candidate2.name)) {
                return candidate2;
            }
        }
        final Set<MetaInfo.Role> allRoles = (Set<MetaInfo.Role>)this.getByEntityType(EntityType.ROLE, token);
        for (final MetaInfo.MetaObject candidate3 : allRoles) {
            if (name.equalsIgnoreCase(candidate3.name)) {
                return candidate3;
            }
        }
        return null;
    }
    
    private MetaInfo.MetaObject getByFullyQualifiedName(final EntityType eType, final String namespace, final String name, Integer version, final AuthToken token) throws MetaDataRepositoryException {
        MetaInfo.MetaObject result;
        if (version == null) {
            version = this.versionManager.getLatestVersionOfMetaObject(eType, namespace, name);
            if (MetadataRepository.logger.isDebugEnabled()) {
                MetadataRepository.logger.debug((Object)("Version: " + version + " for " + name + " in namespace: " + namespace));
            }
            if (version != null) {
                result = (MetaInfo.MetaObject)this.mdc.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
            }
            else {
                if (MetadataRepository.logger.isDebugEnabled()) {
                    MetadataRepository.logger.debug((Object)"Version doesn't exist. Implies a corresponding Object will not be found!!");
                }
                result = null;
            }
        }
        else {
            result = (MetaInfo.MetaObject)this.mdc.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
        }
        if (result == null && this.doPersist) {
            assert version != null;
            result = (MetaInfo.MetaObject)this.mdDBOps.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
            if (result != null) {
                this.versionManager.put(result);
                this.mdc.put(result);
            }
        }
        if (result != null && !PermissionUtility.checkPermission(result, ObjectPermission.Action.read, token, false)) {
            return null;
        }
        return result;
    }
    
    @Override
    public Set<?> getByEntityType(final EntityType eType, final AuthToken token) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getByEntityType(eType, token);
        }
        Set<String> resultStrings = null;
        Set<MetaInfo.MetaObject> resultSet = new HashSet<MetaInfo.MetaObject>();
        Set<MetaInfo.MetaObject> cacheResult = null;
        if (eType == EntityType.SERVER) {
            cacheResult = (Set<MetaInfo.MetaObject>)this.mdc.get(eType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
        }
        else {
            resultStrings = (Set<String>)this.mdc.get(eType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
            if (resultStrings == null && this.doPersist) {
                cacheResult = (Set<MetaInfo.MetaObject>)this.mdDBOps.get(eType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
                if (cacheResult != null) {
                    for (final MetaInfo.MetaObject metaObject : cacheResult) {
                        this.versionManager.put(metaObject);
                        this.mdc.put(metaObject);
                    }
                }
            }
            else {
                cacheResult = new HashSet<MetaInfo.MetaObject>();
                for (final String muri : resultStrings) {
                    MetaInfo.MetaObject mObj = null;
                    final String[] nameComps = muri.split(":");
                    mObj = this.getByFullyQualifiedName(EntityType.valueOf(nameComps[1]), nameComps[0], nameComps[2], Integer.valueOf(nameComps[3]), token);
                    if (mObj != null) {
                        cacheResult.add(mObj);
                    }
                }
            }
        }
        if (cacheResult != null) {
            for (final MetaInfo.MetaObject metaObject : cacheResult) {
                if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false)) {
                    if (resultSet == null) {
                        resultSet = new HashSet<MetaInfo.MetaObject>();
                    }
                    resultSet.add(metaObject);
                }
            }
        }
        this.removeDroppedObject(resultSet);
        return resultSet;
    }
    
    @Override
    public Set<MetaInfo.MetaObject> getByNameSpace(final String namespace, final AuthToken token) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getByNameSpace(namespace, token);
        }
        Set<MetaInfo.MetaObject> cacheResult = null;
        Set<MetaInfo.MetaObject> resultSet = null;
        cacheResult = (Set<MetaInfo.MetaObject>)this.mdc.get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
        if (cacheResult == null && this.doPersist) {
            cacheResult = (Set<MetaInfo.MetaObject>)this.mdDBOps.get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
            if (cacheResult != null) {
                for (final MetaInfo.MetaObject metaObject : cacheResult) {
                    this.versionManager.put(metaObject);
                    this.mdc.put(metaObject);
                }
            }
        }
        if (cacheResult != null) {
            for (final MetaInfo.MetaObject metaObject : cacheResult) {
                if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false)) {
                    if (resultSet == null) {
                        resultSet = new HashSet<MetaInfo.MetaObject>();
                    }
                    resultSet.add(metaObject);
                }
            }
        }
        return resultSet;
    }
    
    @Override
    public Set<?> getByEntityTypeInNameSpace(final EntityType eType, final String namespace, final AuthToken token) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getByEntityTypeInNameSpace(eType, namespace, token);
        }
        Set<MetaInfo.MetaObject> cacheResult = null;
        Set<MetaInfo.MetaObject> resultSet = null;
        cacheResult = (Set<MetaInfo.MetaObject>)this.mdc.get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE);
        if (cacheResult == null && this.doPersist) {
            cacheResult = (Set<MetaInfo.MetaObject>)this.mdDBOps.get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE);
            if (cacheResult != null) {
                for (final MetaInfo.MetaObject metaObject : cacheResult) {
                    this.versionManager.put(metaObject);
                    this.mdc.put(metaObject);
                }
            }
        }
        if (cacheResult != null) {
            for (final MetaInfo.MetaObject metaObject : cacheResult) {
                if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false)) {
                    if (resultSet == null) {
                        resultSet = new HashSet<MetaInfo.MetaObject>();
                    }
                    resultSet.add(metaObject);
                }
            }
        }
        return resultSet;
    }
    
    @Override
    public Set<?> getByEntityTypeInApplication(final EntityType entityType, final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject appMetaObject = this.getMetaObjectByName(EntityType.APPLICATION, namespace, name, null, token);
        if (appMetaObject != null) {
            final Set<UUID> uuidsOfObjectsInApp = ((MetaInfo.Flow)appMetaObject).getObjects(entityType);
            final Set<MetaInfo.MetaObject> metaObjectsInApp = new HashSet<MetaInfo.MetaObject>();
            for (final UUID uu : uuidsOfObjectsInApp) {
                final MetaInfo.MetaObject mm;
                if ((mm = this.getMetaObjectByUUID(uu, token)) != null) {
                    metaObjectsInApp.add(mm);
                }
            }
            return metaObjectsInApp;
        }
        return null;
    }
    
    @Override
    public MetaInfo.MetaObject revert(final EntityType eType, final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        final Integer lastestVersion = this.versionManager.getLatestVersionOfMetaObject(eType, namespace, name);
        MetaInfo.MetaObject metaObject = null;
        if (lastestVersion != null) {
            this.removeMetaObjectByName(eType, namespace, name, lastestVersion, token);
            metaObject = this.getMetaObjectByName(eType, namespace, name, null, token);
        }
        return metaObject;
    }
    
    @Override
    public void removeMetaObjectByName(final EntityType eType, final String namespace, final String name, final Integer version, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("One of the params for needed for removing Meta Object from MetaData Repository is NULL. \nEntityType: " + eType + ", Namespace: " + namespace + ", Name:" + name, eType, namespace, name, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().removeMetaObjectByName(eType, namespace, name, version, token);
        }
        else {
            final MetaInfo.MetaObject metaObject = this.getMetaObjectByName(eType, namespace, name, version, token);
            if (metaObject == null) {
                MetadataRepository.logger.debug((Object)("No MetaObject found for: " + name));
                return;
            }
            this.removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
            if (version == null) {
                final List<Integer> allVersions = this.versionManager.getAllVersionsOfMetaObject(eType, namespace, name);
                if (allVersions != null) {
                    for (final Integer currentVersion : allVersions) {
                        final Object result = this.mdc.remove(eType, null, namespace, name, currentVersion, MDConstants.typeOfRemove.BY_NAME);
                        if (result != null && this.doPersist) {
                            this.mdDBOps.remove(eType, null, namespace, name, currentVersion, MDConstants.typeOfRemove.BY_NAME);
                        }
                    }
                    this.versionManager.removeAllVersions(eType, namespace, name);
                }
            }
            else {
                this.versionManager.removeVersion(eType, namespace, name, version);
                this.mdc.remove(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME);
                if (this.doPersist) {
                    this.mdDBOps.remove(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME);
                }
            }
        }
    }
    
    @Override
    public void removeStatusInfo(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't remove Object with NULL UUID", uuid, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().removeStatusInfo(uuid, token);
        }
        else if (token.equals((Object)HSecurityManager.TOKEN)) {
            final MetaInfo.StatusInfo statusInfo = this.getStatusInfo(uuid, token);
            if (statusInfo != null) {
                this.mdc.remove(null, statusInfo.getOID(), null, "status", null, MDConstants.typeOfRemove.BY_UUID);
            }
            else if (MetadataRepository.logger.isDebugEnabled()) {
                MetadataRepository.logger.debug((Object)("No Status Info object found for: " + uuid));
            }
        }
        else {
            MetadataRepository.logger.error((Object)("User with token: " + token.toString() + " can't remove a StatusInfo Object"));
        }
    }
    
    @Override
    public void removeDeploymentInfo(final Pair<UUID, UUID> deploymentInfo, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't remove Object with NULL UUID", deploymentInfo, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().removeDeploymentInfo(deploymentInfo, token);
        }
        else if (token.equals((Object)HSecurityManager.TOKEN)) {
            final Collection<UUID> allUUIDs = this.getServersForDeployment(deploymentInfo.first, token);
            if (allUUIDs != null) {
                this.mdc.remove(null, deploymentInfo, null, null, null, MDConstants.typeOfRemove.BY_UUID);
            }
            else if (MetadataRepository.logger.isDebugEnabled()) {
                MetadataRepository.logger.debug((Object)("No Deployment Info object found for: " + deploymentInfo));
            }
        }
        else {
            MetadataRepository.logger.error((Object)("User with token: " + token.toString() + " can't remove a StatusInfo Object"));
        }
    }
    
    @Override
    public void removeMetaObjectByUUID(final UUID uuid, final AuthToken token) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Can't remove Object with NULL UUID", uuid, token);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().removeMetaObjectByUUID(uuid, token);
        }
        else {
            final MetaInfo.MetaObject metaObject = this.getMetaObjectByUUID(uuid, token);
            if (metaObject == null) {
                MetadataRepository.logger.debug((Object)("No MetaObject found for: " + uuid));
                return;
            }
            this.removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
            if (metaObject != null) {
                this.versionManager.removeVersion(metaObject.getType(), metaObject.getNsName(), metaObject.getName(), metaObject.version);
                this.mdc.remove(null, metaObject.getUuid(), null, null, null, MDConstants.typeOfRemove.BY_UUID);
                if (metaObject.getType().isStoreable() && this.doPersist) {
                    this.mdDBOps.remove(null, metaObject.getUuid(), null, null, null, MDConstants.typeOfRemove.BY_UUID);
                }
            }
            else if (MetadataRepository.logger.isDebugEnabled()) {
                MetadataRepository.logger.debug((Object)("No MetaObject found for: " + uuid));
            }
        }
    }
    
    @Override
    public void updateMetaObject(final MetaInfo.MetaObject object, AuthToken token) throws MetaDataRepositoryException {
        token = HSecurityManager.TOKEN;
        MDConstants.checkNullParams("Updated Object was NULL", object);
        if (HazelcastSingleton.isClientMember()) {
            MDClientOps.getINSTANCE().updateMetaObject(object, token);
        }
        else {
            final ObjectPermission.Action action = ObjectPermission.Action.update;
            if (PermissionUtility.checkPermission(object, action, token, true)) {
                this.mdc.update(object);
                if (this.doPersist && object.getType().isStoreable() && !object.getMetaInfoStatus().isAdhoc()) {
                    this.mdDBOps.store(object);
                }
            }
            else {
                MetadataRepository.logger.error((Object)("No permission to update: " + object.getFullName()));
            }
        }
    }
    
    @Override
    public String exportMetadataAsJson() {
        return this.mdc.exportMetadataAsJson();
    }
    
    @Override
    public void importMetadataFromJson(final String json, final Boolean replace) {
        this.mdc.importMetadataFromJson(json, replace);
    }
    
    @Override
    public Map dumpMaps(final AuthToken token) {
        final Map map = new HashMap();
        map.putAll((Map)this.mdc.getUuidToURL());
        map.putAll((Map)this.mdc.getUrlToMetaObject());
        map.putAll((Map)this.mdc.getStatus());
        final Set<String> nsStrings = (Set<String>)this.mdc.get(EntityType.NAMESPACE, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
        for (final String namespaceNameNS : nsStrings) {
            final MetaInfo.MetaObject namespaceName = (MetaInfo.MetaObject)this.mdc.getUrlToMetaObject().get((Object)namespaceNameNS);
            final ISet<UUID> set = HazelcastSingleton.get().getSet("#" + namespaceName.name);
            for (final UUID object : set) {
                final MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)this.mdc.get(null, object, null, null, null, MDConstants.typeOfGet.BY_UUID);
                if (metaObject != null) {
                    map.put(object, metaObject.type + " " + metaObject.name + " " + metaObject.version);
                }
            }
        }
        return map;
    }
    
    @Override
    public boolean removeDGInfoFromServer(final MetaInfo.MetaObject metaObject, final AuthToken authToken) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().removeDGInfoFromServer(metaObject, authToken);
        }
        boolean wasRemoved = false;
        if (Server.server.ServerInfo.deploymentGroupsIDs.contains(metaObject.getUuid())) {
            wasRemoved = Server.server.ServerInfo.deploymentGroupsIDs.remove(metaObject.getUuid());
            if (wasRemoved) {
                this.putServer(Server.server.ServerInfo, authToken);
                if (MetadataRepository.logger.isInfoEnabled()) {
                    MetadataRepository.logger.info((Object)("Removed Server" + Server.server.ServerInfo.getName() + " with UUID: " + Server.server.ServerInfo.getUuid() + "from DG: " + metaObject.getName() + " with UUID: " + metaObject.getUuid()));
                }
            }
        }
        return wasRemoved;
    }
    
    public void removeDroppedObject(final Set<MetaInfo.MetaObject> result) {
        final Iterator<MetaInfo.MetaObject> it = result.iterator();
        while (it.hasNext()) {
            final MetaInfo.MetaObject metaObject = it.next();
            if (metaObject.getMetaInfoStatus().isDropped()) {
                it.remove();
            }
        }
    }
    
    public List<MetaInfo.Flow> getAllApplications(final AuthToken token) {
        final List<MetaInfo.Flow> resultList = new ArrayList<MetaInfo.Flow>();
        try {
            final Set<MetaInfo.Flow> result = (Set<MetaInfo.Flow>)this.getByEntityType(EntityType.APPLICATION, token);
            if (result != null) {
                resultList.addAll(result);
            }
            Utility.removeDroppedFlow(resultList);
            Utility.removeAdhocNamedQuery(resultList);
        }
        catch (MetaDataRepositoryException e) {
            MetadataRepository.logger.error((Object)e.getMessage());
        }
        return resultList;
    }
    
    public List<MetaInfo.Namespace> getAllNamespaces(final AuthToken token) {
        Set<MetaInfo.Namespace> result = null;
        try {
            result = (Set<MetaInfo.Namespace>)this.getByEntityType(EntityType.NAMESPACE, token);
            return new ArrayList<MetaInfo.Namespace>(result);
        }
        catch (MetaDataRepositoryException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public Set<EntityType> getPlatformEntitiesByNamespace(final String nsName) {
        final Set<EntityType> entities = new HashSet<EntityType>();
        for (final EntityType tt : EntityType.values()) {
            if (nsName.equals("Global")) {
                if (tt.isGlobal() && !tt.isSystem()) {
                    entities.add(tt);
                }
            }
            else if (!tt.isSystem() && !tt.isGlobal()) {
                entities.add(tt);
            }
        }
        return entities;
    }
    
    private void addStreamDependants(final MetaInfo.Stream strm, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (strm.dataType != null) {
            if (appObjs.get(strm.dataType) != null) {
                return;
            }
            this.addMetaObject(strm.dataType, token, appObjs);
        }
    }
    
    private void addWindowDependants(final MetaInfo.Window window, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (window.stream != null) {
            if (appObjs.get(window.stream) != null) {
                return;
            }
            this.addMetaObject(window.stream, token, appObjs);
        }
    }
    
    private void addCacheDependants(final MetaInfo.Cache cache, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (cache.typename != null) {
            if (appObjs.get(cache.typename) != null) {
                return;
            }
            this.addMetaObject(cache.typename, token, appObjs);
        }
    }
    
    private void addHDStoreDependants(final MetaInfo.HDStore ws, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (ws.contextType != null && appObjs.get(ws.contextType) == null) {
            this.addMetaObject(ws.contextType, token, appObjs);
        }
        if (ws.eventTypes != null) {
            for (final UUID u : ws.eventTypes) {
                if (appObjs.get(u) == null) {
                    this.addMetaObject(u, token, appObjs);
                }
            }
        }
    }
    
    private void addSourceDependants(final MetaInfo.Source src, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (src.outputStream != null) {
            if (appObjs.get(src.outputStream) != null) {
                return;
            }
            this.addMetaObject(src.outputStream, token, appObjs);
        }
    }
    
    private void addTargetDependants(final MetaInfo.Target target, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (target.inputStream != null) {
            if (appObjs.get(target.inputStream) != null) {
                return;
            }
            this.addMetaObject(target.inputStream, token, appObjs);
        }
    }
    
    private void addMetaObject(final UUID objUUID, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        final MetaInfo.MetaObject obj = (MetaInfo.MetaObject)this.mdc.get(null, objUUID, null, null, null, MDConstants.typeOfGet.BY_UUID);
        if (obj == null) {
            return;
        }
        appObjs.put(obj.uuid, obj);
        switch (obj.type) {
            case SOURCE: {
                this.addSourceDependants((MetaInfo.Source)obj, token, appObjs);
                break;
            }
            case CQ: {
                this.addCQDependants((MetaInfo.CQ)obj, token, appObjs);
                break;
            }
            case TARGET: {
                this.addTargetDependants((MetaInfo.Target)obj, token, appObjs);
                break;
            }
            case FLOW: {
                this.addFlowDependants((MetaInfo.Flow)obj, token, appObjs);
                break;
            }
            case STREAM: {
                this.addStreamDependants((MetaInfo.Stream)obj, token, appObjs);
                break;
            }
            case WINDOW: {
                this.addWindowDependants((MetaInfo.Window)obj, token, appObjs);
                break;
            }
            case CACHE: {
                this.addCacheDependants((MetaInfo.Cache)obj, token, appObjs);
                break;
            }
            case HDSTORE: {
                this.addHDStoreDependants((MetaInfo.HDStore)obj, token, appObjs);
            }
        }
    }
    
    private void addFlowDependants(final MetaInfo.Flow flow, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        for (final EntityType cType : flow.getObjectTypes()) {
            final Set<UUID> flowObjs = flow.getObjects(cType);
            for (final UUID objectid : flowObjs) {
                if (appObjs.get(objectid) == null) {
                    this.addMetaObject(objectid, token, appObjs);
                }
            }
        }
    }
    
    private void addCQDependants(final MetaInfo.CQ cq, final AuthToken token, final Map<UUID, MetaInfo.MetaObject> appObjs) {
        if (cq.stream != null && appObjs.get(cq.stream) == null) {
            this.addMetaObject(cq.stream, token, appObjs);
        }
        final CQExecutionPlan plan = cq.plan;
        if (plan != null) {
            for (final UUID ds : plan.getDataSources()) {
                if (appObjs.get(ds) == null) {
                    this.addMetaObject(ds, token, appObjs);
                }
            }
        }
    }
    
    private Map<UUID, MetaInfo.MetaObject> getApplicationDependencyGraph(final String nsName, final String appName, final boolean deepTraverse, final AuthToken token) {
        final Map<UUID, MetaInfo.MetaObject> appObjects = new HashMap<UUID, MetaInfo.MetaObject>();
        MetaInfo.Flow app = null;
        try {
            app = (MetaInfo.Flow)this.getMetaObjectByName(EntityType.APPLICATION, nsName, appName, null, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            MetadataRepository.logger.error((Object)e.getMessage());
        }
        if (app != null) {
            for (final EntityType type : app.getObjectTypes()) {
                final Set<UUID> childObjs = app.getObjects(type);
                for (final UUID objUid : childObjs) {
                    if (appObjects.get(objUid) != null) {
                        continue;
                    }
                    if (!deepTraverse) {
                        MetaInfo.MetaObject obj = null;
                        try {
                            obj = this.getMetaObjectByUUID(objUid, HSecurityManager.TOKEN);
                        }
                        catch (MetaDataRepositoryException e2) {
                            MetadataRepository.logger.error((Object)e2.getMessage());
                        }
                        if (obj == null) {
                            continue;
                        }
                        appObjects.put(obj.uuid, obj);
                    }
                    else {
                        this.addMetaObject(objUid, token, appObjects);
                    }
                }
            }
        }
        return appObjects;
    }
    
    public String getApplicationTQL(final String nsName, final String appName, final boolean deepTraverse, final AuthToken token) throws MetaDataRepositoryException {
        final StringBuffer tql = new StringBuffer();
        final MetaInfo.Flow app = (MetaInfo.Flow)this.getMetaObjectByName(EntityType.APPLICATION, nsName, appName, null, token);
        if (app == null) {
            MetadataRepository.logger.warn((Object)("App " + nsName + "." + appName + " is null"));
            return tql.toString();
        }
        if (app.importStatements != null) {
            for (final String importStmt : app.importStatements) {
                tql.append(importStmt + "\n");
            }
            tql.append("\n");
        }
        try {
            final List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> ll = app.exportTQL(getINSTANCE(), token);
            String recovClause;
            if (app.recoveryPeriod > 0L) {
                recovClause = " RECOVERY " + new Interval(1000000L * app.recoveryPeriod).toHumanReadable() + " INTERVAL";
            }
            else {
                recovClause = "";
            }
            String encryptionClause;
            if (app.encrypted) {
                encryptionClause = " WITH ENCRYPTION";
            }
            else {
                encryptionClause = "";
            }
            tql.append("CREATE APPLICATION " + app.name + encryptionClause + recovClause);
            if (app.ehandlers != null && !app.ehandlers.isEmpty()) {
                tql.append(" EXCEPTIONHANDLER (");
                final Iterator<Map.Entry<String, Object>> it = app.ehandlers.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<String, Object> ehandler = it.next();
                    if (ehandler.getValue() != null) {
                        tql.append(String.format("%s: '%s'", ehandler.getKey(), ehandler.getValue()));
                        if (!it.hasNext()) {
                            continue;
                        }
                        tql.append(", ");
                    }
                }
                tql.append(") ");
            }
            tql.append(";\n\n");
            String currentFlow = null;
            final Set<String> seenFlow = new HashSet<String>();
            seenFlow.add(app.name);
            for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : ll) {
                if (pair.first.getSourceText() == null) {
                    continue;
                }
                boolean firstTimeSeeing = false;
                if (!seenFlow.contains(pair.second.name)) {
                    seenFlow.add(pair.second.name);
                    if (currentFlow != null) {
                        tql.append("END FLOW " + currentFlow + ";\n\n");
                    }
                    tql.append("CREATE FLOW " + pair.second.name + ";\n\n");
                    currentFlow = pair.second.name;
                    firstTimeSeeing = true;
                }
                if (!firstTimeSeeing) {
                    if (pair.second.type == EntityType.FLOW && currentFlow == null) {
                        tql.append("ALTER FLOW " + pair.second.name + ";\n\n");
                        currentFlow = pair.second.name;
                    }
                    else if (pair.second.type == EntityType.FLOW && !pair.second.name.equalsIgnoreCase(currentFlow)) {
                        tql.append("END FLOW " + currentFlow + ";\n\n");
                        tql.append("ALTER FLOW " + pair.second.name + ";\n\n");
                        currentFlow = pair.second.name;
                    }
                    if (pair.second.type == EntityType.APPLICATION) {
                        if (currentFlow != null) {
                            tql.append("END FLOW " + currentFlow + ";\n\n");
                        }
                        currentFlow = null;
                    }
                }
                this.printObject(tql, pair.first, token);
            }
            if (currentFlow != null && !currentFlow.isEmpty()) {
                tql.append("END FLOW " + currentFlow + ";\n\n");
            }
            tql.append("END APPLICATION " + app.name + ";\n\n");
        }
        catch (Exception e) {
            MetadataRepository.logger.error((Object)e);
        }
        return tql.toString();
    }
    
    private void printObject(final StringBuffer tql, final MetaInfo.MetaObject obj, final AuthToken token) throws MetaDataRepositoryException {
        final String temp = obj.getSourceText();
        if (temp != null && !temp.isEmpty()) {
            if (obj.type == EntityType.SOURCE) {
                final MetaInfo.Source sourceMetaObject = (MetaInfo.Source)obj;
                String sourceText = temp;
                try {
                    final List<Stmt> stmt = new Grammar(new Lexer(sourceText + ";")).parseStmt(false);
                    final CreateSourceOrTargetStmt sourceStmt = (CreateSourceOrTargetStmt)stmt.get(0);
                    String realOutput;
                    if (sourceStmt.parserOrFormatter == null) {
                        realOutput = Utility.createSourceStatementText(sourceMetaObject.getName(), sourceStmt.doReplace, sourceStmt.srcOrDest.getAdapterTypeName(), sourceStmt.srcOrDest.getVersion(), sourceStmt.srcOrDest.getProps(), null, null, null, sourceMetaObject.outputClauses);
                    }
                    else {
                        realOutput = Utility.createSourceStatementText(sourceMetaObject.getName(), sourceStmt.doReplace, sourceStmt.srcOrDest.getAdapterTypeName(), sourceStmt.srcOrDest.getVersion(), sourceStmt.srcOrDest.getProps(), sourceStmt.parserOrFormatter.getAdapterTypeName(), sourceStmt.parserOrFormatter.getVersion(), sourceStmt.parserOrFormatter.getProps(), sourceMetaObject.outputClauses);
                    }
                    sourceText = realOutput;
                }
                catch (Exception e) {
                    if (MetadataRepository.logger.isDebugEnabled()) {
                        MetadataRepository.logger.debug((Object)e.getMessage(), (Throwable)e);
                    }
                    sourceText = StringUtils.substringBefore(sourceText, "OUTPUT TO");
                    sourceText = Utility.appendOutputClause(sourceText, sourceMetaObject.outputClauses);
                }
                finally {
                    tql.append(sourceText).append(";\n\n");
                }
            }
            else {
                tql.append(temp).append(";\n\n");
            }
        }
    }
    
    private void drillDownTql(final StringBuffer tql, final AuthToken token, final MetaInfo.Flow app) throws MetaDataRepositoryException {
        final List<UUID> appObjects = app.getDependencies();
        Collections.sort(appObjects, this.timestampComparator);
        for (final UUID uuid : appObjects) {
            final MetaInfo.MetaObject obj = this.getMetaObjectByUUID(uuid, token);
            if (obj.type == EntityType.FLOW) {
                this.drillDownTql(tql, token, (MetaInfo.Flow)obj);
            }
            else {
                this.printObject(tql, obj, token);
            }
        }
    }
    
    public List<MetaInfo.MetaObject> getAllObjectsInApp(final String nsName, final String appName, final boolean deepTraverse, final AuthToken token) {
        final List<MetaInfo.MetaObject> result = new ArrayList<MetaInfo.MetaObject>();
        final Map<UUID, MetaInfo.MetaObject> objs = this.getApplicationDependencyGraph(nsName, appName, deepTraverse, token);
        for (final UUID uid : objs.keySet()) {
            result.add(objs.get(uid));
        }
        return result;
    }
    
    public List<MetaInfo.MetaObject> getAllObjectsInNamespace(final String nsName, final AuthToken token) {
        try {
            final Set<MetaInfo.MetaObject> result = this.getByNameSpace(nsName, token);
            final List<MetaInfo.MetaObject> metaObjectList = new ArrayList<MetaInfo.MetaObject>(result);
            Utility.removeDroppedObjects(metaObjectList);
            return metaObjectList;
        }
        catch (MetaDataRepositoryException e) {
            MetadataRepository.logger.error((Object)e.getMessage());
            return null;
        }
    }
    
    public List<MetaInfo.MetaObject> getAllVersionsObjectsInNamespace(final String nsName, final AuthToken token) {
        final List<MetaInfo.MetaObject> result = (List<MetaInfo.MetaObject>)this.mdc.get(null, null, nsName, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
        return result;
    }
    
    @Override
    public boolean contains(final MDConstants.typeOfRemove contains, final UUID uuid, final String url) {
        return false;
    }
    
    @Override
    public boolean clear(final boolean cleanDB) {
        if (cleanDB) {
            this.mdDBOps.clear();
        }
        this.versionManager.clear();
        return this.mdc.clear();
    }
    
    @Override
    public String registerListerForDeploymentInfo(final EntryListener<UUID, UUID> entryListener) {
        return this.mdc.registerListerForDeploymentInfo(entryListener);
    }
    
    @Override
    public String registerListenerForShowStream(final MessageListener<MetaInfo.ShowStream> messageListener) {
        return this.mdc.registerListenerForShowStream(messageListener);
    }
    
    @Override
    public String registerListenerForStatusInfo(final HazelcastIMapListener mapListener) {
        return this.mdc.registerListenerForStatusInfo(mapListener);
    }
    
    @Override
    public String registerListenerForMetaObject(final HazelcastIMapListener mapListener) {
        return this.mdc.registerListenerForMetaObject(mapListener);
    }
    
    @Override
    public String registerListenerForServer(final HazelcastIMapListener mapListener) {
        return this.mdc.registerListenerForServer(mapListener);
    }
    
    @Override
    public void removeListerForDeploymentInfo(final String regId) {
        this.mdc.removeListerForDeploymentInfo(regId);
    }
    
    @Override
    public void removeListenerForShowStream(final String regId) {
        this.mdc.removeListenerForShowStream(regId);
    }
    
    @Override
    public void removeListenerForStatusInfo(final String regId) {
        this.mdc.removeListenerForStatusInfo(regId);
    }
    
    @Override
    public void removeListenerForMetaObject(final String regId) {
        this.mdc.removeListenerForMetaObject(regId);
    }
    
    @Override
    public void removeListenerForServer(final String regId) {
        this.mdc.removeListenerForServer(regId);
    }
    
    public void addToRecentVersion(final String namespaceName, final UUID objectName) {
        HazelcastSingleton.get().getSet("#" + namespaceName).add((Object)objectName);
    }
    
    public void removeFromRecentVersion(final String namespaceName, final UUID objectName) {
        HazelcastSingleton.get().getSet("#" + namespaceName).remove((Object)objectName);
    }
    
    @Override
    public Position getAppCheckpointForFlow(final UUID sourceUUID, final AuthToken token) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            return MDClientOps.getINSTANCE().getAppCheckpointForFlow(sourceUUID, token);
        }
        return StatusDataStore.getInstance().getAppCheckpoint(sourceUUID);
    }
    
    static {
        MetadataRepository.INSTANCE = new MetadataRepository();
        MetadataRepository.logger = Logger.getLogger((Class)MetadataRepository.class);
    }
}
