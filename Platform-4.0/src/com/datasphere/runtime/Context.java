package com.datasphere.runtime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.datasphere.appmanager.AppManagerRequestClient;
import com.datasphere.appmanager.ChangeApplicationStateResponse;
import com.datasphere.appmanager.FlowUtil;
import com.datasphere.classloading.HDLoader;
import com.datasphere.distribution.HQueue;
import com.datasphere.drop.DropMetaObject;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.CompilationException;
import com.datasphere.exception.FatalException;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.datasphere.intf.QueryManager;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDConstants;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.Imports;
import com.datasphere.runtime.compiler.ObjectName;
import com.datasphere.runtime.compiler.select.ParamDesc;
import com.datasphere.runtime.compiler.select.RSFieldDesc;
import com.datasphere.runtime.compiler.stmts.GracePeriod;
import com.datasphere.runtime.compiler.stmts.GrantPermissionToStmt;
import com.datasphere.runtime.compiler.stmts.GrantRoleToStmt;
import com.datasphere.runtime.compiler.stmts.RecoveryDescription;
import com.datasphere.runtime.compiler.stmts.RevokePermissionFromStmt;
import com.datasphere.runtime.compiler.stmts.RevokeRoleFromStmt;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.compiler.stmts.UserProperty;
import com.datasphere.runtime.components.CQTask;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.MetaObjectPermissionChecker;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.exceptions.ErrorCode;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.meta.MetaInfoStatus;
import com.datasphere.runtime.meta.ProtectedNamespaces;
import com.datasphere.runtime.monitor.MonitorModel;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.tungsten.Tungsten;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Context
{
    private static Logger logger;
    private boolean readOnly;
    private final MDRepository metadataRepository;
    private Imports imports;
    private MetaInfo.Namespace curNamespace;
    private AuthToken sessionID;
    private final HSecurityManager security_manager;
    private MetaInfo.Flow curApp;
    private MetaInfo.Flow curFlow;
    private MetaInfo.User curUser;
    private ITopic<TaskEvent> remoteCmdOutput;
    private String lastRegisteredListenerId;
    private UUID reportingApp;
    private boolean isUIContext;
    public static final String PACKAGE_NAME = "wa";
    public boolean recompileMode;
    Compiler.ExecutionCallback cb;
    boolean isAppValidAfterRecompile;
    List<String> flowsFailed;
    
    protected Context() {
        this.reportingApp = null;
        this.isUIContext = false;
        this.recompileMode = false;
        this.cb = new Compiler.ExecutionCallback() {
            @Override
            public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                compiler.compileStmt(stmt);
            }
        };
        this.isAppValidAfterRecompile = true;
        this.flowsFailed = new ArrayList<String>();
        this.metadataRepository = null;
        this.security_manager = null;
    }
    
    public boolean isReportInProgress(final UUID app) {
        return this.reportingApp != null && app.equals((Object)this.reportingApp);
    }
    
    public void setIsUIContext(final boolean uiVal) {
        this.isUIContext = uiVal;
    }
    
    public boolean getIsUIContext() {
        return this.isUIContext;
    }
    
    public static Context createContextAndLocalServerPersist() {
        return createContextAndLocalServer(true);
    }
    
    public static Context createContextAndLocalServer() {
        return createContextAndLocalServer(false);
    }
    
    public static Context createContextAndLocalServer(final boolean persist) {
        System.setProperty("com.datasphere.config.persist", persist + "");
        System.setProperty("com.datasphere.config.adminPassword", "pass");
        try {
            Server.main(new String[0]);
            final Server srv = Server.server;
            final HSecurityManager sm = HSecurityManager.get();
            final AuthToken sessionID = sm.authenticate("admin", "pass");
            return srv.createContext(sessionID);
        }
        catch (Exception e) {
            Context.logger.error((Object)e, (Throwable)e);
            e.printStackTrace();
            return null;
        }
    }
    
    public static AuthToken createLocalServer() {
        System.setProperty("com.datasphere.config.persist", "false");
        System.setProperty("com.datasphere.config.adminPassword", "pass");
        final String cNameAndPwd = new Long(System.nanoTime()).toString();
        System.setProperty("com.datasphere.config.clusterName", cNameAndPwd);
        System.setProperty("com.datasphere.config.password", cNameAndPwd);
        System.setProperty("com.datasphere.config.company_name", "wa");
        System.setProperty("com.datasphere.config.license_key", "9F0C62AF5-D1DFAFBD0-C36A9B938-EDB01E714-37445A");
        try {
            Server.main(new String[0]);
            final HSecurityManager sm = HSecurityManager.get();
            final AuthToken sessionId = sm.authenticate("admin", "pass");
            return sessionId;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static Context createContext(final AuthToken sid) {
        return new Context(sid);
    }
    
    public Context(final AuthToken sid) {
        this.reportingApp = null;
        this.isUIContext = false;
        this.recompileMode = false;
        this.cb = new Compiler.ExecutionCallback() {
            @Override
            public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                compiler.compileStmt(stmt);
            }
        };
        this.isAppValidAfterRecompile = true;
        this.flowsFailed = new ArrayList<String>();
        this.metadataRepository = MetadataRepository.getINSTANCE();
        this.curNamespace = MetaInfo.GlobalNamespace;
        this.imports = new Imports();
        this.sessionID = sid;
        this.security_manager = HSecurityManager.get();
        this.readOnly = false;
    }
    
    private void removeMetaObject(final UUID id) throws MetaDataRepositoryException {
        this.metadataRepository.removeMetaObjectByUUID(id, this.sessionID);
    }
    
    public void updateMetaObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.metadataRepository.updateMetaObject(obj, this.sessionID);
    }
    
    private void putMetaObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.metadataRepository.putMetaObject(obj, this.sessionID);
    }
    
    private MetaInfo.MetaObject getMetaObject(final EntityType type, final String ns, final String name) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByName(type, ns, name, null, this.sessionID);
    }
    
    private MetaInfo.MetaObject getMetaObject(final UUID id) throws MetaDataRepositoryException {
        return this.metadataRepository.getMetaObjectByUUID(id, this.sessionID);
    }
    
    public void resetAppScope() {
        this.curFlow = null;
        this.curApp = null;
    }
    
    public void resetNamespace() {
        this.resetAppScope();
        this.curNamespace = MetaInfo.GlobalNamespace;
    }
    
    public AuthToken getAuthToken() {
        return this.sessionID;
    }
    
    public boolean setReadOnly(final boolean rdonly) {
        final boolean prevValue = this.readOnly;
        this.readOnly = rdonly;
        return prevValue;
    }
    
    public boolean isReadOnly() {
        return this.readOnly;
    }
    
    public static void shutdown(final String who) {
        try {
            Server.shutDown(who);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void shutdown() {
        shutdown("context");
    }
    
    public void setAdHocQueryCallback(final MessageListener<TaskEvent> listener) {
        if (this.remoteCmdOutput == null) {
            this.remoteCmdOutput = HazelcastSingleton.get().getTopic("session_" + this.sessionID);
        }
        if (this.lastRegisteredListenerId != null) {
            this.remoteCmdOutput.removeMessageListener(this.lastRegisteredListenerId);
        }
        if (listener != null) {
            this.lastRegisteredListenerId = this.remoteCmdOutput.addMessageListener((MessageListener)listener);
        }
    }
    
    private void cleanUp(final EntityType entityType, final String namespaceName, final String objectName) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.MetaObject o = this.getMetaObject(entityType, this.getNamespaceName(namespaceName), objectName);
        if (o != null) {
            this.removeMetaObject(o.getUuid());
        }
        if (o != null && o.getType() == EntityType.APPLICATION) {
            final List<UUID> deps = o.getDependencies();
            for (final UUID dep : deps) {
                final MetaInfo.MetaObject obj = this.getObject(dep);
                if (obj instanceof MetaInfo.WAStoreView) {
                    final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
                    final MetaInfo.HDStore wastore = (MetaInfo.HDStore)this.getMetaObject(wastoreview.wastoreID);
                    try {
                        wastore.getReverseIndexObjectDependencies().remove(dep);
                        this.updateMetaObject(wastore);
                    }
                    catch (Exception ex) {}
                    this.removeMetaObject(dep);
                }
            }
        }
    }
    
    public MetaInfo.Query buildAdHocQuery(final String appName, final String streamName, final String cqName, final String queryName, final CQExecutionPlan.CQExecutionPlanFactory planFactory, String selectText, final String sourceText, final boolean isAdhoc, final List<Property> queryParams, final String namespaceName) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.Namespace ns = this.getNamespace(namespaceName);
        this.curApp = null;
        this.curFlow = null;
        final UUID queryUUID = new UUID(System.currentTimeMillis());
        MetaInfo.Flow app = null;
        MetaInfo.Stream stream = null;
        MetaInfo.CQ cq = null;
        MetaInfo.Query query = null;
        MetaInfo.Flow curApp = this.getCurApp();
        try {
            app = new MetaInfo.Flow();
            app.addReverseIndexObjectDependencies(queryUUID);
            app.construct(EntityType.APPLICATION, appName, ns, null, 0, 0L);
            app.getMetaInfoStatus().setAnonymous(true);
            if (isAdhoc) {
                app.getMetaInfoStatus().setAdhoc(true);
            }
            this.put(app);
            app = (MetaInfo.Flow)this.getObject(app.getUuid());
            this.setCurApp(app);
            final MetaInfoStatus statusOfObjects = isAdhoc ? new MetaInfoStatus().setAnonymous(true).setAdhoc(true) : null;
            stream = this.putStream(false, this.makeObjectName(namespaceName, streamName), UUID.nilUUID(), null, null, null, null, statusOfObjects);
            final CQExecutionPlan plan = planFactory.createPlan();
            cq = this.putCQ(false, this.makeObjectName(namespaceName, cqName), stream.getUuid(), plan, selectText, Collections.emptyList(), statusOfObjects, null);
            this.endBlock(app);
            if (!selectText.trim().endsWith(";")) {
                selectText = selectText.trim() + ";";
            }
            query = new MetaInfo.Query(queryName, ns, app.getUuid(), stream.getUuid(), cq.getUuid(), plan.makeFieldsMap(), selectText, isAdhoc);
            query.setBindParameters(queryParams);
            for (final ParamDesc d : cq.plan.paramsDesc) {
                query.queryParameters.add(d.paramName);
            }
            for (final RSFieldDesc d2 : plan.resultSetDesc) {
                query.projectionFields.add(new QueryManager.QueryProjection(d2));
            }
            query.setUuid(queryUUID);
            if (isAdhoc) {
                query.getMetaInfoStatus().setAdhoc(true);
            }
            this.putQuery(query);
            return query;
        }
        catch (Exception e) {
            if (app != null) {
                this.endBlock(app);
                this.cleanUp(EntityType.APPLICATION, namespaceName, appName);
            }
            if (stream != null) {
                this.cleanUp(EntityType.STREAM, namespaceName, stream.getName());
            }
            if (cq != null) {
                this.cleanUp(EntityType.CQ, namespaceName, cq.getName());
            }
            if (query != null) {
                this.cleanUp(EntityType.QUERY, namespaceName, queryName);
            }
            curApp = null;
            this.curFlow = null;
            throw new RuntimeException(e);
        }
        finally {
            curApp = null;
            this.curFlow = null;
        }
    }
    
    public MetaInfo.MetaObject duplicateMetaObject(final String newName, final MetaInfo.MetaObject obj) throws CloneNotSupportedException, MetaDataRepositoryException {
        this.checkDropForObjectClone(obj);
        final MetaInfo.MetaObject newobj = obj.clone();
        newobj.setUuid(UUID.genCurTimeUUID());
        newobj.setName(newName);
        newobj.getReverseIndexObjectDependencies().clear();
        if (newobj instanceof MetaInfo.WAStoreView) {
            final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)newobj;
            final MetaInfo.MetaObject hdStore = this.getMetaObject(wastoreview.wastoreID);
            hdStore.addReverseIndexObjectDependencies(wastoreview.getUuid());
            this.updateMetaObject(hdStore);
        }
        final String nvUrl = MDConstants.makeURLWithoutVersion(newobj);
        final String vUrl = MDConstants.makeURLWithVersion(nvUrl, newobj.version);
        newobj.setUri(vUrl);
        return newobj;
    }
    
    public MetaInfo.MetaObject checkDropForObjectClone(MetaInfo.MetaObject obj) {
        Label_0234: {
            if (!(obj instanceof MetaInfo.WAStoreView)) {
                if (obj.getMetaInfoStatus().isDropped()) {
                    try {
                        final MetaInfo.MetaObject updatedObj = this.metadataRepository.getMetaObjectByName(obj.getType(), obj.getNsName(), obj.getName(), null, HSecurityManager.TOKEN);
                        if (updatedObj != null && !updatedObj.getMetaInfoStatus().isDropped()) {
                            obj = updatedObj;
                        }
                        return updatedObj;
                    }
                    catch (MetaDataRepositoryException e) {
                        if (Context.logger.isInfoEnabled()) {
                            Context.logger.info((Object)("Metaobject " + obj.getFullName() + " not found"));
                        }
                        break Label_0234;
                    }
                }
                return null;
            }
            MetaInfo.MetaObject hdStore = null;
            try {
                hdStore = this.metadataRepository.getMetaObjectByUUID(((MetaInfo.WAStoreView)obj).wastoreID, HSecurityManager.TOKEN);
                if (hdStore == null || !hdStore.getMetaInfoStatus().isDropped()) {
                    return null;
                }
                final MetaInfo.MetaObject newHDStoreMetaObject = this.metadataRepository.getMetaObjectByName(hdStore.getType(), hdStore.getNsName(), hdStore.getName(), null, HSecurityManager.TOKEN);
                if (newHDStoreMetaObject != null && !newHDStoreMetaObject.getMetaInfoStatus().isDropped()) {
                    ((MetaInfo.WAStoreView)obj).wastoreID = newHDStoreMetaObject.getUuid();
                }
            }
            catch (MetaDataRepositoryException e2) {
                if (Context.logger.isInfoEnabled()) {
                    Context.logger.info((Object)((hdStore == null) ? "WAStoreview" : "HDstore object is null"));
                }
            }
            try {
                this.metadataRepository.updateMetaObject(obj, HSecurityManager.TOKEN);
            }
            catch (MetaDataRepositoryException e) {
                if (Context.logger.isInfoEnabled()) {
                    Context.logger.info((Object)("Metaobject " + obj.getFullName() + " is null."));
                }
            }
        }
        return obj;
    }
    
    public MetaInfo.Query prepareQuery(final MetaInfo.Query query, final List<Property> queryParams) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.Flow app = (MetaInfo.Flow)this.getMetaObject(query.appUUID);
        final MetaInfo.CQ cq = (MetaInfo.CQ)this.getMetaObject(query.cqUUID);
        final MetaInfo.Stream stream = (MetaInfo.Stream)this.getMetaObject(query.streamUUID);
        this.validatePermissions(cq);
        final List<UUID> deps = app.getDependencies();
        if (!deps.remove(cq.getUuid())) {
            throw new RuntimeException("query " + app.name + " does not have cq " + cq.name);
        }
        if (!deps.remove(stream.getUuid())) {
            throw new RuntimeException("query " + app.name + " does not have stream " + stream.name);
        }
        final String uniqueID = "_" + System.nanoTime();
        final String appName = app.getName() + uniqueID;
        final String streamName = stream.getName() + uniqueID;
        final String cqName = cq.getName() + uniqueID;
        final String queryName = query.getName() + uniqueID;
        final CQExecutionPlan.CQExecutionPlanFactory planFactory = new CQExecutionPlan.CQExecutionPlanFactory() {
            @Override
            public CQExecutionPlan createPlan() throws MetaDataRepositoryException, CloneNotSupportedException {
                boolean isCQupdated = false;
                final List<CQExecutionPlan.DataSource> dss = cq.getPlan().dataSources;
                for (int i = 0; i < dss.size(); ++i) {
                    final MetaInfo.MetaObject subtaskObj = Context.this.metadataRepository.getMetaObjectByUUID(dss.get(i).getDataSourceID(), HSecurityManager.TOKEN);
                    final MetaInfo.MetaObject obj = Context.this.checkDropForObjectClone(subtaskObj);
                    if (obj != null) {
                        dss.get(i).replaceDataSourceID(obj.getUuid());
                        isCQupdated = true;
                    }
                }
                if (isCQupdated) {
                    Context.this.metadataRepository.updateMetaObject(cq, HSecurityManager.TOKEN);
                }
                final CQExecutionPlan plan = cq.getPlan().clone();
                final List<Pair<MetaInfo.MetaObject, MetaInfo.MetaObject>> oldnew = new ArrayList<Pair<MetaInfo.MetaObject, MetaInfo.MetaObject>>();
                for (final UUID id : deps) {
                    final MetaInfo.MetaObject obj2 = Context.this.getObject(id);
                    if (obj2 != null) {
                        final String newName = obj2.getName() + uniqueID;
                        final MetaInfo.MetaObject newobj = Context.this.duplicateMetaObject(newName, obj2);
                        newobj.getMetaInfoStatus().setAdhoc(true);
                        Context.this.put(newobj);
                        final CQExecutionPlan.DataSource ds = plan.findDataSource(id);
                        if (ds != null) {
                            ds.replaceDataSourceID(newobj.getUuid());
                        }
                        oldnew.add(Pair.make(obj2, newobj));
                    }
                }
                for (final Pair<MetaInfo.MetaObject, MetaInfo.MetaObject> p : oldnew) {
                    if (p.second instanceof MetaInfo.CQ) {
                        final MetaInfo.CQ newcq = (MetaInfo.CQ)p.second;
                        newcq.setPlan(newcq.getPlan().clone());
                        final CQExecutionPlan plan2 = newcq.getPlan();
                        for (final Pair<MetaInfo.MetaObject, MetaInfo.MetaObject> p2 : oldnew) {
                            final CQExecutionPlan.DataSource ds2 = plan2.findDataSource(p2.first.uuid);
                            if (ds2 != null) {
                                ds2.replaceDataSourceID(p2.second.uuid);
                            }
                        }
                        Context.this.updateMetaObject(newcq);
                    }
                }
                return plan;
            }
        };
        final String selectText = cq.getSelect();
        final String sourceText = query.queryDefinition;
        final boolean isAdhoc = true;
        return this.buildAdHocQuery(appName, streamName, cqName, queryName, planFactory, selectText, sourceText, true, queryParams, query.getNsName());
    }
    
    public void validatePermissions(final MetaInfo.CQ cq) throws MetaDataRepositoryException {
        for (final UUID fromUUID : cq.getPlan().getDataSources()) {
            MetaInfo.MetaObject mo = this.getMetaObject(fromUUID);
            if (mo instanceof MetaInfo.WAStoreView) {
                mo = this.getMetaObject(((MetaInfo.WAStoreView)mo).wastoreID);
            }
            if (mo != null) {
                PermissionUtility.checkPermission(mo, ObjectPermission.Action.select, this.sessionID, true);
            }
        }
    }
    
    public void startAdHocQuery(final MetaInfo.Query queryMetaObject) throws MetaDataRepositoryException {
        if (queryMetaObject == null) {
            throw new RuntimeException("Query metaobject is not found");
        }
        final MetaInfo.Flow app = (MetaInfo.Flow)this.getMetaObject(queryMetaObject.appUUID);
        final UUID cquuid = app.getObjects(EntityType.CQ).iterator().next();
        final MetaInfo.CQ cq = (MetaInfo.CQ)this.getObject(cquuid);
        UUID deploymentGroup = null;
        for (final UUID ds : cq.plan.getDataSources()) {
            if ((deploymentGroup = Utility.getDeploymentGroupFromMetaObject(ds, HSecurityManager.TOKEN)) != null) {
                break;
            }
        }
        final List<MetaInfo.Flow.Detail> deploymentPlan = new ArrayList<MetaInfo.Flow.Detail>();
        MetaInfo.DeploymentGroup dg;
        if (deploymentGroup == null) {
            dg = (MetaInfo.DeploymentGroup)this.getMetaObject(EntityType.DG, "Global", "default");
            if (dg == null) {
                throw new FatalException("Invoking adhoc on not deployed object.");
            }
        }
        else {
            dg = (MetaInfo.DeploymentGroup)this.getObject(deploymentGroup);
        }
        assert dg != null;
        final MetaInfo.Flow.Detail detail = new MetaInfo.Flow.Detail();
        detail.construct(DeploymentStrategy.ON_ONE, dg.uuid, app.uuid, MetaInfo.Flow.Detail.FailOverRule.NONE);
        deploymentPlan.add(detail);
        app.setDeploymentPlan(deploymentPlan);
        this.putFlow(app);
        final List<Property> params = queryMetaObject.getBindParameters();
        this.changeFlowState(ActionType.START_ADHOC, app, params);
    }
    
    public void stopAndDropAdHocQuery(final MetaInfo.Query query) throws MetaDataRepositoryException {
        final MetaInfo.Flow app = (MetaInfo.Flow)this.getMetaObject(query.appUUID);
        if (app != null) {
            this.changeFlowState(ActionType.STOP_ADHOC, app);
            for (final UUID dep : app.getDependencies()) {
                final MetaInfo.MetaObject obj = this.getMetaObject(dep);
                if (obj instanceof MetaInfo.WAStoreView) {
                    final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
                    final MetaInfo.HDStore wastore = (MetaInfo.HDStore)this.getMetaObject(wastoreview.wastoreID);
                    try {
                        wastore.getReverseIndexObjectDependencies().remove(dep);
                        this.updateMetaObject(wastore);
                    }
                    catch (Exception ex) {}
                }
                this.removeMetaObject(dep);
            }
            this.removeMetaObject(app.uuid);
            this.removeMetaObject(query.uuid);
        }
    }
    
    public void stopAdHocQuery(final MetaInfo.Query queryMetaObject) {
        try {
            final MetaInfo.Flow app = (MetaInfo.Flow)this.getObject(queryMetaObject.appUUID);
            if (app != null) {
                this.changeFlowState(ActionType.STOP_ADHOC, app);
            }
        }
        catch (MetaDataRepositoryException e) {
            Context.logger.warn((Object)e.getMessage());
        }
    }
    
    public MetaInfo.MetaObject getFlow(final String name, final EntityType type) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject result = this.get(this.addSchemaPrefix(null, name), type);
        return result;
    }
    
    public String[] splitNamespaceAndName(final String name, final EntityType type) {
        final String[] namespaceAndName = new String[2];
        if (name.indexOf(46) == -1) {
            if (EntityType.isGlobal(type)) {
                namespaceAndName[0] = "Global";
            }
            else {
                namespaceAndName[0] = this.curNamespace.name;
            }
            namespaceAndName[1] = name;
        }
        else {
            namespaceAndName[0] = name.split("\\.")[0];
            namespaceAndName[1] = name.split("\\.")[1];
        }
        return namespaceAndName;
    }
    
    public <T extends MetaInfo.MetaObject> T get(final String name, final EntityType type) throws MetaDataRepositoryException {
        final String[] namespaceAndName = this.splitNamespaceAndName(name, type);
        final MetaInfo.MetaObject resultObject = this.getMetaObject(type, namespaceAndName[0], namespaceAndName[1]);
        if (resultObject == null) {
            return null;
        }
        if (resultObject.getMetaInfoStatus().isDropped()) {
            return null;
        }
        return (T)resultObject;
    }
    
    public void updateFlow(final MetaInfo.Flow flow) throws MetaDataRepositoryException {
        this.updateMetaObject(flow);
    }
    
    public void updateWAStoreView(final MetaInfo.WAStoreView view) throws MetaDataRepositoryException {
        this.putMetaObject(view);
    }
    
    private boolean currentlyInScope() {
        return this.curApp != null || this.curFlow != null;
    }
    
    public MetaInfo.MetaObject put(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        long opStartTime = 0L;
        long opEndTime = 0L;
        if (obj instanceof MetaObjectPermissionChecker && !((MetaObjectPermissionChecker)obj).checkPermissionForMetaPropertyVariable(this.sessionID)) {
            Context.logger.error((Object)"Problem accessing PropVariable. See if you have right permissions or check if the prop variable exists");
        }
        if (this.readOnly) {
            return null;
        }
        if (Context.logger.isInfoEnabled()) {
            opStartTime = System.currentTimeMillis();
        }
        if (this.currentlyInScope() && !this.correctScopeForObject(obj.getType())) {
            throw new RuntimeException("Cannot put " + obj.type + " while inside of application/flow.");
        }
        if (this.curApp != null || this.curFlow != null) {
            final MetaInfo.Flow f = (this.curFlow != null) ? this.curFlow : this.curApp;
            if (this.curApp != null) {
                obj.addReverseIndexObjectDependencies(this.curApp.uuid);
            }
            if (this.curFlow != null) {
                obj.addReverseIndexObjectDependencies(this.curFlow.uuid);
            }
        }
        this.putMetaObject(obj);
        final MetaInfo.MetaObject old = this.getMetaObject(obj.getUuid());
        final MetaInfo.StatusInfo status = new MetaInfo.StatusInfo();
        status.construct(obj.uuid, MetaInfo.StatusInfo.Status.CREATED, obj.type, obj.name);
        this.metadataRepository.putStatusInfo(status, this.sessionID);
        if (this.curApp != null || this.curFlow != null) {
            final MetaInfo.Flow f2 = (this.curFlow != null) ? this.curFlow : this.curApp;
            f2.addObject(obj.type, obj.uuid);
            this.updateFlow(f2);
        }
        if (Context.logger.isInfoEnabled()) {
            opEndTime = System.currentTimeMillis();
            Context.logger.info((Object)("context put for put of " + obj.getFullName() + " took " + (opEndTime - opStartTime) / 1000L + " seconds"));
        }
        return old;
    }
    
    private boolean correctScopeForObject(final EntityType entityType) {
        return (this.curFlow == null || entityType.canBePartOfFlow()) && (this.curFlow != null || this.curApp == null || entityType.canBePartOfApp());
    }
    
    private void removeObject(final UUID uuid) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject metaObjectToDrop = this.getObject(uuid);
        this.dropObject(metaObjectToDrop.name, metaObjectToDrop.type, DropMetaObject.DropRule.NONE);
    }
    
    public MetaInfo.MetaObject removeObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        if (this.readOnly) {
            return null;
        }
        this.removeMetaObject(obj.getUuid());
        return null;
    }
    
    private MetaInfo.MetaObject putStream(final MetaInfo.Stream obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putCache(final MetaInfo.Cache obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putWindow(final MetaInfo.Window obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putNamespace(final MetaInfo.Namespace obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    public MetaInfo.MetaObject putType(final MetaInfo.Type obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putCQ(final MetaInfo.CQ obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putPropertySet(final MetaInfo.PropertySet obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putPropertyVariable(final MetaInfo.PropertyVariable obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putTarget(final MetaInfo.Target obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putSorter(final MetaInfo.Sorter obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putWAStoreView(final MetaInfo.WAStoreView obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putSource(final MetaInfo.Source obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putFlow(final MetaInfo.Flow obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putDeploymentGroup(final MetaInfo.DeploymentGroup obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putHDStore(final MetaInfo.HDStore obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putHDStoreView(final MetaInfo.WAStoreView obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putRole(final MetaInfo.Role obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    private MetaInfo.MetaObject putVisualization(final MetaInfo.Visualization obj) throws MetaDataRepositoryException {
        return this.put(obj);
    }
    
    public void putTmpObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        assert obj.name != null;
        this.put(obj);
    }
    
    public MetaInfo.MetaObject getObject(final UUID uuid) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject result = this.getMetaObject(uuid);
        if (result != null && result.getMetaInfoStatus() != null && result.getMetaInfoStatus().isDropped()) {
            return null;
        }
        return result;
    }
    
    private MetaInfo.Stream getStream(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.STREAM);
    }
    
    private MetaInfo.MetaObject getDataSource(final String name) throws MetaDataRepositoryException {
        MetaInfo.MetaObject ret = this.get(name, EntityType.STREAM);
        if (ret == null) {
            ret = this.get(name, EntityType.WINDOW);
        }
        if (ret == null) {
            ret = this.get(name, EntityType.CACHE);
        }
        if (ret == null) {
            ret = this.get(name, EntityType.HDSTORE);
        }
        return ret;
    }
    
    public MetaInfo.Type getType(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.TYPE);
    }
    
    private MetaInfo.Cache getCache(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.CACHE);
    }
    
    public MetaInfo.CQ getCQ(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.CQ);
    }
    
    private MetaInfo.PropertySet getPropertySet(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.PROPERTYSET);
    }
    
    private MetaInfo.PropertyVariable getPropertyVariable(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.PROPERTYVARIABLE);
    }
    
    private MetaInfo.Source getSource(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.SOURCE);
    }
    
    private MetaInfo.Target getTarget(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.TARGET);
    }
    
    private MetaInfo.Sorter getSorter(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.SORTER);
    }
    
    public MetaInfo.DeploymentGroup getDeploymentGroup(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.DG);
    }
    
    private MetaInfo.User getUser(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.USER);
    }
    
    private MetaInfo.Role getRole(final String name) throws MetaDataRepositoryException {
        return this.get(name, EntityType.ROLE);
    }
    
    public MetaInfo.PropertyTemplateInfo getAdapterProperties(final String name, final String version) throws MetaDataRepositoryException {
        final Set<MetaInfo.PropertyTemplateInfo> ptiSet = (Set<MetaInfo.PropertyTemplateInfo>)this.metadataRepository.getByEntityType(EntityType.PROPERTYTEMPLATE, this.sessionID);
        final String[] nsAndName = this.splitNamespaceAndName(name, EntityType.PROPERTYTEMPLATE);
        final List<String> versionList = new ArrayList<String>();
        for (final MetaInfo.PropertyTemplateInfo pti : ptiSet) {
            if (version == null) {
                if (!pti.name.equals(nsAndName[1]) || !pti.nsName.equals(nsAndName[0]) || pti.adapterVersion == null || pti.adapterVersion.equals("0.0.0") || pti.getMetaInfoStatus().isDropped()) {
                    continue;
                }
                versionList.add(pti.adapterVersion);
            }
            else {
                if (pti.name.equals(nsAndName[1]) && pti.nsName.equals(nsAndName[0]) && pti.adapterVersion != null && pti.adapterVersion.equals(version) && !pti.getMetaInfoStatus().isDropped()) {
                    return pti;
                }
                continue;
            }
        }
        if (versionList.size() > 0) {
            throw new MetaDataRepositoryException("PropertyTemplate " + name + " exists with multiple version " + versionList.toString() + ". Require Version for this Adapter");
        }
        return this.get(name, EntityType.PROPERTYTEMPLATE);
    }
    
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        final ClassLoader classLoader = HDLoader.get();
        return classLoader.loadClass(name);
    }
    
    public Class<?> getClassWithoutReplacingPrimitives(final String name) throws ClassNotFoundException {
        final Class<?> c = CompilerUtils.getPrimitiveClassByName(name);
        if (c == null) {
            return this.getClass(name);
        }
        return c;
    }
    
    public Class<?> getClass(final String name) throws ClassNotFoundException {
        try {
            Class<?> c = CompilerUtils.getClassNameShortcut(name);
            if (c == null) {
                c = this.loadClass(name);
            }
            return c;
        }
        catch (ClassNotFoundException e) {
            final Class<?> c2 = this.imports.findClass(name);
            if (c2 == null) {
                throw e;
            }
            return c2;
        }
    }
    
    public MetaInfo.Namespace putNamespace(final boolean doReplace, final String name) throws MetaDataRepositoryException {
        final MetaInfo.Namespace nameSpace = this.getNamespace(name);
        if (nameSpace == null || nameSpace.getMetaInfoStatus().isDropped()) {
            final MetaInfo.Namespace newschema = new MetaInfo.Namespace();
            newschema.construct(name, new UUID(System.currentTimeMillis()));
            final MetaInfo.Namespace namespace = (MetaInfo.Namespace)this.putNamespace(newschema);
            this.addAppSpecificRoles(namespace);
            this.addAppRolesToGlobalRoles(namespace);
            return newschema;
        }
        if (!doReplace) {
            throw new CompilationException("Namespace " + name + " already exists");
        }
        throw new FatalException("Cannot create or replace namespace, drop the namespace first.");
    }
    
    private void addAppSpecificRoles(final MetaInfo.Namespace ns) throws MetaDataRepositoryException {
        if (ns.name.equals("Global")) {
            return;
        }
        try {
            final List<ObjectPermission> permissions1 = new ArrayList<ObjectPermission>();
            permissions1.add(new ObjectPermission(ns.name + ":*:*:*"));
            permissions1.add(new ObjectPermission("Global:*:namespace:" + ns.name));
            final MetaInfo.Role admin = new MetaInfo.Role();
            admin.construct(ns, "admin", null, permissions1);
            this.putMetaObject(admin);
            List<ObjectPermission> permissions2 = null;
            permissions2 = new ArrayList<ObjectPermission>();
            permissions2.add(new ObjectPermission(ns.name + ":create,update,read,select,start,stop:*:*"));
            permissions2.add(new ObjectPermission("Global:read,select:namespace:" + ns.name));
            final MetaInfo.Role dev = new MetaInfo.Role();
            dev.construct(ns, "dev", null, permissions2);
            this.putMetaObject(dev);
            List<ObjectPermission> permissions3 = null;
            permissions3 = new ArrayList<ObjectPermission>();
            permissions3.add(new ObjectPermission(ns.name + ":read,select:*:*"));
            permissions3.add(new ObjectPermission("Global:read,select:namespace:" + ns.name));
            final MetaInfo.Role eu = new MetaInfo.Role();
            eu.construct(ns, "enduser", null, permissions3);
            this.putMetaObject(eu);
        }
        catch (SecurityException se) {
            Context.logger.error((Object)("error in creating roles for namespace " + ns.name));
        }
    }
    
    private void addAppRolesToGlobalRoles(final MetaInfo.Namespace namespace) throws MetaDataRepositoryException {
        if (Context.logger.isInfoEnabled()) {
            Context.logger.info((Object)"added app specific roles ");
        }
        final String uid = HSecurityManager.getAutheticatedUserName(this.sessionID);
        final MetaInfo.User u = (MetaInfo.User)this.getMetaObject(EntityType.USER, "Global", uid);
        final MetaInfo.Role r1 = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, namespace.getName(), "admin");
        final MetaInfo.Role r2 = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, namespace.getName(), "dev");
        final MetaInfo.Role r3 = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, namespace.getName(), "enduser");
        if (!namespace.name.equals("admin")) {
            if (!u.getUserId().equalsIgnoreCase("admin")) {
                u.grantRole(r1);
                this.updateMetaObject(u);
            }
            final MetaInfo.Role adr = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, "Global", "appadmin");
            adr.grantRole(r1);
            this.updateMetaObject(adr);
            final MetaInfo.Role adv = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, "Global", "appdev");
            adv.grantRole(r2);
            this.updateMetaObject(adv);
            final MetaInfo.Role aur = (MetaInfo.Role)this.getMetaObject(EntityType.ROLE, "Global", "appuser");
            aur.grantRole(r3);
            this.updateMetaObject(aur);
        }
    }
    
    public MetaInfo.Cache putCache(final boolean doReplace, final ObjectName objectName, final String adapterClassName, final Map<String, Object> reader_propertySet, final Map<String, Object> parser_propertySet, final Map<String, Object> query_propertySet, final UUID typename, final Class<?> ret) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.MetaObject dataSource = this.getDataSource(name);
        if (dataSource != null) {
            if (this.recompileMode) {
                this.removeObject(dataSource);
                oldUUID = dataSource.uuid;
            }
            else {
                if (!doReplace) {
                    throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
                }
                this.dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
            }
        }
        MetaInfo.Cache newcache = new MetaInfo.Cache();
        newcache.construct(name, this.getNamespace(objectName.getNamespace()), adapterClassName, reader_propertySet, parser_propertySet, query_propertySet, typename, ret);
        if (this.recompileMode && oldUUID != null) {
            newcache.uuid = oldUUID;
        }
        this.putCache(newcache);
        newcache = (MetaInfo.Cache)this.getDataSource(name);
        return newcache;
    }
    
    public MetaInfo.Type putType(final boolean doReplace, final ObjectName objectName, final String extendsType, final Map<String, String> fields, final List<String> keyFields, final boolean isAnonymous) throws MetaDataRepositoryException {
        return this.putType(doReplace, objectName, extendsType, fields, keyFields, new MetaInfoStatus().setAnonymous(isAnonymous));
    }
    
    public MetaInfo.Type putType(final boolean doReplace, final ObjectName objectName, final String extendsType, final Map<String, String> fields, final List<String> keyFields, final MetaInfoStatus statusOfObjects) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final String name = this.addSchemaPrefix(objectName);
        MetaInfo.Type type = this.getType(name);
        MetaInfo.Flow saveCurrentFlow = null;
        MetaInfo.Flow saveCurrentApp = null;
        if (type != null) {
            if (this.recompileMode) {
                if (type != null) {
                    oldUUID = type.uuid;
                    this.removeObject(type);
                }
            }
            else if (!type.getMetaInfoStatus().isDropped()) {
                if (!doReplace) {
                    throw new CompilationException("Type " + name + " already exists");
                }
                saveCurrentFlow = this.getCurFlow();
                saveCurrentApp = this.getCurApp();
                final MetaInfo.Flow aflow = type.getCurrentFlow();
                final MetaInfo.Flow anapp = type.getCurrentApp();
                if (anapp != null && !anapp.equals(aflow)) {
                    this.setCurFlow(aflow);
                }
                this.setCurApp(anapp);
                DropMetaObject.DropType.drop(this, type, DropMetaObject.DropRule.NONE, this.sessionID);
            }
        }
        final HDLoader loader = HDLoader.get();
        final String className = "wa." + name + "_1_0";
        if (loader.isExistingClass(className)) {
            if (type == null && loader.isGeneratedClass(className)) {
                loader.removeTypeClass(this.getNamespaceName(objectName.getNamespace()), className);
            }
            else {
                if (!loader.isGeneratedClass(className) || !doReplace) {
                    throw new CompilationException("Cannot create type " + type + " another class " + className + " already exists");
                }
                loader.removeTypeClass(this.getNamespaceName(objectName.getNamespace()), className);
            }
        }
        final MetaInfo.Namespace ns = this.getNamespace(objectName.getNamespace());
        UUID extendsTypeUUID = null;
        MetaInfo.Type et = null;
        if (extendsType != null) {
            et = this.getType(this.addSchemaPrefix(null, extendsType));
            if (et != null) {
                extendsTypeUUID = et.uuid;
            }
        }
        type = new MetaInfo.Type();
        type.construct(name, ns, className, extendsTypeUUID, fields, keyFields, true);
        if (this.recompileMode && oldUUID != null) {
            type.uuid = oldUUID;
        }
        if (statusOfObjects != null) {
            type.setMetaInfoStatus(statusOfObjects);
        }
        this.putType(type);
        type = this.getType(name);
        try {
            type.generateClass();
        }
        catch (Exception e) {
            this.removeMetaObject(type.getUuid());
            throw new RuntimeException(e.getMessage());
        }
        if (et != null) {
            if (et.extendedBy == null) {
                et.extendedBy = new ArrayList<UUID>();
            }
            et.extendedBy.add(type.uuid);
            this.putType(et);
        }
        if (saveCurrentFlow != null) {
            this.setCurFlow(saveCurrentFlow);
        }
        if (saveCurrentApp != null) {
            this.setCurApp(saveCurrentApp);
        }
        return type;
    }
    
    public Class<?> getTypeClass(final UUID uuid) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject type = this.getObject(uuid);
        assert type instanceof MetaInfo.Type;
        final MetaInfo.Type t = (MetaInfo.Type)type;
        try {
            final Class<?> c = ClassLoader.getSystemClassLoader().loadClass(t.className);
            return c;
        }
        catch (ClassNotFoundException e) {
            throw new CompilationException("Internal error: cannot load type <" + t.name + ">", e);
        }
    }
    
    public MetaInfo.Type getTypeInfo(final UUID uuid) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject type = this.getObject(uuid);
        assert type instanceof MetaInfo.Type;
        return (MetaInfo.Type)type;
    }
    
    public MetaInfo.Stream putStream(final boolean doReplace, final ObjectName objectName, final UUID type, final List<String> partitioning_fields, final GracePeriod gp, final String pset, final UUID oldUUIDparam, final MetaInfoStatus statusOfObjects) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.MetaObject dataSource = this.getDataSource(name);
        if (dataSource != null) {
            if (this.recompileMode) {
                this.removeObject(dataSource);
                oldUUID = dataSource.uuid;
            }
            else {
                if (!doReplace) {
                    throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
                }
                this.dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
            }
        }
        final MetaInfo.Namespace ns = this.getNamespace(objectName.getNamespace());
        MetaInfo.Stream newstream = new MetaInfo.Stream();
        newstream.construct(name, ns, type, partitioning_fields, gp, pset);
        if (this.recompileMode) {
            if (oldUUID != null) {
                newstream.uuid = oldUUID;
            }
            if (oldUUIDparam != null) {
                newstream.uuid = oldUUIDparam;
            }
        }
        if (statusOfObjects != null) {
            newstream.setMetaInfoStatus(statusOfObjects);
        }
        this.putStream(newstream);
        newstream = (MetaInfo.Stream)this.getObject(newstream.uuid);
        return newstream;
    }
    
    public MetaInfo.Window putWindow(final boolean doReplace, final ObjectName objectName, final UUID stream, final List<String> partitioningFields, final Pair<IntervalPolicy, IntervalPolicy> windowPolicy, final boolean jumping, final boolean persistent, final boolean implicit, final Object options) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.MetaObject dataSource = this.getDataSource(name);
        if (dataSource != null) {
            if (this.recompileMode) {
                this.removeObject(dataSource);
                oldUUID = dataSource.uuid;
            }
            else {
                if (!doReplace) {
                    throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
                }
                if (dataSource.type != EntityType.WINDOW) {
                    throw new CompilationException("Window have the same name with the stream");
                }
                this.dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
            }
        }
        final IntervalPolicy windowLen = windowPolicy.first;
        if (jumping && windowPolicy.second != null) {
            throw new CompilationException("Jumping window cannot have a Slide policy.");
        }
        if (windowLen.isAttrBased()) {
            final String on_field = windowLen.getAttrPolicy().getAttrName();
            final MetaInfo.Stream stream_metainfo = (MetaInfo.Stream)this.getObject(stream);
            final MetaInfo.Type type_metainfo = (MetaInfo.Type)this.getObject(stream_metainfo.dataType);
            boolean matched = false;
            for (final Map.Entry<String, String> f : type_metainfo.fields.entrySet()) {
                if (f.getKey().equalsIgnoreCase(on_field)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw new CompilationException("stream <" + stream_metainfo.name + "> has no such field <" + on_field + "> ");
            }
        }
        MetaInfo.Window newwindow = new MetaInfo.Window();
        final MetaInfo.Namespace ns = this.getNamespace(Utility.splitDomain(name));
        if (ns == null) {
            throw new CompilationException("Cannot find the namespace to put windows: " + Utility.splitDomain(name));
        }
        newwindow.construct(Utility.splitName(name), ns, stream, windowPolicy, jumping, persistent, partitioningFields, implicit, options);
        if (this.recompileMode && oldUUID != null) {
            newwindow.uuid = oldUUID;
        }
        this.putWindow(newwindow);
        newwindow = (MetaInfo.Window)this.getDataSource(name);
        return newwindow;
    }
    
    public MetaInfo.CQ putCQ(final boolean doReplace, final ObjectName objectName, final UUID outputStream, final CQExecutionPlan plan, final String select, final List<String> fieldList, final MetaInfoStatus statusOfObjects, final String uiConfig) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(objectName);
        UUID oldUUID = null;
        final MetaInfo.CQ cq = this.getCQ(name);
        if (cq != null) {
            if (this.recompileMode) {
                if (cq != null) {
                    oldUUID = cq.uuid;
                    this.removeObject(cq);
                }
            }
            else if (!cq.getMetaInfoStatus().isDropped()) {
                if (!doReplace) {
                    throw new CompilationException("Continuous query " + name + " already exists");
                }
                DropMetaObject.DropCQ.drop(this, cq, DropMetaObject.DropRule.NONE, this.sessionID);
            }
        }
        MetaInfo.CQ newcq = new MetaInfo.CQ();
        newcq.construct(name, this.getNamespace(objectName.getNamespace()), outputStream, plan, select, fieldList, uiConfig);
        if (this.recompileMode && oldUUID != null) {
            newcq.uuid = oldUUID;
        }
        if (statusOfObjects != null) {
            newcq.setMetaInfoStatus(statusOfObjects);
        }
        this.putCQ(newcq);
        newcq = this.getCQ(name);
        return newcq;
    }
    
    public MetaInfo.PropertySet putPropertySet(final boolean doReplace, final String propsName, final Map<String, Object> propertySet) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(null, propsName);
        final MetaInfo.PropertySet props = this.getPropertySet(name);
        if (props != null) {
            if (!doReplace) {
                throw new CompilationException("Property Set " + name + " already exists");
            }
            this.removeObject(props.uuid);
        }
        final MetaInfo.PropertySet newprops = new MetaInfo.PropertySet();
        newprops.construct(name, this.getNamespace(null), propertySet);
        this.putPropertySet(newprops);
        return newprops;
    }
    
    public MetaInfo.PropertyVariable putPropertyVariable(final boolean doReplace, final String paramName, final Map<String, Object> propertySet) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(null, paramName);
        final MetaInfo.PropertyVariable propertyVariable = this.getPropertyVariable(name);
        if (propertyVariable != null) {
            if (!doReplace) {
                throw new CompilationException("PropertyVariable " + name + " already exists");
            }
            this.removeObject(propertyVariable.uuid);
        }
        final MetaInfo.PropertyVariable newPropertyVariable = new MetaInfo.PropertyVariable();
        newPropertyVariable.construct(name, this.getNamespace(null), propertySet);
        this.putPropertyVariable(newPropertyVariable);
        return newPropertyVariable;
    }
    
    public MetaInfo.Source putSource(final boolean doReplace, final ObjectName objectName, final String adapterClassName, final Map<String, Object> propertySet, final Map<String, Object> parserPropertySet, final UUID outputStream, final UUID oldUUID) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.Source src = this.getSource(name);
        if (src != null) {
            if (!src.getMetaInfoStatus().isDropped()) {
                if (!doReplace) {
                    throw new CompilationException("Source " + name + " already exists");
                }
            }
        }
        MetaInfo.Source newsrc = new MetaInfo.Source();
        newsrc.construct(name, this.getNamespace(objectName.getNamespace()), adapterClassName, propertySet, parserPropertySet, outputStream);
        if (this.recompileMode && oldUUID != null) {
            newsrc.uuid = oldUUID;
        }
        this.putSource(newsrc);
        newsrc = this.getSource(name);
        return newsrc;
    }
    
    public MetaInfo.Target putTarget(final boolean doReplace, final ObjectName objectName, final String adapterClassName, final Map<String, Object> propertySet, final Map<String, Object> formatterPropertySet, final Map<String, Object> parallelismProperties, final UUID inputStream, final UUID oldUUID) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.Target target = this.getTarget(name);
        if (target != null) {
            if (!target.getMetaInfoStatus().isDropped()) {
                if (!doReplace) {
                    throw new CompilationException("Target " + name + " already exists");
                }
            }
        }
        MetaInfo.Target newtarget = new MetaInfo.Target();
        newtarget.construct(name, this.getNamespace(objectName.getNamespace()), adapterClassName, propertySet, formatterPropertySet, parallelismProperties, inputStream);
        if (this.recompileMode && oldUUID != null) {
            newtarget.uuid = oldUUID;
        }
        this.putTarget(newtarget);
        newtarget = this.getTarget(name);
        return newtarget;
    }
    
    public MetaInfo.Flow putFlow(final EntityType type, final boolean doReplace, final ObjectName objectName, final boolean encrypted, final RecoveryDescription recov, final Map<String, Object> eh, final Map<EntityType, LinkedHashSet<UUID>> objects, final Set<String> importStatements) throws MetaDataRepositoryException {
        assert type == EntityType.APPLICATION;
        UUID oldUUID = null;
        int oldRecovType = 0;
        long oldRecovPeriod = 0L;
        final String name = this.addSchemaPrefix(objectName);
        MetaInfo.Flow flow = (MetaInfo.Flow)this.getFlow(name, type);
        if (flow != null) {
            if (this.recompileMode) {
                if (flow != null) {
                    oldUUID = flow.uuid;
                    oldRecovType = flow.recoveryType;
                    oldRecovPeriod = flow.recoveryPeriod;
                    this.removeObject(flow);
                }
            }
            else if (!flow.getMetaInfoStatus().isDropped()) {
                if (!doReplace) {
                    throw new CompilationException(type + " " + flow.name + " already exists");
                }
                DropMetaObject.DropApplication.drop(this, flow, DropMetaObject.DropRule.CASCADE, this.sessionID);
                flow = (MetaInfo.Flow)this.getFlow(name, type);
            }
        }
        final boolean startBlock = objects == null;
        if (startBlock) {
            if (type == EntityType.APPLICATION) {
                if (this.curApp != null) {
                    throw new CompilationException("cannot create APPLICATION <" + name + "> inside of another APPLICATION <" + this.curApp.name + ">, use command 'END APPLICATION <" + this.curApp.name + ">' to close the application scope.");
                }
                if (this.curFlow != null) {
                    throw new CompilationException("cannot create APPLICATION <" + name + "> inside of FLOW <" + this.curFlow.name + ">, use command 'END FLOW <" + this.curFlow.name + ">' to close the flow scope.");
                }
            }
            else {
                if (this.curApp == null) {
                    throw new CompilationException("cannot create FLOW <" + name + "> outside of APPLICATION");
                }
                if (this.curFlow != null) {
                    throw new CompilationException("cannot create FLOW <" + name + "> inside of another FLOW <" + this.curFlow.name + ">");
                }
            }
        }
        final MetaInfo.Namespace ns = this.getNamespace(objectName.getNamespace());
        final int recoveryType = (recov != null) ? recov.type : 0;
        final long recoveryPeriod = (recov != null) ? recov.interval : 0L;
        final MetaInfo.Flow newflow = new MetaInfo.Flow();
        newflow.construct(type, name, ns, objects, recoveryType, recoveryPeriod);
        if (type.equals(EntityType.APPLICATION)) {
            newflow.setEncrypted(encrypted);
            newflow.setEhandlers(eh);
            newflow.setImportStatements(importStatements);
        }
        if (this.recompileMode && oldUUID != null) {
            newflow.uuid = oldUUID;
            newflow.recoveryType = oldRecovType;
            newflow.recoveryPeriod = oldRecovPeriod;
        }
        final MetaInfo.Flow flower = (MetaInfo.Flow)this.putFlow(newflow);
        if (startBlock) {
            if (type == EntityType.APPLICATION) {
                if (this.recompileMode) {
                    this.curApp = flower;
                }
                else if (flow != null && !flow.getMetaInfoStatus().isDropped()) {
                    this.curApp = flow;
                }
                else {
                    this.curApp = flower;
                }
            }
            else if (this.recompileMode) {
                this.curFlow = flower;
            }
            else if (flow != null && !flow.getMetaInfoStatus().isDropped()) {
                this.curFlow = flow;
            }
            else {
                this.curFlow = flower;
            }
        }
        return flower;
    }
    
    public MetaInfo.DeploymentGroup putDeploymentGroup(final String deploymentGroupName, final List<String> deploymentGroup, final Long minServers, final Long maxApps) throws MetaDataRepositoryException {
        final MetaInfo.DeploymentGroup dGroup = this.getDeploymentGroup(deploymentGroupName);
        if (dGroup != null) {
            throw new CompilationException("Deployment Group " + deploymentGroupName + " already exists");
        }
        final MetaInfo.DeploymentGroup newDGroup = new MetaInfo.DeploymentGroup();
        newDGroup.construct(deploymentGroupName);
        newDGroup.addConfiguredMembers(deploymentGroup);
        newDGroup.setMinimumRequiredServers(minServers);
        newDGroup.setMaxApps(maxApps);
        if (Context.logger.isInfoEnabled()) {
            Context.logger.info((Object)("Create new DG : " + newDGroup.toString()));
        }
        this.putDeploymentGroup(newDGroup);
        return newDGroup;
    }
    
    private MetaInfo.MetaObject getBaseServer(final String baseServerName) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject server = this.metadataRepository.getServer(baseServerName, this.sessionID);
        if (server != null) {
            return server;
        }
        final MetaInfo.MetaObject agent = this.getMetaObject(EntityType.AGENT, "Global", baseServerName);
        if (agent == null) {
            throw new CompilationException("Server or Agent " + baseServerName + " does not exist");
        }
        return agent;
    }
    
    public void alterDeploymentGroup(final String deploymentGroupName, final List<String> nodesAdded, final List<String> nodesRemoved, final Long minServers, final Long limiApplications) throws MetaDataRepositoryException {
        if (deploymentGroupName == null) {
            throw new CompilationException("Null server name or deployment group entered.");
        }
        final MetaInfo.DeploymentGroup dGroup = this.getDeploymentGroup(deploymentGroupName);
        if (dGroup == null) {
            throw new CompilationException("Deployment Group " + deploymentGroupName + " does not exists");
        }
        if (nodesAdded != null) {
            dGroup.addConfiguredMembers(nodesAdded);
        }
        if (nodesRemoved != null) {
            dGroup.removeConfiguredMember(nodesRemoved);
        }
        if (minServers != null) {
            dGroup.setMinimumRequiredServers(minServers);
        }
        if (limiApplications != null) {
            dGroup.setMaxApps(limiApplications);
        }
        this.metadataRepository.updateMetaObject(dGroup, this.sessionID);
        if (Context.logger.isInfoEnabled()) {
            Context.logger.info((Object)("Alter DG : " + dGroup.toString()));
        }
    }
    
    public Imports getImports() {
        return this.imports;
    }
    
    public String getNamespaceName(@Nullable final String optionalNamespace) {
        return this.getNamespace(optionalNamespace).getName();
    }
    
    @Nonnull
    public String getCurNamespaceName() {
        return this.getNamespace(null).getName();
    }
    
    @Nonnull
    public MetaInfo.Namespace getCurNamespace() {
        return this.getNamespace(null);
    }
    
    public MetaInfo.Namespace getNamespace(@Nullable String optionalNamespace) {
        MetaInfo.Namespace namespaceObject = null;
        if (optionalNamespace == null) {
            return this.curNamespace;
        }
        if (Utility.checkIfFullName(optionalNamespace)) {
            optionalNamespace = Utility.splitDomain(optionalNamespace);
        }
        try {
            namespaceObject = this.get(optionalNamespace, EntityType.NAMESPACE);
        }
        catch (MetaDataRepositoryException e) {
            if (Context.logger.isInfoEnabled()) {
                Context.logger.info((Object)("Looking for namespace: " + optionalNamespace + ", namespace doesn't exist."));
            }
        }
        if (namespaceObject == null) {
            return null;
        }
        return namespaceObject;
    }
    
    public ObjectName makeObjectName(final String name) {
        if (Utility.checkIfFullName(name)) {
            return ObjectName.makeObjectName(Utility.splitDomain(name), Utility.splitName(name));
        }
        return ObjectName.makeObjectName(this.getCurNamespaceName(), Utility.splitName(name));
    }
    
    public ObjectName makeObjectName(final String namespacename, final String objname) {
        return this.makeObjectName(this.addSchemaPrefix(namespacename, objname));
    }
    
    public String addSchemaPrefix(@Nullable final String optionalNamespaceName, @Nonnull final String name) {
        return Utility.convertToFullQualifiedName(this.getNamespace(optionalNamespaceName), name);
    }
    
    public String addSchemaPrefix(final ObjectName object) {
        return Utility.convertToFullQualifiedName(this.getNamespace(object.getNamespace()), object.getName());
    }
    
    public void recompileQuery(final String queryName) throws Exception {
        final String fullQualifiedName = Utility.convertToFullQualifiedName(this.getNamespace(null), queryName);
        final MetaInfo.Query queryMetaObject = (MetaInfo.Query)this.getMetaObject(EntityType.QUERY, Utility.splitDomain(fullQualifiedName), Utility.splitName(fullQualifiedName));
        if (queryMetaObject == null) {
            throw new RuntimeException("No such query exist.");
        }
        if (queryMetaObject.getMetaInfoStatus().isValid()) {
            throw new RuntimeException("Query " + queryMetaObject.getFullName() + " is already valid, no need to recompile.");
        }
        final UUID queryId = queryMetaObject.getUuid();
        final String selectQuery = queryMetaObject.queryDefinition;
        final String namespaceName = queryMetaObject.getNsName();
        this.dropObject(queryMetaObject.getFullName(), EntityType.QUERY, DropMetaObject.DropRule.NONE);
        try {
            final AtomicBoolean isCompilationSucessful = new AtomicBoolean(true);
            Compiler.compile("CREATE NAMEDQUERY " + queryMetaObject.name + " " + queryMetaObject.queryDefinition, this, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    try {
                        compiler.compileStmt(stmt);
                    }
                    catch (Warning e) {
                        isCompilationSucessful.set(false);
                    }
                }
            });
            if (!isCompilationSucessful.get()) {
                throw new RuntimeException("Problem in recompilation");
            }
        }
        catch (Exception e) {
            final MetaInfo.Query Q = new MetaInfo.Query(queryName, this.getNamespace(namespaceName), null, null, null, null, selectQuery, false);
            Q.setUuid(queryId);
            Q.getMetaInfoStatus().setValid(false);
            this.putQuery(Q);
            throw new RuntimeException("Recompilation failed");
        }
    }
    
    public void useNamespace(final String nsName) throws MetaDataRepositoryException {
        final MetaInfo.Namespace ns = this.getNamespace(nsName);
        if (ns == null) {
            throw new CompilationException("Namespace " + nsName + " does not exist");
        }
        this.curNamespace = ns;
    }
    
    public void alterAppOrFlow(final EntityType type, final String name, final boolean recompile) throws MetaDataRepositoryException {
        final MetaInfo.Flow f = this.getFlowInCurSchema(name, type);
        if (f == null) {
            throw new CompilationException(type + " " + name + " does not exist in current namespace " + this.curNamespace.name);
        }
        PermissionUtility.checkPermission(f, ObjectPermission.Action.update, this.sessionID, true);
        if (f.isDeployed()) {
            throw new CompilationException(type + " " + f.getFullName() + " is deployed, cannot alter. (Undeploy first) ");
        }
        if (recompile) {
            this.curApp = null;
            this.curFlow = null;
            this.recompileMode = true;
            try {
                this.recompile(f);
            }
            catch (Exception e) {
                throw e;
            }
            finally {
                this.recompileMode = false;
            }
            return;
        }
        if (type == EntityType.APPLICATION) {
            final MetaInfo.Namespace ns = this.getNamespace(f.nsName);
            if (ns == null) {
                throw new CompilationException("There is no namespace " + f.nsName + " for application " + name);
            }
            this.curApp = f;
            this.curFlow = null;
            this.curNamespace = ns;
        }
        else {
            if (this.curApp != null) {
                final Set<UUID> flowsInApp = this.curApp.getObjects(EntityType.FLOW);
                if (flowsInApp == null || !flowsInApp.contains(f.uuid)) {
                    throw new CompilationException("FLOW " + name + " does not exist");
                }
            }
            this.curFlow = f;
        }
    }
    
    private void executeTQL(final String tqlText) throws Exception {
        Compiler.compile(tqlText, this, this.cb);
    }
    
    private void recompileObject(final MetaInfo.MetaObject metaObject, final Map<String, String> recompileMessagesBookkeeping) throws Exception {
        if (metaObject.getSourceText().toUpperCase().startsWith("CREATE OR REPLACE")) {
            try {
                this.executeTQL(metaObject.getSourceText() + ";");
                return;
            }
            catch (Throwable e) {
                recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
                this.isAppValidAfterRecompile = false;
                if (Context.logger.isDebugEnabled()) {
                    Context.logger.debug((Object)"-> FAILURE \n");
                    if (e instanceof NullPointerException) {
                        Context.logger.debug((Object)("Internal error:" + e + "\n"));
                    }
                    else {
                        final String msg = e.getLocalizedMessage();
                        Context.logger.debug((Object)(msg + "\n"));
                    }
                }
                throw new RuntimeException("Error in recompile");
            }
        }
        try {
            this.executeTQL("CREATE OR REPLACE " + metaObject.getSourceText().substring(6) + ";");
        }
        catch (Throwable e) {
            recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
            this.isAppValidAfterRecompile = false;
            if (Context.logger.isDebugEnabled()) {
                Context.logger.debug((Object)"-> FAILURE \n");
                if (e instanceof NullPointerException) {
                    Context.logger.debug((Object)("Internal error:" + e + "\n"));
                }
                else {
                    final String msg = e.getLocalizedMessage();
                    Context.logger.debug((Object)(msg + "\n"));
                }
            }
            throw new RuntimeException("Error in recompile");
        }
    }
    
    void createOrEndApplicationOrFlow(final MetaInfo.Flow flow, final boolean start) throws Exception {
        if (start) {
            try {
                this.executeTQL("CREATE OR REPLACE " + flow.type + " " + flow.name + ";");
                return;
            }
            catch (Throwable e) {
                this.isAppValidAfterRecompile = false;
                if (Context.logger.isDebugEnabled()) {
                    Context.logger.debug((Object)"-> FAILURE \n");
                    if (e instanceof NullPointerException) {
                        Context.logger.debug((Object)("Internal error:" + e + "\n"));
                    }
                    else {
                        final String msg = e.getLocalizedMessage();
                        Context.logger.debug((Object)(msg + "\n"));
                    }
                }
                throw new RuntimeException("Error in recompile");
            }
        }
        try {
            this.executeTQL("END " + flow.type + " " + flow.name + ";");
        }
        catch (Throwable e) {
            this.isAppValidAfterRecompile = false;
            if (Context.logger.isDebugEnabled()) {
                Context.logger.debug((Object)"-> FAILURE \n");
                if (e instanceof NullPointerException) {
                    Context.logger.debug((Object)("Internal error:" + e + "\n"));
                }
                else {
                    final String msg = e.getLocalizedMessage();
                    Context.logger.debug((Object)(msg + "\n"));
                }
            }
            throw new RuntimeException("Error in recompile");
        }
    }
    
    void recompileVisualization(final MetaInfo.Visualization metaObject) throws Exception {
        this.executeTQL("CREATE VISUALIZATION \"" + metaObject.fname + "\";");
    }
    
    void recompileSubscription(final MetaInfo.Target metaObject) throws Exception {
        String str = "";
        for (final Map.Entry<String, Object> entry : metaObject.properties.entrySet()) {
            str = str + entry.getKey() + ":" + entry.getValue() + ",";
        }
        str = str.substring(0, str.length() - 1);
        final MetaInfo.Stream stream = (MetaInfo.Stream)MetadataRepository.getINSTANCE().getMetaObjectByUUID(metaObject.getInputStream(), HSecurityManager.TOKEN);
        final String sr = "CREATE SUBSCRIPTION " + metaObject.getName() + " USING " + metaObject.adapterClassName + "(" + str + ") INPUT FROM " + stream.name + ";";
        this.executeTQL(sr);
    }
    
    public void addToFlow(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.updateReverseIndex(obj);
        if (this.curApp != null || this.curFlow != null) {
            final MetaInfo.Flow f = (this.curFlow != null) ? this.curFlow : this.curApp;
            f.addObject(obj.type, obj.uuid);
            this.updateFlow(f);
        }
    }
    
    private void updateReverseIndex(final MetaInfo.MetaObject metaObject) throws MetaDataRepositoryException {
        final MetaInfo.Flow app = this.curApp;
        if (app != null) {
            metaObject.addReverseIndexObjectDependencies(app.uuid);
        }
        final MetaInfo.Flow flow = this.curFlow;
        if (flow != null) {
            metaObject.addReverseIndexObjectDependencies(flow.uuid);
        }
        this.updateMetaObject(metaObject);
    }
    
    private void recompileSpecificObject(final UUID uuidOfObject, final Map<String, String> recompileMessagesBookkeeping) throws MetaDataRepositoryException {
        if (uuidOfObject != null) {
            if (Context.logger.isDebugEnabled()) {
                Context.logger.debug((Object)("----> RECOMPILE " + uuidOfObject));
            }
            final MetaInfo.MetaObject metaObject = this.getObject(uuidOfObject);
            if (metaObject == null) {
                if (Context.logger.isDebugEnabled()) {
                    Context.logger.debug((Object)("No Such UUID exists " + uuidOfObject));
                }
            }
            else if (metaObject.getMetaInfoStatus().isValid()) {
                if (Context.logger.isDebugEnabled()) {
                    Context.logger.debug((Object)("---> added to flow " + metaObject.name));
                }
                this.addToFlow(metaObject);
            }
            else if (metaObject != null && metaObject.type == EntityType.TYPE) {
                if (((MetaInfo.Type)metaObject).generated && metaObject.getSourceText() != null) {
                    try {
                        this.recompileObject(metaObject, recompileMessagesBookkeeping);
                    }
                    catch (Exception e) {
                        if (Context.logger.isDebugEnabled()) {
                            Context.logger.debug((Object)("---> added old object to flow " + metaObject.name));
                        }
                        this.addToFlow(metaObject);
                        if (this.curFlow != null) {
                            this.flowsFailed.add(this.curFlow.name);
                        }
                    }
                }
                else if (!((MetaInfo.Type)metaObject).generated) {
                    this.addToFlow(metaObject);
                }
            }
            else if (metaObject != null && metaObject.type == EntityType.VISUALIZATION) {
                try {
                    this.recompileVisualization((MetaInfo.Visualization)metaObject);
                }
                catch (Exception e) {
                    if (Context.logger.isDebugEnabled()) {
                        Context.logger.debug((Object)("---> added old object to flow " + metaObject.name));
                    }
                    this.addToFlow(metaObject);
                    if (this.curFlow != null) {
                        this.flowsFailed.add(this.curFlow.name);
                    }
                }
            }
            else if (metaObject != null && metaObject.type == EntityType.TARGET && metaObject.getSourceText() == null && ((MetaInfo.Target)metaObject).isSubscription()) {
                try {
                    this.recompileSubscription((MetaInfo.Target)metaObject);
                }
                catch (Exception e) {
                    recompileMessagesBookkeeping.put(metaObject.getFullName(), e.getMessage());
                    if (Context.logger.isDebugEnabled()) {
                        Context.logger.debug((Object)("---> added old object to flow " + metaObject.name));
                    }
                    this.addToFlow(metaObject);
                    if (this.curFlow != null) {
                        this.flowsFailed.add(this.curFlow.name);
                    }
                }
            }
            else if (metaObject != null && metaObject.getSourceText() != null) {
                try {
                    this.recompileObject(metaObject, recompileMessagesBookkeeping);
                }
                catch (Exception e) {
                    if (Context.logger.isDebugEnabled()) {
                        Context.logger.debug((Object)("---> added old object to flow " + metaObject.name));
                    }
                    this.addToFlow(metaObject);
                    if (this.curFlow != null) {
                        this.flowsFailed.add(this.curFlow.name);
                    }
                }
            }
            else if (Context.logger.isDebugEnabled()) {
                Context.logger.debug((Object)("Object's source text is null: " + uuidOfObject + " " + metaObject.name));
            }
        }
    }
    
    private void alterOrEndFlow(final MetaInfo.Flow flowOrApp, final boolean start) throws Exception {
        if (start) {
            this.executeTQL("ALTER " + flowOrApp.type + " " + flowOrApp.name + ";");
        }
        else {
            this.executeTQL("END " + flowOrApp.type + " " + flowOrApp.name + ";");
        }
    }
    
    private void recompile(final MetaInfo.Flow appMetaObject) throws MetaDataRepositoryException {
        final Map<String, String> recompileMessagesBookkeeping = new LinkedHashMap<String, String>();
        this.isAppValidAfterRecompile = true;
        this.flowsFailed.clear();
        if (appMetaObject.getMetaInfoStatus().isValid()) {
            return;
        }
        List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> recompileOrderOfObjectsBasedOnTopologicalSort = null;
        try {
            recompileOrderOfObjectsBasedOnTopologicalSort = appMetaObject.exportTQL(MetadataRepository.getINSTANCE(), HSecurityManager.TOKEN);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        final List<MetaInfo.Flow> openFlows = new ArrayList<MetaInfo.Flow>();
        try {
            this.createOrEndApplicationOrFlow(appMetaObject, true);
        }
        catch (Exception ex) {}
        if (appMetaObject.objects.get(EntityType.FLOW) != null) {
            for (final UUID flowUUID : appMetaObject.objects.get(EntityType.FLOW)) {
                final MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)this.getObject(flowUUID);
                if (flowMetaObject == null) {
                    continue;
                }
                if (flowMetaObject.getMetaInfoStatus().isValid()) {
                    this.addToFlow(flowMetaObject);
                }
                else {
                    openFlows.add(flowMetaObject);
                    try {
                        this.createOrEndApplicationOrFlow(flowMetaObject, true);
                    }
                    catch (Exception ex2) {}
                    try {
                        this.createOrEndApplicationOrFlow(flowMetaObject, false);
                    }
                    catch (Exception ex3) {}
                }
            }
        }
        try {
            this.createOrEndApplicationOrFlow(appMetaObject, false);
        }
        catch (Exception ex4) {}
        try {
            for (final Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : recompileOrderOfObjectsBasedOnTopologicalSort) {
                try {
                    this.alterOrEndFlow(pair.second, true);
                }
                catch (Exception ex5) {}
                this.recompileSpecificObject(pair.first.getUuid(), recompileMessagesBookkeeping);
                try {
                    this.alterOrEndFlow(pair.second, false);
                }
                catch (Exception ex6) {}
            }
        }
        catch (Exception e2) {
            if (Context.logger.isInfoEnabled()) {
                Context.logger.info((Object)e2.getMessage(), (Throwable)e2);
            }
        }
        if (!this.isAppValidAfterRecompile) {
            final MetaInfo.Flow newAppObj = this.get(appMetaObject.nsName + "." + appMetaObject.name, EntityType.APPLICATION);
            newAppObj.getMetaInfoStatus().setValid(false);
            this.updateMetaObject(newAppObj);
            for (final String flow : this.flowsFailed) {
                final MetaInfo.Flow newFlowObj = this.get(appMetaObject.nsName + "." + flow, EntityType.FLOW);
                newFlowObj.getMetaInfoStatus().setValid(false);
                this.updateMetaObject(newFlowObj);
            }
            final StringBuffer buffer = new StringBuffer();
            for (final Map.Entry<String, String> entry : recompileMessagesBookkeeping.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();
                buffer.append("For: " + key + " \nIssue Occurred: " + value + "\n");
            }
            throw new Warning(buffer.toString());
        }
    }
    
    public MetaInfo.Type getTypeInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getType(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.Stream getStreamInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getStream(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.MetaObject getDataSourceInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getDataSource(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.PropertySet getPropertySetInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getPropertySet(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.Source getSourceInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getSource(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.Target getTargetInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getTarget(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.CQ getCQInCurSchema(final String name) throws MetaDataRepositoryException {
        return this.getCQ(this.addSchemaPrefix(null, name));
    }
    
    public MetaInfo.Flow getFlowInCurSchema(final String name, final EntityType type) throws MetaDataRepositoryException {
        final String flowName = this.addSchemaPrefix(null, name);
        assert type == EntityType.APPLICATION;
        return this.get(flowName, type);
    }
    
    public void showStreamStmt(final MetaInfo.ShowStream show_stream) throws MetaDataRepositoryException {
        if (this.readOnly) {
            return;
        }
        this.metadataRepository.putShowStream(show_stream, HSecurityManager.TOKEN);
    }
    
    public AuthToken getSessionID() {
        return this.sessionID;
    }
    
    public MetaInfo.HDStore putHDStore(final boolean doReplace, final ObjectName objectName, final UUID typeid, final Interval howOften, final List<UUID> eventTypes, final List<String> eventKeys, final Map<String, Object> props) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.MetaObject dataSource = this.getDataSource(name);
        if (dataSource != null) {
            if (this.recompileMode) {
                try {
                    DropMetaObject.DropHDStore.removeIndex(this, dataSource, DropMetaObject.DropRule.NONE, this.sessionID);
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
                this.removeObject(dataSource);
                oldUUID = dataSource.uuid;
            }
            else {
                if (!doReplace) {
                    throw new CompilationException(dataSource.type + " with name <" + name + "> already exists");
                }
                this.dropObject(dataSource.name, dataSource.type, DropMetaObject.DropRule.NONE);
            }
        }
        MetaInfo.HDStore newwas = new MetaInfo.HDStore();
        newwas.construct(name, this.getNamespace(objectName.getNamespace()), typeid, howOften, eventTypes, eventKeys, props);
        try {
            if (this.recompileMode) {
                ((MetaInfo.HDStore)dataSource).removeGeneratedClasses();
            }
            newwas.generateClasses();
        }
        catch (Exception e2) {
            Context.logger.error((Object)"Problem generating classes: ", (Throwable)e2);
        }
        if (this.recompileMode && oldUUID != null) {
            newwas.uuid = oldUUID;
        }
        this.putHDStore(newwas);
        newwas = (MetaInfo.HDStore)this.getDataSource(name);
        return newwas;
    }
    
    public MetaInfo.User putUser(final String uname, final String password, final UserProperty userProperty) throws Exception {
        final MetaInfo.User newuser = new MetaInfo.User();
        newuser.construct(uname, password);
        MetaInfo.Namespace usernamespace = null;
        final List<String> role_name = userProperty.lroles;
        final String namespace = userProperty.defaultnamespace;
        final List<Property> attributes = userProperty.properties;
        newuser.setAlias(userProperty.getAlias());
        if (userProperty.ldap != null && !userProperty.ldap.isEmpty()) {
            newuser.setOriginType(MetaInfo.User.AUTHORIZATION_TYPE.LDAP);
            final String[] fullyQualifiedLdapName = this.splitNamespaceAndName(userProperty.ldap, EntityType.PROPERTYSET);
            newuser.setLdap(fullyQualifiedLdapName[0] + "." + fullyQualifiedLdapName[1]);
        }
        final List<String> lrole = new ArrayList<String>();
        if (role_name != null) {
            for (final String string : role_name) {
                final MetaInfo.Role r = this.security_manager.getRole(string.split(":")[0], string.split(":")[1]);
                if (r == null) {
                    throw new CompilationException("ROLE " + string + " does not exist");
                }
                lrole.add(r.getRole());
            }
        }
        final String user_namespace_name = (namespace == null) ? uname : namespace;
        usernamespace = this.getNamespace(user_namespace_name);
        if (usernamespace == null) {
            usernamespace = this.putNamespace(false, user_namespace_name);
        }
        newuser.setDefaultNamespace(user_namespace_name);
        if (attributes != null && attributes.size() > 0) {
            for (final Property attr : attributes) {
                this.setUserAttributes(newuser, attr.name, attr.value.toString());
            }
        }
        newuser.setOriginType(userProperty.originType);
        this.security_manager.addUser(newuser, this.sessionID);
        lrole.add(user_namespace_name + ":admin");
        this.security_manager.grantUserRoles(newuser.getUserId(), lrole, this.sessionID);
        this.grantDefaultUserPermissions(newuser);
        return newuser;
    }
    
    private void grantDefaultUserPermissions(final MetaInfo.User user) throws Exception {
        final List<ObjectPermission> defaultUserPermissions = new ArrayList<ObjectPermission>();
        final Set<String> domains = new HashSet<String>();
        final Set<ObjectPermission.Action> actions = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes = new HashSet<ObjectPermission.ObjectType>();
        final Set<String> names = new HashSet<String>();
        domains.add("Global");
        actions.add(ObjectPermission.Action.read);
        actions.add(ObjectPermission.Action.update);
        objectTypes.add(ObjectPermission.ObjectType.user);
        names.add(user.getUserId());
        final ObjectPermission readSelfPerm = new ObjectPermission(domains, actions, objectTypes, names);
        defaultUserPermissions.add(readSelfPerm);
        final MetaInfo.Role uRole = new MetaInfo.Role();
        final MetaInfo.Namespace uNamespace = this.getNamespace(user.getName());
        uRole.construct(uNamespace, "useradmin", null, defaultUserPermissions);
        this.security_manager.addRole(uRole, this.sessionID);
        final List<String> rlist = new ArrayList<String>();
        rlist.add(uRole.getRole());
        final MetaInfo.Role rSys = this.security_manager.getRole("Global", "systemuser");
        rlist.add(rSys.getRole());
        final MetaInfo.Role rUi = this.security_manager.getRole("Global", "uiuser");
        rlist.add(rUi.getRole());
        this.security_manager.grantUserRoles(user.getUserId(), rlist, this.sessionID);
    }
    
    private void setUserAttributes(final MetaInfo.User u, final String what, final String value) throws SecurityException, UnsupportedEncodingException, GeneralSecurityException {
        if (value == null) {
            return;
        }
        final List<MetaInfo.ContactMechanism> contactMechanismList = u.getContactMechanisms();
        final String upperCase = what.toUpperCase();
        switch (upperCase) {
            case "EMAIL": {
                for (final MetaInfo.ContactMechanism cm : contactMechanismList) {
                    if (cm.getType() == MetaInfo.ContactMechanism.ContactType.email) {
                        u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.email, value);
                        break;
                    }
                }
                u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.email, value);
                return;
            }
            case "SMS": {
                for (final MetaInfo.ContactMechanism cm : contactMechanismList) {
                    if (cm.getType() == MetaInfo.ContactMechanism.ContactType.sms) {
                        u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.sms, value);
                        break;
                    }
                }
                u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.sms, value);
                return;
            }
            case "PHONE": {
                for (final MetaInfo.ContactMechanism cm : contactMechanismList) {
                    if (cm.getType() == MetaInfo.ContactMechanism.ContactType.phone) {
                        u.updateContactMechanism(cm.getIndex(), MetaInfo.ContactMechanism.ContactType.phone, value);
                        break;
                    }
                }
                u.addContactMechanism(MetaInfo.ContactMechanism.ContactType.phone, value);
                return;
            }
            case "PASSWORD": {
                u.setEncryptedPassword(HSecurityManager.encrypt(value, u.uuid.toEightBytes()));
                return;
            }
            case "FIRSTNAME": {
                u.setFirstName(value);
                return;
            }
            case "LASTNAME": {
                u.setLastName(value);
                return;
            }
            case "MAINEMAIL": {
                u.setMainEmail(value);
                return;
            }
            case "TIMEZONE": {
                final TimeZone tz = TimeZone.getTimeZone(value);
                if (tz.getID().equals(value)) {
                    u.setUserTimeZone(value);
                    break;
                }
                throw new UnsupportedEncodingException("Unknown timezone - " + value);
            }
        }
        Context.logger.warn((Object)("Cannot set unknown user attribute: " + what));
    }
    
    public MetaInfo.Visualization putVisualization(final String objectName, final String fname) throws MetaDataRepositoryException {
        UUID oldUUID = null;
        final File file = new File(fname);
        Scanner scanner = null;
        try {
            scanner = new Scanner(file);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String json = "";
        if (scanner != null) {
            while (scanner.hasNextLine()) {
                json += scanner.nextLine();
            }
            scanner.close();
            final String visualizationName = this.addSchemaPrefix(null, objectName);
            final MetaInfo.Visualization visualizationMetaObject = this.get(visualizationName, EntityType.VISUALIZATION);
            if (visualizationMetaObject != null) {
                if (this.recompileMode) {
                    if (visualizationMetaObject != null) {
                        oldUUID = visualizationMetaObject.uuid;
                        this.removeObject(visualizationMetaObject);
                    }
                }
                else {
                    DropMetaObject.DropVisualization.drop(this, visualizationMetaObject, DropMetaObject.DropRule.NONE, this.sessionID);
                }
            }
            MetaInfo.Visualization newvisualization = new MetaInfo.Visualization();
            newvisualization.construct(visualizationName, this.getNamespace(null), json, fname);
            if (this.recompileMode && oldUUID != null) {
                newvisualization.uuid = oldUUID;
            }
            this.putVisualization(newvisualization);
            newvisualization = (MetaInfo.Visualization)this.getObject(newvisualization.uuid);
            return newvisualization;
        }
        throw new RuntimeException("File creator is failing.");
    }
    
    public void UpdateUserInfoStmt(final String username, final Map<String, Object> props_toupdate) throws Exception {
        if (props_toupdate == null || props_toupdate.isEmpty()) {
            return;
        }
        final MetaInfo.User u = this.get(username, EntityType.USER);
        if (u == null) {
            throw new RuntimeException("User " + username + " is not found.");
        }
        for (final String key : props_toupdate.keySet()) {
            if (key.equalsIgnoreCase("oldpassword")) {
                continue;
            }
            if (key.equalsIgnoreCase("password") || key.equalsIgnoreCase("newpassword")) {
                final ObjectPermission permission = new ObjectPermission(u.getNsName(), ObjectPermission.Action.update, ObjectPermission.ObjectType.user, u.getName());
                if (!this.security_manager.isAllowed(this.getAuthToken(), permission)) {
                    final String providedOldPassword = (String)props_toupdate.get("oldpassword");
                    if (providedOldPassword == null) {
                        throw new Exception("Must provide 'oldpassword'");
                    }
                    final String userOldPasswordEncrypted = u.getEncryptedPassword();
                    final String providedOldPasswordEncrypted = HSecurityManager.encrypt(providedOldPassword, u.uuid.toEightBytes());
                    if (!userOldPasswordEncrypted.equals(providedOldPasswordEncrypted)) {
                        throw new Exception("Old password is incorrect");
                    }
                }
                final String value = (String)props_toupdate.get(key);
                this.setUserAttributes(u, "password", value);
            }
            else {
                final String value2 = (String)props_toupdate.get(key);
                this.setUserAttributes(u, key, value2);
            }
        }
        this.security_manager.updateUser(u, this.sessionID);
        if (HazelcastSingleton.isClientMember() && Tungsten.currUserMetaInfo.getName().equals(u.getName())) {
            final TimeZone jtz = TimeZone.getTimeZone(u.getUserTimeZone());
            Tungsten.userTimeZone = (u.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz));
        }
    }
    
    public MetaInfo.Role putRole(final String rname) throws Exception {
        final String[] part = rname.split(":");
        final MetaInfo.Namespace ns = this.getNamespace(part[0]);
        if (ns == null) {
            throw new SecurityException("No permission to create role " + part[0] + "." + part[1]);
        }
        final MetaInfo.Role newRole = new MetaInfo.Role();
        newRole.construct(ns, part[1]);
        this.security_manager.addRole(newRole, this.sessionID);
        return newRole;
    }
    
    private String objectFullName(final EntityType objType, final String objectName) {
        String name;
        if (objType == EntityType.DG) {
            name = objectName;
        }
        else if (objType != EntityType.NAMESPACE && objectName.split("\\.").length == 1) {
            name = this.addSchemaPrefix(null, objectName);
        }
        else {
            name = objectName;
        }
        return name;
    }
    
    private MetaInfo.MetaObject doesObjectExists(final EntityType objType, final String objectName) throws MetaDataRepositoryException {
        final MetaInfo.MetaObject obj = this.get(objectName, objType);
        if (obj == null) {
            return null;
        }
        return obj;
    }
    
    private MetaInfo.MetaObject getUserOrRole(final String objName, final EntityType objType) {
        MetaInfo.MetaObject metaObject = null;
        if (objType == EntityType.USER) {
            try {
                metaObject = this.security_manager.getUser(objName);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        else if (objType == EntityType.ROLE) {
            try {
                metaObject = this.security_manager.getRole(objName);
            }
            catch (Exception ex) {}
        }
        return metaObject;
    }
    
    public List<String> dropObject(final String objName, final EntityType objType, final DropMetaObject.DropRule dropRule) throws MetaDataRepositoryException {
        List<String> resultString = new ArrayList<String>();
        final MetaInfo.Namespace currentNamespace = this.getNamespace(null);
        final String fullObjectName = this.objectFullName(objType, objName);
        MetaInfo.MetaObject objectToDrop = null;
        if (this.metadataRepository == null) {
            resultString.add("MetaData Cache is not initialized!");
            return resultString;
        }
        if (currentNamespace == null) {
            resultString.add("Current namespace is not set!");
            return resultString;
        }
        if (objType == EntityType.ROLE || objType == EntityType.USER) {
            objectToDrop = this.getUserOrRole(objName, objType);
        }
        if (objType != EntityType.ROLE && objType != EntityType.USER && (objectToDrop = this.doesObjectExists(objType, fullObjectName)) == null) {
            throw new Warning(objType + " " + objName + " does not exist");
        }
        switch (objType) {
            case ALERTSUBSCRIBER: {
                break;
            }
            case APPLICATION: {
                resultString = DropMetaObject.DropApplication.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case CACHE: {
                resultString = DropMetaObject.DropCache.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case CQ: {
                resultString = DropMetaObject.DropCQ.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case DG: {
                resultString = DropMetaObject.DropDG.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case FLOW: {
                resultString = DropMetaObject.DropFlow.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case INITIALIZER: {
                break;
            }
            case NAMESPACE: {
                if (this.getNamespace(null).name.equalsIgnoreCase(this.splitName(objName))) {
                    throw new FatalException("Namespace cannot be dropped while in use; Use different namespace to drop it.");
                }
                if (ProtectedNamespaces.getEnum(this.splitName(objName)) != null) {
                    throw new FatalException("Namespace " + objName + " cannot be dropped.");
                }
                resultString = DropMetaObject.DropNamespace.drop(this, objectToDrop, dropRule, this.sessionID);
                this.curFlow = null;
                this.curApp = null;
                break;
            }
            case PROPERTYSET: {
                resultString = DropMetaObject.DropPropertySet.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case PROPERTYVARIABLE: {
                resultString = DropMetaObject.DropPropertyVariable.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case PROPERTYTEMPLATE: {
                break;
            }
            case ROLE: {
                resultString = DropMetaObject.DropRole.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case SERVER: {
                break;
            }
            case SOURCE: {
                resultString = DropMetaObject.DropSource.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case STREAM: {
                resultString = DropMetaObject.DropStream.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case STREAM_GENERATOR: {
                break;
            }
            case TARGET: {
                resultString = DropMetaObject.DropTarget.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case TYPE: {
                resultString = DropMetaObject.DropType.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case USER: {
                resultString = DropMetaObject.DropUser.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case VISUALIZATION: {
                resultString = DropMetaObject.DropVisualization.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case HDSTORE: {
                resultString = DropMetaObject.DropHDStore.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case WINDOW: {
                resultString = DropMetaObject.DropWindow.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case DASHBOARD: {
                resultString = DropMetaObject.DropDashboard.drop(this, objectToDrop, dropRule, this.sessionID);
                break;
            }
            case UNKNOWN: {
                resultString.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
                return resultString;
            }
            case QUERY: {
                if (!(objectToDrop instanceof MetaInfo.Query)) {
                    throw new Warning("Wrong type to drop, trying to drop Query, but actually " + objectToDrop.type);
                }
                if (((MetaInfo.Query)objectToDrop).isAdhocQuery()) {
                    throw new Warning("Query is adhoc query not NamedQuery.");
                }
                this.deleteQuery((MetaInfo.Query)objectToDrop, this.sessionID);
                resultString.add(objectToDrop.type + " " + objectToDrop.name + " dropped successfully\n");
                break;
            }
            default: {
                resultString.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
                return resultString;
            }
        }
        return resultString;
    }
    
    private String splitName(final String name) {
        final String[] splitName = name.split("\\.");
        if (splitName.length == 2) {
            return splitName[1];
        }
        return name;
    }
    
    public void GrantRoleToStmt(final GrantRoleToStmt stmt) {
        MetaInfo.User toUser = null;
        MetaInfo.Role toRole = null;
        try {
            if (stmt.towhat == EntityType.USER) {
                toUser = this.security_manager.getUser(stmt.name);
            }
            if (stmt.towhat == EntityType.ROLE) {
                toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
            }
            final List<String> rlist = new ArrayList<String>();
            for (final String rolename : stmt.rolename) {
                final MetaInfo.Role r = this.security_manager.getRole(rolename.split(":")[0], rolename.split(":")[1]);
                rlist.add(r.getRole());
            }
            if (toUser != null) {
                this.security_manager.grantUserRoles(toUser.getUserId(), rlist, this.sessionID);
            }
            else {
                this.security_manager.grantRoleRoles(toRole.getRole(), rlist, this.sessionID);
            }
        }
        catch (Exception e) {
            throw new FatalException(ErrorCode.Error.GRANTROLEFAILURE.getDescription());
        }
    }
    
    public void GrantPermissionToStmt(final GrantPermissionToStmt stmt, final List<String> permissions) {
        MetaInfo.User toUser = null;
        MetaInfo.Role toRole = null;
        try {
            if (stmt.towhat == EntityType.USER) {
                toUser = this.security_manager.getUser(stmt.name);
                if (toUser == null) {
                    throw new RuntimeException("Invalid user name: " + stmt.name);
                }
            }
            if (stmt.towhat == EntityType.ROLE) {
                toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
                if (toRole == null) {
                    throw new RuntimeException("Invalid role name: " + stmt.name);
                }
            }
            for (final String perm : permissions) {
                final ObjectPermission op = new ObjectPermission(perm);
                if (toUser != null) {
                    this.security_manager.grantUserPermission(toUser.getUserId(), op, this.sessionID);
                }
                else {
                    this.security_manager.grantRolePermission(toRole.getRole(), op, this.sessionID);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error granting permission to " + stmt.towhat + " : " + stmt.name + ". Message : " + e.getMessage());
        }
    }
    
    public void RevokePermissionFromStmt(final RevokePermissionFromStmt stmt, final List<String> permissions) {
        MetaInfo.User toUser = null;
        MetaInfo.Role toRole = null;
        try {
            if (stmt.towhat == EntityType.USER) {
                toUser = this.security_manager.getUser(stmt.name);
                if (toUser == null) {
                    throw new RuntimeException("Invalid user name: " + stmt.name);
                }
            }
            if (stmt.towhat == EntityType.ROLE) {
                toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
                if (toRole == null) {
                    throw new RuntimeException("Invalid role name: " + stmt.name);
                }
            }
            for (final String perm : permissions) {
                final ObjectPermission op = new ObjectPermission(perm);
                if (toUser != null) {
                    this.security_manager.revokeUserPermission(toUser.getUserId(), op, this.sessionID);
                }
                else {
                    this.security_manager.revokeRolePermission(toRole.getRole(), op, this.sessionID);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error granting permission to " + stmt.towhat + " : " + stmt.name + ". Message : " + e.getMessage());
        }
    }
    
    public void RevokeRoleFromStmt(final RevokeRoleFromStmt stmt) {
        MetaInfo.User toUser = null;
        MetaInfo.Role toRole = null;
        try {
            if (stmt.towhat == EntityType.USER) {
                toUser = this.security_manager.getUser(stmt.name);
            }
            if (stmt.towhat == EntityType.ROLE) {
                toRole = this.security_manager.getRole(stmt.name.split(":")[0], stmt.name.split(":")[1]);
            }
            final List<String> rlist = new ArrayList<String>();
            for (final String rolename : stmt.rolename) {
                final MetaInfo.Role r = this.security_manager.getRole(rolename.split(":")[0], rolename.split(":")[1]);
                rlist.add(r.getRole());
            }
            if (toUser != null) {
                this.security_manager.revokeUserRoles(toUser.getUserId(), rlist, this.sessionID);
            }
            else {
                this.security_manager.revokeRoleRoles(toRole.getRole(), rlist, this.sessionID);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error revoking role: " + stmt.rolename + ". Message : " + e.getMessage());
        }
    }
    
    public synchronized AuthToken Connect(final String uname, final String password, final String clusterName, final String host) throws MetaDataRepositoryException {
        if (clusterName != null && !clusterName.isEmpty()) {
            final HazelcastInstance in = HazelcastSingleton.initIfPopulated(clusterName, host);
            if (in == null) {
                throw new MetaDataRepositoryException("Cluster " + clusterName + " not found");
            }
            final boolean isCleared = MetadataRepository.getINSTANCE().clear(false);
            if (!isCleared) {
                Context.logger.warn((Object)"Failed to clear the MDR when changing clusters.");
            }
            MonitorModel.resetDbConnection();
        }
        AuthToken session_id;
        try {
            final String clientId = HazelcastSingleton.get().getLocalEndpoint().getUuid();
            session_id = this.security_manager.authenticate(uname, password, clientId, "Tungsten");
        }
        catch (Exception ex) {
            throw new MetaDataRepositoryException(ex.getLocalizedMessage());
        }
        if (session_id != null) {
            System.out.println("Successfully connected as " + uname);
            if (this.sessionID != null) {
                this.security_manager.logout(this.sessionID);
            }
            final IMap<UUID, UUID> nodeIDToAuthToken = HazelcastSingleton.get().getMap("#nodeIDToAuthToken");
            nodeIDToAuthToken.put(HazelcastSingleton.getNodeId(), session_id);
            Tungsten.checkAndCleanupAdhoc(false);
            Tungsten.setSessionQueue(HQueue.getQueue("consoleQueue" + session_id));
            Tungsten.setQueuelistener(new HQueue.Listener() {
                @Override
                public void onItem(final Object item) {
                    if (Tungsten.isAdhocRunning.get()) {
                        Tungsten.prettyPrintEvent(item);
                    }
                }
            });
            try {
                Tungsten.getSessionQueue().subscribeForTungsten(Tungsten.getQueuelistener());
            }
            catch (Exception e) {
                Context.logger.error((Object)e.getMessage(), (Throwable)e);
            }
            Tungsten.session_id = session_id;
            Tungsten.currUserMetaInfo = (MetaInfo.User)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", uname, null, session_id);
            final TimeZone jtz = TimeZone.getTimeZone(Tungsten.currUserMetaInfo.getUserTimeZone());
            Tungsten.userTimeZone = (Tungsten.currUserMetaInfo.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz));
            this.sessionID = session_id;
            this.curUser = this.getUser(uname);
            final String curUsrNamespace = this.curUser.getDefaultNamespace();
            this.useNamespace(curUsrNamespace);
            ConsoleReader.clearHistory();
            return session_id;
        }
        Context.logger.info((Object)("Could not log in as as " + uname));
        return null;
    }
    
    public String getCurUser() {
        if (this.curUser != null) {
            return this.curUser.name;
        }
        return null;
    }
    
    public String endBlock(final MetaInfo.Flow flow) {
        if (flow.type == EntityType.FLOW) {
            if (this.curFlow == null) {
                return "END FLOW <" + flow.name + "> block has no matching CREATE FLOW <" + flow.name + ">";
            }
            if (!this.curFlow.name.equalsIgnoreCase(flow.name)) {
                return "END FLOW <" + flow.name + "> block does not match previous CREATE FLOW <" + this.curFlow.name + ">";
            }
            this.curFlow = null;
        }
        else {
            if (this.curApp == null) {
                return "END APPLICATION <" + flow.name + "> block has no matching CREATE APPLICATION <" + flow.name + ">";
            }
            if (!this.curApp.name.equalsIgnoreCase(flow.name)) {
                return "END APPLICATION <" + flow.name + "> block does not match previous CREATE APPLICATION <" + this.curApp.name + ">";
            }
            if (this.curFlow != null) {
                return "there is no END FLOW <" + this.curFlow.name + "> statement before END APPLICATION <" + flow.name + ">";
            }
            this.curApp = null;
        }
        return null;
    }
    
    public JsonNode executeReportAction(final UUID appID, final int action, final String fileName, final boolean retAsJson, final AuthToken clToken) throws Exception {
        final MetaInfo.StatusInfo si = this.metadataRepository.getStatusInfo(appID, HSecurityManager.TOKEN);
        if (si == null || (si.getStatus() != MetaInfo.StatusInfo.Status.DEPLOYED && si.getStatus() != MetaInfo.StatusInfo.Status.RUNNING)) {
            throw new Exception("Unable to execute report action on application as it is not deployed");
        }
        if (action == 1) {
            if (this.reportingApp != null) {
                final MetaInfo.MetaObject repObj = this.metadataRepository.getMetaObjectByUUID(this.reportingApp, HSecurityManager.TOKEN);
                if (repObj == null) {
                    throw new Exception("Invalid application object.");
                }
                if (!repObj.getMetaInfoStatus().isDropped()) {
                    throw new Exception("Report already started, Stop first.");
                }
            }
            this.reportingApp = appID;
        }
        else if (action == 2) {
            if (this.reportingApp == null || !this.reportingApp.equals((Object)appID)) {
                throw new Exception("No report in progress for this application. Please start first.");
            }
            this.reportingApp = null;
        }
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        final Collection<ReportStats.BaseReportStats> list = DistributedExecutionManager.exec(hz, (Callable<ReportStats.BaseReportStats>)new ReportStmtRemoteCall(appID, action, clToken, fileName), srvs);
        if (action == 1) {
            return null;
        }
        long totalEvents = 0L;
        long totalHDs = 0L;
        String lastHDStr = null;
        long lastHDTS = -1L;
        long totalEventsOutput = 0L;
        long lastEventOutputTS = -1L;
        long firstEventTS = -1L;
        String firstEvent = null;
        long lastEventTS = -1L;
        String lastEvent = null;
        String lastEventAtTarget = null;
        long lastCheckpointTS = -1L;
        for (final ReportStats.BaseReportStats baseStats : list) {
            if (baseStats == null) {
                continue;
            }
            final ReportStats.FlowReportStats flStats = (ReportStats.FlowReportStats)baseStats;
            final List<ReportStats.SourceReportStats> srcList = flStats.srcStats;
            for (final ReportStats.SourceReportStats srcStats : srcList) {
                totalEvents += srcStats.getEventsSeen();
                if (firstEventTS == -1L || srcStats.getFirstEventTS() < firstEventTS) {
                    firstEventTS = srcStats.getFirstEventTS();
                    firstEvent = srcStats.getFirstEventStr();
                }
                if (lastEventTS == -1L || lastEventTS < srcStats.getLastEventTS()) {
                    lastEventTS = srcStats.getLastEventTS();
                    lastEvent = srcStats.getLastEventStr();
                }
            }
            final List<ReportStats.HDstoreReportStats> wsStatList = flStats.wsStats;
            for (final ReportStats.HDstoreReportStats wsStats : wsStatList) {
                totalHDs += wsStats.getHDsSeen();
                if (lastHDTS == -1L || wsStats.getLasthdTS() > lastHDTS) {
                    lastHDTS = wsStats.getLasthdTS();
                    lastHDStr = wsStats.getLastHDStr();
                }
            }
            final List<ReportStats.TargetReportStats> tgtStatList = flStats.tgtStats;
            for (final ReportStats.TargetReportStats tgtStats : tgtStatList) {
                totalEventsOutput += tgtStats.getEventsOutput();
                if (lastEventOutputTS == -1L || tgtStats.getLastEventTS() > lastEventOutputTS) {
                    lastEventOutputTS = tgtStats.getLastEventTS();
                    lastEventAtTarget = tgtStats.getLastEventStr();
                }
            }
            if (lastCheckpointTS != -1L && flStats.getCheckpointTS() <= lastCheckpointTS) {
                continue;
            }
            lastCheckpointTS = flStats.getCheckpointTS();
        }
        final StringBuilder bldr = new StringBuilder();
        final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
        JsonNode jNode = null;
        ObjectNode retNode = null;
        if (retAsJson) {
            retNode = jsonMapper.createObjectNode();
        }
        if (totalEvents > 0L) {
            if (retAsJson) {
                retNode.put("TotalEvents", totalEvents);
            }
            else {
                bldr.append("Total Events - " + totalEvents + "\n");
            }
            if (retAsJson) {
                retNode.put("FirstEventTime", firstEventTS);
            }
            else {
                bldr.append("First Event Time - " + new Time(firstEventTS).toString() + "\n");
            }
            jNode = jsonMapper.readTree(firstEvent);
            ((ObjectNode)jNode).remove("_id");
            ((ObjectNode)jNode).remove("originTimeStamp");
            ((ObjectNode)jNode).remove("key");
            ((ObjectNode)jNode).remove("sourceUUID");
            ((ObjectNode)jNode).put("timeStamp", new DateTime(jNode.get("timeStamp").asLong()).toString());
            if (retAsJson) {
                retNode.put("FirstEvent", jNode.toString());
            }
            else {
                bldr.append("First Event - " + jNode.toString() + "\n");
            }
            if (retAsJson) {
                retNode.put("LastEventTime", lastEventTS);
            }
            else {
                bldr.append("Last Event Time - " + new Time(lastEventTS).toString() + "\n");
            }
            jNode = jsonMapper.readTree(lastEvent);
            ((ObjectNode)jNode).remove("_id");
            ((ObjectNode)jNode).remove("originTimeStamp");
            ((ObjectNode)jNode).remove("key");
            ((ObjectNode)jNode).remove("sourceUUID");
            ((ObjectNode)jNode).put("timeStamp", new DateTime(jNode.get("timeStamp").asLong()).toString());
            if (retAsJson) {
                retNode.put("LastEvent", jNode.toString());
            }
            else {
                bldr.append("Last Event - " + jNode.toString() + "\n");
            }
            if (lastEventTS > firstEventTS) {
                if (retAsJson) {
                    retNode.put("AverageThroughput", totalEvents / (lastEventTS - firstEventTS) * 1000L);
                }
                else {
                    bldr.append("Average Throughput for this interval - " + totalEvents / (lastEventTS - firstEventTS) * 1000L + "\n");
                }
            }
        }
        else {
            bldr.append("No events seen at sources for this reporting interval.\n");
        }
        if (totalHDs > 0L) {
            if (retAsJson) {
                retNode.put("TotalHDsCreated", totalHDs);
            }
            else {
                bldr.append("Total HDs created - " + totalHDs + "\n");
            }
            if (retAsJson) {
                retNode.put("LastHDTime", lastHDTS);
            }
            else {
                bldr.append("Last HD created at - " + new Time(lastHDTS).toString() + "\n");
            }
            jNode = jsonMapper.readTree(lastHDStr);
            ((ObjectNode)jNode).remove("_id");
            ((ObjectNode)jNode).remove("id");
            ((ObjectNode)jNode).remove("uuid");
            ((ObjectNode)jNode).put("hdTs", new DateTime(jNode.get("hdTs").asLong()).toString());
            if (retAsJson) {
                retNode.put("LastHD", jNode.toString());
            }
            else {
                bldr.append("Last HD - " + jNode.toString() + "\n");
            }
        }
        if (totalEventsOutput > 0L) {
            if (retAsJson) {
                retNode.put("TotalEventsOutput", totalEventsOutput);
            }
            else {
                bldr.append("Total Events output at target - " + totalEventsOutput + "\n");
            }
            if (retAsJson) {
                retNode.put("LastEventTime", lastEventOutputTS);
            }
            else {
                bldr.append("Last Event Seen at target at - " + new Time(lastEventOutputTS).toString() + "\n");
            }
            jNode = jsonMapper.readTree(lastEventAtTarget);
            ((ObjectNode)jNode).remove("_id");
            ((ObjectNode)jNode).remove("originTimeStamp");
            ((ObjectNode)jNode).remove("key");
            ((ObjectNode)jNode).remove("sourceUUID");
            ((ObjectNode)jNode).put("timeStamp", new DateTime(jNode.get("timeStamp").asLong()).toString());
            if (retAsJson) {
                retNode.put("LastEventOutput", jNode.toString());
            }
            else {
                bldr.append("Last Event Output at target - " + jNode.toString() + "\n");
            }
        }
        if (lastCheckpointTS != -1L) {
            if (retAsJson) {
                retNode.put("LastCheckpointTime", lastCheckpointTS);
            }
            else {
                bldr.append("Last Checkpoint taken by the app at - " + new Time(lastCheckpointTS).toString() + "\n");
            }
        }
        if (retAsJson) {
            return (JsonNode)retNode;
        }
        final String outString = bldr.toString();
        System.out.println(outString);
        if (fileName != null) {
            final File outFile = new File(fileName);
            final FileOutputStream fos = new FileOutputStream(outFile);
            final PrintStream ps = new PrintStream(fos);
            ps.println(outString);
        }
        return null;
    }
    
    public void changeCQInputOutput(final String cqname, final int mode, final int limit, final boolean turnoff, final AuthToken clToken) throws MetaDataRepositoryException {
        MetaInfo.CQ cqMeta = null;
        final String[] sNames = this.splitNamespaceAndName(cqname, EntityType.CQ);
        try {
            cqMeta = (MetaInfo.CQ)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.CQ, sNames[0], sNames[1], -1, this.getAuthToken());
            if (cqMeta == null) {
                return;
            }
            PermissionUtility.checkPermission(cqMeta, ObjectPermission.Action.update, this.getAuthToken(), true);
        }
        catch (Exception e) {
            if (Context.logger.isDebugEnabled()) {
                Context.logger.debug((Object)("Unable to find object or no permissions for " + cqname));
            }
            throw new MetaDataRepositoryException("Unable to find object or no permissions for " + cqname);
        }
        this.execute(new DumpStmtRemoteCall(cqMeta.getUuid(), mode, limit, turnoff, clToken));
    }
    
    public Collection<Result> execute(final Callable<Result> action) {
        if (this.readOnly) {
            return Collections.emptyList();
        }
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        return DistributedExecutionManager.exec(hz, action, srvs);
    }
    
    public Collection<Result> executeOnOne(final Callable<Result> action) {
        if (this.readOnly) {
            return Collections.emptyList();
        }
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
        return DistributedExecutionManager.exec(hz, action, srvs);
    }
    
    public Collection<Result> SetStmtRemoteCall(final String paramname, final Object paramvalue) {
        return this.execute(new SetStmtRemoteCall(paramname, paramvalue));
    }
    
    public Collection<Result> changeFlowState(final ActionType what, final MetaInfo.Flow flow) {
        return this.execute(new ChangeFlowState(what, flow, null, (UUID)this.sessionID, null, null));
    }
    
    public Collection<Result> remoteCallOnObject(final ActionType what, final MetaInfo.MetaObject object, final Object... loadDetails) {
        return this.executeOnOne(new RemoteCallOnObject(what, object, this.sessionID, loadDetails));
    }
    
    public Collection<Result> changeFlowState(final ActionType what, final MetaInfo.Flow flow, final List<Property> params) {
        return this.execute(new ChangeFlowState(what, flow, null, (UUID)this.sessionID, params, null));
    }
    
    public ChangeApplicationStateResponse changeApplicationState(final ActionType what, final MetaInfo.Flow flow, final Map<String, Object> params) throws Exception {
        final AppManagerRequestClient appManagerRequestHandler = new AppManagerRequestClient(this.sessionID);
        final ChangeApplicationStateResponse response = appManagerRequestHandler.sendRequest(what, flow, params);
        appManagerRequestHandler.close();
        return response;
    }
    
    public Collection<Object> executeShutdown(final String where) {
        if (this.readOnly) {
            return Collections.emptyList();
        }
        final HazelcastInstance hz = HazelcastSingleton.get();
        Set<Member> srvs;
        if (where.equalsIgnoreCase("ALL")) {
            srvs = DistributedExecutionManager.getAllServers(hz);
        }
        else {
            srvs = DistributedExecutionManager.getServerByAddress(hz, where);
            if (srvs.isEmpty()) {
                throw new RuntimeException("cannot find server " + where);
            }
        }
        return DistributedExecutionManager.exec(hz, (Callable<Object>)new ShutDown(), srvs);
    }
    
    public String getStatus(final String name, final EntityType type) throws Exception {
        final MetaInfo.Flow app = this.getFlowInCurSchema(name, type);
        if (app == null) {
            throw new RuntimeException("cannot find " + type + " " + name);
        }
        final MetaInfo.StatusInfo.Status si = FlowUtil.getCurrentStatusFromAppManager(app.getUuid());
        if (si != null) {
            return si.name();
        }
        return "UNKNOWN";
    }
    
    public Set<String> getErrors(final String name, final EntityType type, final AuthToken token) throws Exception {
        final MetaInfo.Flow app = this.getFlowInCurSchema(name, type);
        if (app == null) {
            throw new RuntimeException("cannot find " + type + " " + name);
        }
        final Set<String> errors = FlowUtil.getCurrentErrorsFromAppManager(app.getUuid(), token);
        return errors;
    }
    
    public void setCurApp(final MetaInfo.Flow flow) {
        this.curApp = flow;
    }
    
    public void setCurFlow(final MetaInfo.Flow flow) {
        this.curFlow = flow;
    }
    
    public MetaInfo.Flow getCurApp() {
        return this.curApp;
    }
    
    public MetaInfo.Flow getCurFlow() {
        return this.curFlow;
    }
    
    public MetaInfo.Sorter putSorter(final boolean doReplace, final ObjectName objectName, final Interval sortTimeInterval, final List<MetaInfo.Sorter.SorterRule> inOutRules, final UUID errorStream) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(objectName);
        final MetaInfo.Sorter sorter = this.getSorter(name);
        if (sorter != null) {
            if (!doReplace) {
                throw new CompilationException("Stream sorter " + name + " already exists");
            }
            this.removeObject(sorter.uuid);
        }
        final MetaInfo.Sorter newsorter = new MetaInfo.Sorter();
        newsorter.construct(name, this.getNamespace(objectName.getNamespace()), sortTimeInterval, inOutRules, errorStream);
        this.putSorter(newsorter);
        return newsorter;
    }
    
    public MetaInfo.WAStoreView putWAStoreView(final String waStoreName, final String namespaceName, final UUID wastoreID, final IntervalPolicy windowLen, final Boolean isJumping, final boolean subscribeToUpdates, final boolean isAdhoc) throws MetaDataRepositoryException {
        final String name = this.addSchemaPrefix(namespaceName, waStoreName);
        MetaInfo.WAStoreView view = new MetaInfo.WAStoreView();
        view.construct(name, this.getNamespace(namespaceName), wastoreID, windowLen, isJumping, subscribeToUpdates);
        view.getMetaInfoStatus().setAdhoc(isAdhoc);
        this.putWAStoreView(view);
        view = (MetaInfo.WAStoreView)this.getMetaObject(view.getUuid());
        final MetaInfo.MetaObject hdStore = this.getMetaObject(wastoreID);
        hdStore.addReverseIndexObjectDependencies(view.getUuid());
        this.updateMetaObject(hdStore);
        return view;
    }
    
    public List<MetaInfo.Query> listQueries(final QueryManager.TYPE[] entityTypes, final AuthToken authToken) throws MetaDataRepositoryException {
        if (entityTypes == null) {
            return null;
        }
        final List<MetaInfo.Query> resultSet = new ArrayList<MetaInfo.Query>();
        final Set<MetaInfo.Query> metaDataQueryResult = (Set<MetaInfo.Query>)this.metadataRepository.getByEntityType(EntityType.QUERY, authToken);
        for (final QueryManager.TYPE entityType : entityTypes) {
            for (final MetaInfo.Query query : metaDataQueryResult) {
                if (query.isAdhocQuery() && entityType == QueryManager.TYPE.ADHOC) {
                    resultSet.add(query);
                }
                if (!query.isAdhocQuery() && entityType == QueryManager.TYPE.NAMEDQUERY) {
                    resultSet.add(query);
                }
            }
        }
        return resultSet;
    }
    
    public void putQuery(final MetaInfo.Query query) {
        try {
            this.putMetaObject(query);
        }
        catch (MetaDataRepositoryException e) {
            Context.logger.warn((Object)e.getMessage());
        }
    }
    
    public void deleteQuery(final MetaInfo.Query query, final AuthToken authToken) throws MetaDataRepositoryException {
        if (query.appUUID != null) {
            final MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)this.getMetaObject(query.appUUID);
            if (applicationMetaObject == null) {
                throw new RuntimeException("Application MetaObject not found");
            }
            final List<UUID> deps = applicationMetaObject.getDependencies();
            this.removeMetaObject(applicationMetaObject.uuid);
            for (final UUID dep : deps) {
                final MetaInfo.MetaObject obj = this.getObject(dep);
                if (obj instanceof MetaInfo.WAStoreView) {
                    final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)obj;
                    final MetaInfo.HDStore wastore = (MetaInfo.HDStore)this.getMetaObject(wastoreview.wastoreID);
                    try {
                        wastore.getReverseIndexObjectDependencies().remove(dep);
                        this.updateMetaObject(wastore);
                    }
                    catch (Exception ex) {}
                }
                this.removeMetaObject(dep);
            }
        }
        this.removeMetaObject(query.uuid);
    }
    
    public UUID createDashboard(final String json, final String toNamespace) throws MetaDataRepositoryException {
        VisualizationArtifacts visualizationArtifacts = null;
        try {
            visualizationArtifacts = (VisualizationArtifacts)new ObjectMapper().readValue(json, (Class)VisualizationArtifacts.class);
        }
        catch (IOException e6) {
            final List<JsonNode> qvObjects = new ArrayList<JsonNode>();
            final List<JsonNode> pageObjects = new ArrayList<JsonNode>();
            visualizationArtifacts = new VisualizationArtifacts();
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            JsonNode listNode = null;
            try {
                listNode = jsonMapper.readTree(json);
            }
            catch (IOException e1) {
                e1.printStackTrace();
                return null;
            }
            final Iterator<JsonNode> it = (Iterator<JsonNode>)listNode.elements();
            while (it.hasNext()) {
                final JsonNode node = it.next();
                if (node instanceof ArrayNode) {
                    for (final JsonNode jsonNode : node) {
                        try {
                            this.extractInfo(visualizationArtifacts, qvObjects, pageObjects, jsonMapper, jsonNode);
                        }
                        catch (ClassNotFoundException | IOException ex3) {
                            Context.logger.warn((Object)ex3.getMessage());
                            return null;
                        }
                    }
                }
                if (node instanceof ObjectNode) {
                    try {
                        this.extractInfo(visualizationArtifacts, qvObjects, pageObjects, jsonMapper, node);
                    }
                    catch (ClassNotFoundException | IOException ex4) {
                        Context.logger.warn((Object)ex4.getMessage());
                        return null;
                    }
                }
            }
            try {
                final List<MetaInfo.MetaObject> convertedDashboardObjects = ServerUpgradeUtility.DashboardConverter.convert(qvObjects, pageObjects);
                for (final MetaInfo.MetaObject mo : convertedDashboardObjects) {
                    if (mo.getType() == EntityType.QUERYVISUALIZATION) {
                        visualizationArtifacts.addQueryVisualization((MetaInfo.QueryVisualization)mo);
                    }
                    if (mo.getType() == EntityType.PAGE) {
                        visualizationArtifacts.addPages((MetaInfo.Page)mo);
                    }
                }
            }
            catch (IOException e4) {
                Context.logger.warn((Object)e4.getMessage());
                return null;
            }
        }
        if (visualizationArtifacts == null || visualizationArtifacts.getDashboard() == null || visualizationArtifacts.getPages().contains(null) || visualizationArtifacts.getQueryVisualizations().contains(null)) {
            throw new RuntimeException("Failed to create Dashboard object, there are missing objects.");
        }
        if (this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, (toNamespace == null) ? this.getNamespaceName(null) : toNamespace, visualizationArtifacts.getDashboard().getName(), null, HSecurityManager.TOKEN) != null) {
            throw new CompilationException("Dashboard already exists, cannot import the dashboard");
        }
        this.modifyAndValidate(visualizationArtifacts, toNamespace);
        for (final MetaInfo.Query queryMetaObject : visualizationArtifacts.getParametrizedQuery()) {
            if (this.metadataRepository.getMetaObjectByName(EntityType.QUERY, queryMetaObject.getNsName(), queryMetaObject.getName(), null, HSecurityManager.TOKEN) != null) {
                continue;
            }
            try {
                this.useNamespace(queryMetaObject.getNsName());
                Compiler.compile("CREATE NAMEDQUERY " + queryMetaObject.name + " " + ServerUpgradeUtility.convertNewQueries(queryMetaObject.queryDefinition), this, new Compiler.ExecutionCallback() {
                    @Override
                    public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                        try {
                            compiler.compileStmt(stmt);
                        }
                        catch (Warning warning) {}
                        finally {
                            Context.this.useNamespace(toNamespace);
                        }
                    }
                });
            }
            catch (Exception e5) {
                Context.logger.warn((Object)e5.getMessage());
            }
        }
        for (final MetaInfo.QueryVisualization qv : visualizationArtifacts.getQueryVisualizations()) {
            if (this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, qv.getNsName(), qv.getName(), null, HSecurityManager.TOKEN) != null) {
                Context.logger.warn((Object)("Ignoring import of exiting object " + qv.getFullName()));
            }
            else {
                this.put(qv);
            }
        }
        for (final MetaInfo.Page page : visualizationArtifacts.getPages()) {
            if (this.metadataRepository.getMetaObjectByName(EntityType.PAGE, page.getNsName(), page.getName(), null, HSecurityManager.TOKEN) != null) {
                Context.logger.warn((Object)("Ignoring import of exiting object " + page.getFullName()));
            }
            else {
                this.put(page);
            }
        }
        final MetaInfo.Dashboard dashboard = visualizationArtifacts.getDashboard();
        this.put(dashboard);
        return dashboard.getUuid();
    }
    
    public void extractInfo(final VisualizationArtifacts visualizationArtifacts, final List<JsonNode> qvObjects, final List<JsonNode> pageObjects, final ObjectMapper jsonMapper, final JsonNode moNode) throws IOException, ClassNotFoundException {
        final String className = moNode.get("metaObjectClass").asText();
        if (className.equalsIgnoreCase("com.datasphere.runtime.MetaInfo$Dashboard") || className.equalsIgnoreCase("com.datasphere.runtime.meta.MetaInfo$Dashboard")) {
            final String moNodeText = moNode.toString();
            final Class<?> moClass = Class.forName("com.datasphere.runtime.meta.MetaInfo$Dashboard", false, ClassLoader.getSystemClassLoader());
            final Object ob = jsonMapper.readValue(moNodeText, (Class)moClass);
            visualizationArtifacts.setDashboard((MetaInfo.Dashboard)ob);
        }
        if (className.equalsIgnoreCase("com.datasphere.runtime.MetaInfo$Query") || className.equalsIgnoreCase("com.datasphere.runtime.meta.MetaInfo$Query")) {
            final MetaInfo.Query query = MetaInfo.Query.deserialize(moNode);
            visualizationArtifacts.addParametrizedQuery(query);
        }
        if (className.equalsIgnoreCase("com.datasphere.runtime.MetaInfo$QueryVisualization") || className.equalsIgnoreCase("com.datasphere.runtime.meta.MetaInfo$QueryVisualization")) {
            qvObjects.add(moNode);
        }
        if (className.equalsIgnoreCase("com.datasphere.runtime.MetaInfo$Page") || className.equalsIgnoreCase("com.datasphere.runtime.meta.MetaInfo$Page")) {
            pageObjects.add(moNode);
        }
    }
    
    private void validateSameNamespace(final String id, final String toNSName, final String type) throws MetaDataRepositoryException {
        final String nsNameOfObject = Utility.splitDomain(id);
        if (!nsNameOfObject.equalsIgnoreCase(toNSName)) {
            throw new MetaDataRepositoryException(type + " should be in the same namespace as the Dashboard.");
        }
    }
    
    private String getCaseSensitiveNamespaceAndName(final String str) throws MetaDataRepositoryException {
        final String name = Utility.splitName(str);
        final String nsName = Utility.splitDomain(str);
        final MetaInfo.Namespace ns = this.getNamespace(nsName);
        if (ns == null) {
            throw new MetaDataRepositoryException(" Namespace " + nsName + " does not exist. Please create it before performing this operation");
        }
        return ns.getName() + "." + name;
    }
    
    private void modifyAndValidate(final VisualizationArtifacts visualizationArtifacts, final String toNamespace) throws MetaDataRepositoryException {
        final List<String> namespaceDependentPages = new ArrayList<String>();
        final List<String> namespaceDependentQueryVisualisations = new ArrayList<String>();
        final List<String> namespaceDependentQueries = new ArrayList<String>();
        final MetaInfo.Namespace namespace = this.getNamespace(toNamespace);
        final MetaInfo.Dashboard dashboard = visualizationArtifacts.getDashboard();
        dashboard.setNsName(namespace.getName());
        dashboard.setNamespaceId(namespace.getUuid());
        dashboard.setUri(this.replaceNamespaceOfURI(dashboard.getUri(), namespace));
        if (dashboard.getPages() != null) {
            for (int i = 0; i < dashboard.getPages().size(); ++i) {
                final String pageName = dashboard.getPages().get(i);
                if (pageName.indexOf(".") == -1) {
                    namespaceDependentPages.add(pageName);
                }
                else {
                    this.validateSameNamespace(pageName, toNamespace, "Page");
                    dashboard.getPages().set(i, this.getCaseSensitiveNamespaceAndName(pageName));
                }
            }
        }
        dashboard.setUuid(UUID.genCurTimeUUID());
        for (final MetaInfo.Page page : visualizationArtifacts.getPages()) {
            if (page.getQueryVisualizations() != null) {
                for (int j = 0; j < page.getQueryVisualizations().size(); ++j) {
                    final String qv = page.getQueryVisualizations().get(j);
                    if (qv.indexOf(".") == -1) {
                        namespaceDependentQueryVisualisations.add(qv);
                    }
                    else {
                        this.validateSameNamespace(qv, toNamespace, "Visualization");
                        page.getQueryVisualizations().set(j, this.getCaseSensitiveNamespaceAndName(qv));
                    }
                }
            }
            if (namespaceDependentPages.contains(page.getName())) {
                page.setNsName(namespace.getName());
                page.setNamespaceId(namespace.getUuid());
                page.setUri(this.replaceNamespaceOfURI(page.getUri(), namespace));
                page.setUuid(UUID.genCurTimeUUID());
            }
            else {
                page.setNsName(this.getNamespaceName(page.nsName));
            }
        }
        for (final MetaInfo.QueryVisualization queryVisualization : visualizationArtifacts.getQueryVisualizations()) {
            if (queryVisualization.getQuery() != null && queryVisualization.getQuery().indexOf(".") == -1) {
                namespaceDependentQueries.add(queryVisualization.getQuery());
            }
            if (queryVisualization.getQuery() != null && queryVisualization.getQuery().indexOf(".") != -1) {
                queryVisualization.setQuery(this.getCaseSensitiveNamespaceAndName(queryVisualization.getQuery()));
            }
            if (namespaceDependentQueryVisualisations.contains(queryVisualization.getName())) {
                queryVisualization.setNsName(namespace.getName());
                if (queryVisualization.getQuery() != null) {
                    if (queryVisualization.getQuery().indexOf(".") != -1 && !Utility.splitDomain(queryVisualization.getQuery()).equalsIgnoreCase(namespace.getName())) {
                        final String queryNSName = Utility.splitDomain(queryVisualization.getQuery());
                        final MetaInfo.Namespace QueryNS = this.getNamespace(queryNSName);
                        queryVisualization.setQuery(queryVisualization.getQuery().isEmpty() ? "" : Utility.convertToFullQualifiedName(QueryNS, Utility.splitName(queryVisualization.getQuery())));
                    }
                    else {
                        queryVisualization.setQuery(queryVisualization.getQuery().isEmpty() ? "" : Utility.splitName(queryVisualization.getQuery()));
                    }
                }
                queryVisualization.setNamespaceId(namespace.getUuid());
                queryVisualization.setUri(this.replaceNamespaceOfURI(queryVisualization.getUri(), namespace));
                queryVisualization.setUuid(UUID.genCurTimeUUID());
            }
            else {
                queryVisualization.setNsName(this.getNamespaceName(queryVisualization.nsName));
            }
        }
        for (final MetaInfo.Query query : visualizationArtifacts.getParametrizedQuery()) {
            if (namespaceDependentQueries.contains(query.getName())) {
                query.setNsName(namespace.getName());
                query.setNamespaceId(namespace.getUuid());
                query.setUri(this.replaceNamespaceOfURI(query.getUri(), namespace));
            }
            else {
                query.setNsName(this.getNamespaceName(query.nsName));
            }
        }
    }
    
    private String replaceNamespaceOfURI(final String URI, final MetaInfo.Namespace namespace) {
        final String[] URI_SPLIT = URI.split(":");
        URI_SPLIT[0] = namespace.getName().toUpperCase();
        final String URI_UPDATED = Arrays.toString(URI_SPLIT).replace(", ", ":").replaceAll("[\\[\\]]", "");
        return URI_UPDATED;
    }
    
    static {
        Context.logger = Logger.getLogger((Class)Context.class);
    }
    
    public static class Result implements Serializable
    {
        private static final long serialVersionUID = 4217752353922101919L;
        public UUID serverID;
        public Collection<MetaInfo.MetaObjectInfo> processedObjects;
        public Collection<String> serverDGs;
        public Map<UUID, Boolean> runningStatus;
        public MetaInfo.StatusInfo.Status flowStatus;
        
        private static String id2name(final UUID id) throws MetaDataRepositoryException {
            final MetaInfo.MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, HSecurityManager.TOKEN);
            if (!(o instanceof MetaInfo.Server)) {
                return id.toString();
            }
            final MetaInfo.Server s = (MetaInfo.Server)o;
            if (s.name != null && !s.name.isEmpty()) {
                return s.name;
            }
            return "srv_" + s.id;
        }
        
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            try {
                sb.append("\nON " + id2name(this.serverID) + " IN " + this.serverDGs + "\n[\n");
            }
            catch (MetaDataRepositoryException e) {
                Context.logger.error((Object)e.getMessage());
            }
            for (final MetaInfo.MetaObjectInfo o : this.processedObjects) {
                sb.append("\t(" + o.nsName + "." + o.name + " " + o.type + "),\n");
            }
            sb.append("]\n");
            return sb.toString();
        }
    }
    
    public static class SetStmtRemoteCall implements RemoteCall<Result>
    {
        private static final long serialVersionUID = -1130287312659963213L;
        String paramname;
        Object paramvalue;
        
        public SetStmtRemoteCall() {
        }
        
        public SetStmtRemoteCall(final String paramname, final Object paramvalue) {
            this.paramname = paramname;
            this.paramvalue = paramvalue;
        }
        
        @Override
        public Result call() throws Exception {
            final Server srv = Server.server;
            srv.changeOptions(this.paramname, this.paramvalue);
            return null;
        }
    }
    
    public static class ReportStmtRemoteCall implements RemoteCall<ReportStats.BaseReportStats>
    {
        private static final long serialVersionUID = -1130287312659963214L;
        UUID flowuuid;
        int action;
        AuthToken clientToken;
        String fileName;
        
        public ReportStmtRemoteCall() {
        }
        
        public ReportStmtRemoteCall(final UUID flowuuid, final int reportaction, final AuthToken cltoken, final String fileName) {
            this.flowuuid = flowuuid;
            this.action = reportaction;
            this.clientToken = cltoken;
            this.fileName = fileName;
        }
        
        @Override
        public ReportStats.BaseReportStats call() throws Exception {
            final FlowComponent fc = Server.getServer().getOpenObject(this.flowuuid);
            if (fc == null) {
                return null;
            }
            if (this.action == 1) {
                fc.addSessionToReport(this.clientToken);
                return null;
            }
            if (this.action == 2) {
                return fc.removeSessionToReport(this.clientToken, true);
            }
            return null;
        }
    }
    
    public static class DumpStmtRemoteCall implements RemoteCall<Result>
    {
        private static final long serialVersionUID = -1130287312659963214L;
        UUID cqUid;
        int mode;
        int limit;
        AuthToken clientToken;
        boolean turnoff;
        
        public DumpStmtRemoteCall() {
            this.turnoff = false;
        }
        
        public DumpStmtRemoteCall(final UUID cquuid, final int dumpmode, final int limitVal, final boolean turnoff, final AuthToken cltoken) {
            this.turnoff = false;
            this.cqUid = cquuid;
            this.mode = dumpmode;
            this.limit = limitVal;
            this.clientToken = cltoken;
            this.turnoff = turnoff;
        }
        
        @Override
        public Result call() throws Exception {
            final FlowComponent fc = Server.getServer().getOpenObject(this.cqUid);
            if (fc == null) {
                return null;
            }
            final CQTask cqfc = (CQTask)fc;
            int traceFlags = 0;
            switch (this.mode) {
                case 1: {
                    if (this.turnoff) {
                        traceFlags &= 0xFFFFFFFE;
                        break;
                    }
                    traceFlags |= 0x1;
                    break;
                }
                case 2: {
                    if (this.turnoff) {
                        traceFlags &= 0xFFFFFFFD;
                        break;
                    }
                    traceFlags |= 0x2;
                    break;
                }
                case 3: {
                    if (this.turnoff) {
                        traceFlags &= 0xFFFFFFFC;
                        break;
                    }
                    traceFlags |= 0x3;
                    break;
                }
                case 4: {
                    final MetaInfo.CQ cqMeta = (MetaInfo.CQ)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.cqUid, HSecurityManager.TOKEN);
                    if (cqMeta != null) {
                        final List<String> sourceClasses = cqMeta.getPlan().sourcecode;
                        if (sourceClasses != null) {
                            for (final String temp : sourceClasses) {
                                System.out.println(temp);
                            }
                        }
                    }
                    return null;
                }
            }
            final TraceOptions traceOptions = new TraceOptions(traceFlags, null, null, this.limit);
            final HQueue tracequeue = HQueue.getQueue("consoleQueue" + this.clientToken);
            cqfc.setTraceOptions(traceOptions, tracequeue);
            return null;
        }
    }
    
    public static class ChangeFlowState implements RemoteCall<Result>
    {
        private static final long serialVersionUID = -8137681093645820789L;
        private final ActionType what;
        private final MetaInfo.Flow flow;
        private final List<UUID> servers;
        private final UUID clientSessionID;
        private final List<Property> params;
        private final Long epochNumber;
        
        public ChangeFlowState() {
            this(null, null, null, null, null, null);
        }
        
        public ChangeFlowState(final ActionType what, final MetaInfo.Flow flow, final List<UUID> servers, final UUID clientSessionID, final Long epochNumber) {
            this(what, flow, servers, clientSessionID, null, epochNumber);
        }
        
        public ChangeFlowState(final ActionType what, final MetaInfo.Flow flow, final List<UUID> servers, final UUID clientSessionID, final List<Property> params, final Long epochNumber) {
            this.what = what;
            this.flow = flow;
            this.servers = servers;
            this.clientSessionID = clientSessionID;
            this.params = params;
            this.epochNumber = epochNumber;
        }
        
        @Override
        public Result call() throws Exception {
            if (HazelcastSingleton.isClientMember()) {
                return null;
            }
            final Server srv = Server.server;
            if (srv != null) {
                final Result r = new Result();
                if (Context.logger.isInfoEnabled()) {
                    Context.logger.info((Object)("Got request " + this.what + " for flow " + this.flow.uuid + " with servers " + this.servers + " EpochNumber " + this.epochNumber));
                }
                srv.changeFlowState(this.what, this.flow, this.servers, this.clientSessionID, this.params, this.epochNumber);
                r.processedObjects = srv.getDeployedObjects(this.flow.getUuid());
                r.serverID = srv.getServerID();
                r.serverDGs = srv.getDeploymentGroups();
                if (this.what == ActionType.STATUS) {
                    r.runningStatus = srv.whatIsRunning(this.flow.getUuid());
                    r.flowStatus = srv.getFlowStatus(this.flow);
                }
                return r;
            }
            throw new RuntimeException("non-lite member has no running server");
        }
        
        public ActionType getAction() {
            return this.what;
        }
        
        public UUID getFlowUuid() {
            return this.flow.uuid;
        }
        
        public long getEpochNumber() {
            return this.epochNumber;
        }
        
        public List<Property> getParams() {
            return this.params;
        }
        
        @Override
        public String toString() {
            return this.what + " for " + this.flow.getFullName() + " on " + this.servers;
        }
    }
    
    public static class RemoteCallOnObject implements RemoteCall<Result>
    {
        private static final long serialVersionUID = -8137681093645820789L;
        private final ActionType what;
        private final MetaInfo.MetaObject obj;
        private final UUID clientSessionID;
        private final Object[] loadDetails;
        
        public RemoteCallOnObject() {
            this(null, null, null, new Object[] { null, null, null });
        }
        
        public RemoteCallOnObject(final ActionType what, final MetaInfo.MetaObject obj, final AuthToken sessionID, final Object... loadDetails) {
            this.loadDetails = loadDetails;
            this.what = what;
            this.obj = obj;
            this.clientSessionID = (UUID)sessionID;
        }
        
        @Override
        public Result call() throws Exception {
            if (HazelcastSingleton.isClientMember()) {
                return null;
            }
            final Server srv = Server.server;
            if (srv != null) {
                final Result r = new Result();
                if (Context.logger.isInfoEnabled()) {
                    Context.logger.info((Object)("Got request " + this.what.toString() + " for flow " + this.obj.uuid));
                }
                srv.remoteCallOnObject(this.what, this.obj, this.loadDetails, this.clientSessionID);
                return r;
            }
            throw new RuntimeException("non-lite member has no running server");
        }
    }
    
    public static class ShutDown implements RemoteCall<Object>
    {
        private static final long serialVersionUID = -4760044703938736365L;
        
        @Override
        public Object call() throws Exception {
            System.exit(888);
            return null;
        }
    }
}
