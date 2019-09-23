package com.datasphere.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

import com.datasphere.drop.DropMetaObject;
import com.datasphere.exception.CompilationException;
import com.datasphere.exception.FatalException;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.Warning;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.intf.DashboardOperations;
import com.datasphere.intf.QueryManager;
import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.license.LicenseManager;
import com.datasphere.logging.UserCommandLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.datasphere.metaRepository.MDConstants;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.PermissionUtility;
import com.datasphere.runtime.compiler.AST;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.Lexer;
import com.datasphere.runtime.compiler.ObjectName;
import com.datasphere.runtime.compiler.TypeDefOrName;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.select.AST2JSON;
import com.datasphere.runtime.compiler.select.ParamDesc;
import com.datasphere.runtime.compiler.stmts.ConnectStmt;
import com.datasphere.runtime.compiler.stmts.CreateAdHocSelectStmt;
import com.datasphere.runtime.compiler.stmts.CreateDashboardStatement;
import com.datasphere.runtime.compiler.stmts.CreatePropertySetStmt;
import com.datasphere.runtime.compiler.stmts.CreateUserStmt;
import com.datasphere.runtime.compiler.stmts.DeploymentRule;
import com.datasphere.runtime.compiler.stmts.EventType;
import com.datasphere.runtime.compiler.stmts.ExceptionHandler;
import com.datasphere.runtime.compiler.stmts.MappedStream;
import com.datasphere.runtime.compiler.stmts.OutputClause;
import com.datasphere.runtime.compiler.stmts.RecoveryDescription;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.compiler.stmts.UserProperty;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.ObjectReference;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.CQUtility;
import com.datasphere.utility.Utility;
import com.datasphere.utility.WindowUtility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.web.api.ClientCreateOperation;
import com.datasphere.web.api.ClientOperations;

import java_cup.runtime.Symbol;

public class QueryValidator implements QueryManager, DashboardOperations
{
    private static Logger logger;
    private final Server srv;
    private final Map<AuthToken, Context> contexts;
    private boolean updateMode;
    private MetadataRepository metadataRepository;
    private ClientOperations clientOperations;
    private static final Pattern typePat;
    private static String SOURCE_SIDE_FILTERING_VALIDATION_ERROR;
    
    public QueryValidator(final Server srv) {
        this.metadataRepository = MetadataRepository.getINSTANCE();
        this.clientOperations = ClientOperations.getInstance();
        this.srv = srv;
        this.contexts = Factory.makeMap();
        this.updateMode = false;
    }
    
    public Context getContext(final AuthToken token) {
        final Context context = this.contexts.get(token);
        if (context == null) {
            throw new RuntimeException("Could not find context for invalid authorization token: " + token);
        }
        return context;
    }
    
    public void createNewContext(final AuthToken token) throws MetaDataRepositoryException {
        if (this.contexts.containsKey(token)) {
            return;
        }
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)"createNewContext -- ");
        }
        final Context ctx = this.srv.createContext(token);
        ctx.setIsUIContext(true);
        final String userName = HSecurityManager.getAutheticatedUserName(token);
        if (userName == null) {
            ctx.useNamespace("Global");
        }
        else {
            final MetaInfo.User user = HSecurityManager.get().getUser(userName);
            ctx.useNamespace(user.getDefaultNamespace());
        }
        this.addContext(token, ctx);
    }
    
    public void setCurrentApp(final String name, final AuthToken token) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("setCurrentApp : " + name));
        }
        final Context context = this.getContext(token);
        if (name == null) {
            context.setCurApp(null);
        }
        else {
            assert this.metadataRepository != null;
            final String[] namespaceAndName = name.split("\\.");
            if (namespaceAndName[0] != null && namespaceAndName[1] != null) {
                final MetaInfo.Flow f = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, namespaceAndName[0], namespaceAndName[1], null, token);
                context.setCurApp(f);
            }
        }
    }
    
    public void setCurrentFlow(final String name, final AuthToken token) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("setCurrentFlow : " + name));
        }
        final Context c = this.getContext(token);
        if (name == null) {
            c.setCurFlow(null);
        }
        else {
            assert this.metadataRepository != null;
            final String[] namespaceAndName = name.split("\\.");
            if (namespaceAndName[0] != null && namespaceAndName[1] != null) {
                final MetaInfo.Flow f = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.FLOW, namespaceAndName[0], namespaceAndName[1], null, token);
                c.setCurFlow(f);
            }
        }
    }
    
    public void addContext(final AuthToken token, final Context ctx) {
        this.contexts.put(token, ctx);
    }
    
    public void removeContext(final AuthToken token) {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)"removeContext : ");
        }
        this.contexts.remove(token);
    }
    
    public void setUpdateMode(final boolean mode) {
        this.updateMode = mode;
    }
    
    private void compile(final AuthToken token, final Stmt stmt) throws MetaDataRepositoryException {
        this.compile(token, stmt, this.updateMode);
    }
    
    private void compile(final AuthToken token, final Stmt stmt, final boolean updateMode) throws MetaDataRepositoryException {
        final Context context = this.getContext(token);
        final boolean oldVal = context.setReadOnly(!updateMode);
        try {
            Compiler.compile(stmt, context, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws Exception {
                    compiler.compileStmt(stmt);
                    if (!(stmt instanceof CreatePropertySetStmt) && !(stmt instanceof CreateUserStmt)) {
                        if (!(stmt instanceof ConnectStmt)) {
                            QueryValidator.this.storeCommand(token, QueryValidator.this.getUserId(token), (stmt.sourceText == null || stmt.sourceText.isEmpty()) ? stmt.toString() : stmt.sourceText);
                        }
                    }
                }
            });
        }
        catch (MetaDataRepositoryException e) {
            throw e;
        }
        catch (CompilationException ce) {
            throw ce;
        }
        catch (Warning warning) {
            throw warning;
        }
        catch (Throwable e2) {
            throw new RuntimeException(e2);
        }
        finally {
            context.setReadOnly(oldVal);
        }
    }
    
    public String compileText(final AuthToken token, final String text) throws Exception {
        final String returnString = this.compileText(token, text, false);
        return returnString;
    }
    
    public String compileText(final AuthToken token, final String text, final boolean readOnly) throws Exception {
        final Context context = this.getContext(token);
        final boolean oldVal = context.setReadOnly(readOnly);
        final StringBuilder builder = new StringBuilder();
        try {
            Compiler.compile(text, context, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    try {
                        compiler.compileStmt(stmt);
                        QueryValidator.this.storeStmt(token, QueryValidator.this.getUserId(token), stmt);
                    }
                    catch (Warning e) {
                        builder.append(e.getLocalizedMessage());
                        builder.append("\n");
                    }
                }
            });
        }
        catch (Exception e) {
            this.setCurrentApp(null, token);
            this.setCurrentFlow(null, token);
            throw e;
        }
        finally {
            context.setReadOnly(oldVal);
        }
        return builder.toString();
    }
    
    private String getUserId(final AuthToken token) {
        final Context context = this.contexts.get(token);
        String uid = null;
        if (context != null) {
            uid = context.getCurUser();
        }
        if (uid == null) {
            uid = HSecurityManager.getAutheticatedUserName(token);
        }
        return uid;
    }
    
    public void storeStmt(final AuthToken token, final String userid, final Stmt stmt) {
        if (!(stmt instanceof CreatePropertySetStmt) && !(stmt instanceof CreateUserStmt)) {
            if (!(stmt instanceof ConnectStmt)) {
                this.storeCommand(token, userid, (stmt.sourceText == null || stmt.sourceText.isEmpty()) ? stmt.toString() : stmt.sourceText);
            }
        }
    }
    
    public void storeCommand(final AuthToken token, final String userid, final String text) {
        try {
            UserCommandLogger.logCmd(userid, token.getUUIDString(), text);
        }
        catch (Exception ex) {
            if (QueryValidator.logger.isDebugEnabled()) {
                QueryValidator.logger.debug((Object)("error storing command : " + text));
            }
        }
    }
    
    public void testCompatibility(final AuthToken token, final String userid, final String text) throws Exception {
        QueryValidator.logger.warn((Object)("testing compatibility : " + text));
        String json = null;
        if (text.split(" ").length > 2) {
            json = text.split(" ")[2];
        }
        ServerUpgradeUtility.testCompatibility(json);
    }
    
    private static Pair<EntityType, String> makeEntity(final String entityType, final String entityName) {
        final EntityType e = EntityType.forObject(entityType);
        final Pair<EntityType, String> p = Pair.make(e, entityName);
        return p;
    }
    
    public void CreateAlterStmt(final AuthToken token, final String entityType, final String entityName) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, false));
    }
    
    public void CreateAlterRecompileStmt(final AuthToken token, final String entityType, final String entityName) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateAlterRecompileStmt (token " + token + ", entityType " + entityType + ", entityName " + entityName + ")"));
        }
        this.compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, true));
    }
    
    public static List<Property> makePropList(final Map<String, String>[] props) {
        if (props == null) {
            return null;
        }
        final List<Property> plist = new ArrayList<Property>();
        for (final Map<String, String> prop : props) {
            assert prop.size() == 1;
            if (prop != null) {
                for (final Map.Entry<String, String> e : prop.entrySet()) {
                    final Property p = new Property(e.getKey(), e.getValue());
                    plist.add(p);
                }
            }
        }
        return plist;
    }
    
    public static String[] GetCurrentLicensingOption() {
        final List optList = LicenseManager.get().getLicenseOptions();
        if (optList == null) {
            return null;
        }
        final String[] optArr = new String[optList.size()];
        int i = 0;
        for (final Object opt : optList) {
            optArr[i++] = (String)opt;
        }
        return optArr;
    }
    
    public Map<String, Boolean> AreTemplatesAllowed(final String[] tnames) {
        final HashMap<String, Boolean> tnameMap = new HashMap<String, Boolean>();
        for (final String templateName : tnames) {
            tnameMap.put(templateName, Server.getServer().isTemplateAllowed(templateName));
        }
        return tnameMap;
    }
    
    public String GetFlowStatus(final AuthToken token, final String appOrFlowName, final String eType) throws Exception {
        final Context context = this.getContext(token);
        final String ret = context.getStatus(appOrFlowName, EntityType.forObject(eType));
        return ret;
    }
    
    public Set<String> GetFlowErrors(final AuthToken token, final String appOrFlowName, final String eType) throws Exception {
        final Context context = this.getContext(token);
        final Set<String> ret = context.getErrors(appOrFlowName, EntityType.forObject(eType), token);
        return ret;
    }
    
    public String getApplicationTQL(final String nsName, final String appName, final boolean deepTraverse, final AuthToken token) throws MetaDataRepositoryException {
        final MetadataRepository mr = MetadataRepository.getINSTANCE();
        return mr.getApplicationTQL(nsName, appName, deepTraverse, token);
    }
    
    public void CreateStartFlowStatement(final AuthToken token, final String appOrFlowName, final String eType) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateStartStmt(appOrFlowName, EntityType.forObject(eType), null));
    }
    
    public void CreateStopFlowStatement(final AuthToken token, final String appOrFlowName, final String eType) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateStopStmt(appOrFlowName, EntityType.forObject(eType)));
    }
    
    public void CreateQuiesceFlowStatement(final AuthToken token, final String appOrFlowName, final String eType) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateQuiesceStmt(appOrFlowName, EntityType.forObject(eType)));
    }
    
    public void CreateResumeFlowStatement(final AuthToken token, final String appOrFlowName, final String eType) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateResumeStmt(appOrFlowName, EntityType.forObject(eType)));
    }
    
    public void CreateUndeployFlowStatement(final AuthToken token, final String appOrFlowName, final String eType) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateUndeployStmt(appOrFlowName, EntityType.forObject(eType)));
    }
    
    private DeploymentRule makeRule(final Map<String, String> desc, final boolean canBeInDefaultDG) {
        String flowName = null;
        DeploymentStrategy st = DeploymentStrategy.ON_ALL;
        String deploymentGroup = canBeInDefaultDG ? "default" : null;
        for (final Map.Entry<String, String> e : desc.entrySet()) {
            final String key = e.getKey();
            final String value = e.getValue();
            if (key.equalsIgnoreCase("flow")) {
                flowName = value;
            }
            else if (key.equalsIgnoreCase("strategy")) {
                if (value.equalsIgnoreCase("all")) {
                    st = DeploymentStrategy.ON_ALL;
                }
                else {
                    if (!value.equalsIgnoreCase("any")) {
                        throw new RuntimeException("unkown deployment strategy <" + value + ">");
                    }
                    st = DeploymentStrategy.ON_ONE;
                }
            }
            else {
                if (!key.equalsIgnoreCase("group")) {
                    throw new RuntimeException("unknown field <" + key + "> in flow deployment rule");
                }
                deploymentGroup = value;
            }
        }
        if (flowName == null || deploymentGroup == null) {
            throw new RuntimeException("invalid deployment rule <" + desc + ">");
        }
        return AST.CreateDeployRule(st, flowName, deploymentGroup);
    }
    
    public void CreateAlterStatement(final AuthToken token, final String entityType, final String entityName) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, false));
    }
    
    public void CreateAlterRecompileStatement(final AuthToken token, final String entityType, final String entityName) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateAlterRecompileStatement (token " + token + ", entityType " + entityType + ", entityName " + entityName + ")"));
        }
        this.compile(token, AST.CreateAlterAppOrFlowStmt(EntityType.forObject(entityType), entityName, true));
    }
    
    public void CreateDeployFlowStatement(final AuthToken token, final String eType, final Map<String, String> appDesc, final Map<String, String>[] detailsDesc) throws MetaDataRepositoryException {
        final DeploymentRule appRule = this.makeRule(appDesc, true);
        final List<DeploymentRule> details = new ArrayList<DeploymentRule>();
        if (detailsDesc != null) {
            for (final Map<String, String> d : detailsDesc) {
                final DeploymentRule flowSubRule = this.makeRule(d, false);
                details.add(flowSubRule);
            }
        }
        this.compile(token, AST.CreateDeployStmt(EntityType.forObject(eType), appRule, details, null), true);
    }
    
    public void CreateStreamStatement(final AuthToken requestToken, final String stream_name, final Boolean doReplace, final String[] partition_fields, final String typeName, final StreamPersistencePolicy spp) throws MetaDataRepositoryException {
        final String streamNameWithoutDomain = Utility.splitName(stream_name);
        final String str = Utility.createStreamStatementText(streamNameWithoutDomain, doReplace, typeName, partition_fields);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateStreamStatement : " + streamNameWithoutDomain + " " + doReplace + " " + typeName));
        }
        final Stmt stmt = AST.CreateStreamStatement(streamNameWithoutDomain, doReplace, Arrays.asList(partition_fields), new TypeDefOrName(typeName, null), null, spp);
        stmt.sourceText = str;
        this.compile(requestToken, stmt);
    }
    
    public static List<TypeField> makeFieldList(final Map<String, String>[] defs) {
        final List<TypeField> flist = new ArrayList<TypeField>();
        for (final Map<String, String> def : defs) {
            assert def.size() == 1;
            for (final Map.Entry<String, String> e : def.entrySet()) {
                final Matcher m = QueryValidator.typePat.matcher(e.getValue());
                if (!m.matches()) {
                    throw new RuntimeException("invalid type name " + e.getValue());
                }
                final String name = m.group(3);
                int ndims = 0;
                if (m.group(1) != null) {
                    final String dims = m.group(1);
                    for (int k = dims.indexOf(91); k != -1; k = dims.indexOf(91, k + 1)) {
                        ++ndims;
                    }
                }
                final boolean isKey = m.group(4) != null;
                final TypeName tn = new TypeName(name, ndims);
                final TypeField tf = new TypeField(e.getKey(), tn, isKey);
                flist.add(tf);
            }
        }
        return flist;
    }
    
    public void CreateStreamStatementWithTypeDef(final AuthToken token, final String streamName, final Boolean doReplace, final String[] partition_fields, final Map<String, String>[] fields, final String pset, final boolean doPersist) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateStreamStatement(streamName, doReplace, Arrays.asList(partition_fields), new TypeDefOrName(null, makeFieldList(fields)), null, null));
    }
    
    public void CreateTypeStatement(final AuthToken token, final String type_name, final Boolean doReplace, final Map<String, String>[] fields) throws MetaDataRepositoryException {
        final String typeNameWithoutDomain = Utility.splitName(type_name);
        final List<TypeField> typeFields = makeFieldList(fields);
        final String str = Utility.createTypeStatementText(typeNameWithoutDomain, doReplace, typeFields);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateTypeStatement_New " + typeNameWithoutDomain + " " + doReplace + " "));
        }
        final TypeDefOrName tdon = new TypeDefOrName(null, typeFields);
        final Stmt stmt = AST.CreateTypeStatement(typeNameWithoutDomain, doReplace, tdon);
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    public void CreateTypeStatement_New(final AuthToken token, final String type_name, final Boolean doReplace, final TypeField[] fields) throws MetaDataRepositoryException {
        final String typeNameWithoutDomain = Utility.splitName(type_name);
        final String str = Utility.createTypeStatementText(typeNameWithoutDomain, doReplace, Arrays.asList(fields));
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateTypeStatement_New " + typeNameWithoutDomain + " " + doReplace + " "));
        }
        final TypeDefOrName tdon = new TypeDefOrName(null, Arrays.asList(fields));
        final Stmt stmt = AST.CreateTypeStatement(typeNameWithoutDomain, doReplace, tdon);
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    public void CreateAppStatement(final AuthToken token, final String n, final Boolean r, final Map<String, String>[] entities, final Boolean encrypt, final RecoveryDescription recov, final ExceptionHandler eh, final String[] importStatements, final List<DeploymentRule> deploymentRules) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isDebugEnabled()) {
            QueryValidator.logger.debug((Object)("AuthToken: " + token));
        }
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateAppStatement " + n + " " + r + " " + entities));
        }
        List<Pair<EntityType, String>> l = null;
        if (entities != null) {
            l = new ArrayList<Pair<EntityType, String>>();
            for (final Map<String, String> ent : entities) {
                assert ent.size() == 1;
                for (final Map.Entry<String, String> e : ent.entrySet()) {
                    l.add(makeEntity(e.getKey(), e.getValue()));
                }
            }
        }
        this.compile(token, AST.CreateFlowStatement(n, r, EntityType.APPLICATION, l, encrypt, recov, eh));
    }
    
    public void CreateNameSpaceStatement(final AuthToken token, final String n, final Boolean r) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("---> " + n + " " + r));
        }
        final String name = Utility.splitName(n);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("---> Using name: " + name));
        }
        this.compile(token, AST.CreateNamespaceStatement(name, r));
    }
    
    public void CreateVisualizationStatement(final AuthToken token, final String objectName, final String n) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateVisualization(objectName, n));
    }
    
    public void CreateCacheStatement_New(final AuthToken token, final String n, final Boolean r, final String reader_type, final List<Property> reader_props, final String parser_type, final List<Property> parser_props, final List<Property> query_props, final String typename) throws MetaDataRepositoryException {
        final String cacheNameWithoutDomain = Utility.splitName(n);
        final String str = Utility.createCacheStatementText(cacheNameWithoutDomain, r, reader_type, reader_props, parser_type, parser_props, query_props, typename);
        final Stmt stmt = AST.CreateCacheStatement(cacheNameWithoutDomain, r, AST.CreateAdapterDesc(reader_type, reader_props), (parser_type != null) ? AST.CreateAdapterDesc(parser_type, parser_props) : null, query_props, typename);
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    public void CreateDeploymentGroupStatement(final AuthToken token, final String groupname, final String[] deploymentGroup, final Long minServers, final Long maxApps) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateDeploymentGroupStatement(groupname, Arrays.asList(deploymentGroup), minServers, maxApps));
    }
    
    public void CreateFlowStatement(final AuthToken token, final String n, final Boolean r, final Map<String, String>[] entities) throws MetaDataRepositoryException {
        List<Pair<EntityType, String>> l = null;
        if (entities != null) {
            l = new ArrayList<Pair<EntityType, String>>();
            for (final Map<String, String> ent : entities) {
                assert ent.size() == 1;
                for (final Map.Entry<String, String> e : ent.entrySet()) {
                    l.add(makeEntity(e.getKey(), e.getValue()));
                }
            }
        }
        this.compile(token, AST.CreateFlowStatement(n, r, EntityType.FLOW, l, false, null, null));
    }
    
    public void CreatePropertySet(final AuthToken token, final String n, final Boolean r, final Map<String, String>[] props) throws MetaDataRepositoryException {
        this.compile(token, AST.CreatePropertySet(n, r, makePropList(props)));
    }
    
    public void CreateRoleStatement(final AuthToken token, final String name) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateRoleStatement(name));
    }
    
    public void CreateSourceStatement(final AuthToken token, final String n, final Boolean r, final String adapType, final Map<String, String>[] adapProps, final String parserType, final Map<String, String>[] parserProps, final String instream) throws MetaDataRepositoryException {
        final String sourceNameWithoutDomain = Utility.splitName(n);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateSourceStatement_New (token " + token + ", entityName " + sourceNameWithoutDomain + ", replace " + r + " )"));
        }
        String str;
        if (parserType != null && parserProps != null) {
            str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, null, makePropList(adapProps), parserType, null, makePropList(parserProps), instream, null);
        }
        else {
            str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, null, makePropList(adapProps), null, null, null, instream, null);
        }
        final ArrayList<OutputClause> sinks = new ArrayList<OutputClause>();
        sinks.add(AST.newOutputClause(instream, null, null, null, null, null));
        if (parserType != null && parserProps != null) {
            final Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), AST.CreateAdapterDesc(parserType, makePropList(parserProps)), sinks);
            stmt.sourceText = str;
            this.compile(token, stmt);
        }
        else {
            final Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, makePropList(adapProps)), null, sinks);
            stmt.sourceText = str;
            this.compile(token, stmt);
        }
    }
    
    public void CreateFilteredSourceStatement_New(final AuthToken authToken, final String sourceName, final Boolean doReplace, final String adapterType, final String adapterVersion, final List<Property> adapterProperty, final String parserType, final String parserVersion, final List<Property> parserProperty, final List<OutputClause> outputClauses) throws MetaDataRepositoryException {
        Validate.notNull((Object)authToken, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Authentication Token", null });
        Validate.notNull((Object)sourceName, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Source name", null });
        Validate.notNull((Object)doReplace, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Replace flag", null });
        Validate.notNull((Object)adapterType, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter name", null });
        Validate.notNull((Object)adapterProperty, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter properties", null });
        Validate.notNull((Object)outputClauses, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Output clause", null });
        Validate.notEmpty((CharSequence)sourceName, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Source name", "empty" });
        Validate.notEmpty((CharSequence)adapterType, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter name", "empty" });
        Validate.notEmpty((Collection)adapterProperty, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Adapter properties", "empty" });
        Validate.notEmpty((Collection)outputClauses, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Output clause", "empty" });
        final OutputClause outputClause = outputClauses.get(0);
        final List<TypeField> typeFields = outputClause.getTypeDefinition();
        final MappedStream mappedStreams = outputClause.getGeneratedStream();
        final String outputStream = outputClause.getStreamName();
        final String selectText = outputClause.getFilterText();
        if (mappedStreams != null) {
            Validate.isTrue(typeFields == null, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Type definition", "not null" });
        }
        if (typeFields != null) {
            Validate.notEmpty((Collection)typeFields, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Type definition", "empty" });
            Validate.isTrue(mappedStreams == null, QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR, new Object[] { "Map clause", "not null" });
        }
        final String sourceNameWithoutDomain = Utility.splitName(sourceName);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateFilteredSourceStatement_New (token " + authToken + ", entityName " + sourceNameWithoutDomain + ", replace " + doReplace + " )"));
        }
        final String str = Utility.createSourceStatementText(sourceName, doReplace, adapterType, adapterVersion, adapterProperty, parserType, parserVersion, parserProperty, outputClauses) + ";";
        try {
            this.compileText(authToken, str);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new MetaDataRepositoryException(e.getMessage(), e);
        }
    }
    
    public void CreateSourceStatement_New(final AuthToken token, final String n, final Boolean r, final String adapType, final String adapVersion, final List<Property> adapProps, final String parserType, final String parserVersion, final List<Property> parserProps, final String instream) throws MetaDataRepositoryException {
        final String sourceNameWithoutDomain = Utility.splitName(n);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateSourceStatement_New (token " + token + ", entityName " + sourceNameWithoutDomain + ", replace " + r + " )"));
        }
        String str;
        if (parserType != null && parserProps != null) {
            str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, adapVersion, adapProps, parserType, parserVersion, parserProps, instream, null);
        }
        else {
            str = Utility.createSourceStatementText(sourceNameWithoutDomain, r, adapType, adapVersion, adapProps, null, null, null, instream, null);
        }
        final ArrayList<OutputClause> sinks = new ArrayList<OutputClause>();
        sinks.add(AST.newOutputClause(instream, null, null, null, null, null));
        if (parserType != null && parserProps != null) {
            final Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapVersion, adapProps), AST.CreateAdapterDesc(parserType, parserVersion, parserProps), sinks);
            stmt.sourceText = str;
            this.compile(token, stmt);
        }
        else {
            final Stmt stmt = AST.CreateSourceStatement(sourceNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapVersion, adapProps), null, sinks);
            stmt.sourceText = str;
            this.compile(token, stmt);
        }
    }
    
    public void CreateTargetStatement(final AuthToken token, final String n, final Boolean r, final String adapType, final String adapVersion, final Map<String, String>[] adapProps, final String formatterType, final String formatterVersion, final Map<String, String>[] formatterProps, final String stream) throws MetaDataRepositoryException {
        final String targetNameWithoutDomain = Utility.splitName(n);
        final String str = Utility.createTargetStatementText(targetNameWithoutDomain, r, adapType, adapVersion, makePropList(adapProps), formatterType, formatterVersion, makePropList(formatterProps), stream);
        Stmt stmt = null;
        if (formatterType != null && formatterProps != null) {
            stmt = AST.CreateTargetStatement(targetNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapVersion, makePropList(adapProps)), AST.CreateAdapterDesc(formatterType, formatterVersion, makePropList(formatterProps)), stream, null);
        }
        else {
            stmt = AST.CreateTargetStatement(targetNameWithoutDomain, r, AST.CreateAdapterDesc(adapType, adapVersion, makePropList(adapProps)), null, stream, null);
        }
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    public void CreateUserStatement(final AuthToken token, final String userid, final String password, final String[] role_name, final String namespace, final Map<String, String>[] props) throws MetaDataRepositoryException {
        final UserProperty userProperty = new UserProperty(Arrays.asList(role_name), namespace, makePropList(props));
        this.compile(token, AST.CreateUserStatement(userid, password, userProperty, null, null));
    }
    
    public void CreateUseSchemaStmt(final AuthToken token, final String schemaName) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("Use NameSpace " + schemaName));
        }
        final String name = Utility.splitName(schemaName);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("Actually using NameSpace: " + name));
        }
        this.compile(token, AST.CreateUseNamespaceStmt(name));
    }
    
    private List<EventType> createEventTypes(final Map<String, List<String>> ets) {
        final List<EventType> ret = new ArrayList<EventType>();
        for (final Map.Entry<String, List<String>> et : ets.entrySet()) {
            final String typeName = et.getKey();
            final List<String> keyFields = et.getValue();
            final EventType evt = new EventType(typeName, keyFields);
            if (keyFields.isEmpty()) {
                throw new RuntimeException("Event field is missing key attribute, so cannot create a hdstore");
            }
            ret.add(evt);
        }
        return ret;
    }
    
    public void CreateWASWithTypeNameStatement(final AuthToken token, final String n, final Boolean r, final String typeName, final Map<String, List<String>> ets, final String howLong, final Map<String, String>[] props) throws MetaDataRepositoryException {
        final String wasNameWithoutDomain = Utility.splitName(n);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateWASWithTypeNameStatement " + wasNameWithoutDomain));
        }
        Interval interval = null;
        if (StringUtils.isNotBlank((CharSequence)howLong)) {
            interval = Utility.parseInterval(howLong);
        }
        final String str = Utility.createWASStatementText(wasNameWithoutDomain, r, typeName, ets, howLong, new HStorePersistencePolicy(interval, makePropList(props)));
        HStorePersistencePolicy hStorePersistencePolicy;
        if (howLong == null) {
            hStorePersistencePolicy = new HStorePersistencePolicy(interval, makePropList(props));
        }
        else {
            hStorePersistencePolicy = new HStorePersistencePolicy(interval, makePropList(props));
        }
        final Stmt stmt = AST.CreateWASStatement(wasNameWithoutDomain, r, new TypeDefOrName(typeName, null), this.createEventTypes(ets), hStorePersistencePolicy);
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    public void CreateWASWithTypeDefStatement(final AuthToken token, final String n, final Boolean r, final Map<String, String>[] typeDef, final Map<String, List<String>> ets, final String howLong, final Map<String, String>[] props) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("CreateWASStatementWithTypeDef" + n));
        }
        this.compile(token, AST.CreateWASStatement(n, r, new TypeDefOrName(null, makeFieldList(typeDef)), this.createEventTypes(ets), new HStorePersistencePolicy(Utility.parseInterval(howLong), makePropList(props))));
    }
    
    public void CreateWindowStatement(final AuthToken token, final String window_name, final Boolean doReplace, final String stream_name, final Map<String, Object> window_len, final boolean isJumping, final String[] partition_fields, final boolean isPersistent, final Map<String, Object> slidePolicy) throws MetaDataRepositoryException {
        WindowUtility.validateIntervalPolicies(isJumping, window_len, slidePolicy);
        final String windowNameWithoutDomain = Utility.splitName(window_name);
        final String str = Utility.createWindowStatementText(windowNameWithoutDomain, doReplace, stream_name, window_len, isJumping, partition_fields, isPersistent);
        final Stmt stmt = AST.CreateWindowStatement(windowNameWithoutDomain, doReplace, stream_name, Pair.make(makeIntervalPolicy(window_len), makeIntervalPolicy(slidePolicy)), isJumping, Arrays.asList(partition_fields));
        stmt.sourceText = str;
        this.compile(token, stmt);
    }
    
    private static IntervalPolicy makeIntervalPolicy(final Map<String, Object> desc) {
        final Object countVal = desc.get("count");
        Object timeVal = desc.get("time");
        Integer countSize = null;
        Interval timeLimit = null;
        Long timeoutVal = null;
        if (countVal != null) {
            if (countVal instanceof String) {
                countSize = Integer.valueOf((String)countVal);
            }
            else if (countVal instanceof Integer) {
                countSize = (Integer)countVal;
            }
            else {
                if (!(countVal instanceof Long)) {
                    throw new RuntimeException("invalid count interval");
                }
                countSize = (int)countVal;
            }
        }
        if (timeVal != null) {
            timeVal = desc.get("time");
            if (timeVal instanceof String) {
                timeLimit = Utility.parseInterval((String)timeVal);
            }
            else {
                if (!(timeVal instanceof Long)) {
                    throw new RuntimeException("invalid time interval");
                }
                timeLimit = new Interval((long)timeVal);
            }
        }
        final Object timeout = desc.get("range");
        final String range = null;
        if (timeout != null) {
            if (timeout instanceof String) {
                timeoutVal = Long.valueOf((String)timeout);
            }
            else {
                if (!(timeout instanceof Long)) {
                    throw new RuntimeException("invalid RANGE attribute");
                }
                timeoutVal = (Long)timeout;
            }
        }
        final Object onField = desc.get("on");
        String field = null;
        if (onField != null) {
            if (!(onField instanceof String)) {
                throw new RuntimeException("invalid ON attribute");
            }
            field = (String)onField;
        }
        if (timeoutVal != null && field != null && !field.isEmpty() && timeLimit != null) {
            return IntervalPolicy.createTimeAttrPolicy(timeLimit, field, timeoutVal);
        }
        if (countSize != null && timeoutVal != null && (field == null || field.isEmpty())) {
            return IntervalPolicy.createTimeCountPolicy(new Interval(timeoutVal), countSize);
        }
        if (countSize != null && timeLimit != null) {
            return IntervalPolicy.createTimeCountPolicy(timeLimit, countSize);
        }
        if (field != null && !field.isEmpty() && timeLimit != null) {
            return IntervalPolicy.createAttrPolicy(field, timeLimit.value);
        }
        if (countSize != null) {
            return IntervalPolicy.createCountPolicy(countSize);
        }
        if (timeLimit != null) {
            return IntervalPolicy.createTimePolicy(timeLimit);
        }
        assert false;
        return null;
    }
    
    public void ImportClassDeclaration(final AuthToken token, final String pkgname, final boolean isStatic) throws MetaDataRepositoryException {
        this.compile(token, AST.ImportClassDeclaration(pkgname, isStatic));
    }
    
    public void ImportPackageDeclaration(final AuthToken token, final String pkgname, final boolean isStatic) throws MetaDataRepositoryException {
        this.compile(token, AST.ImportPackageDeclaration(pkgname, isStatic));
    }
    
    public void CreateCqStatement(final AuthToken token, final String cq_name, final Boolean doReplace, final String dest_stream_name, final String[] field_name_list, final String selectText, final String uiConfig) throws Exception {
        final Context context = this.getContext(token);
        final String tqlText = selectText + (selectText.endsWith(";") ? "" : ";");
        final boolean oldVal = context.setReadOnly(!this.updateMode);
        try {
            Compiler.compile(tqlText, context, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws Exception {
                    if (!(stmt instanceof CreateAdHocSelectStmt)) {
                        throw new RuntimeException("invalid select statement");
                    }
                    final Select select = ((CreateAdHocSelectStmt)stmt).select;
                    final String cqNameWithoutDomain = Utility.splitName(cq_name);
                    final Stmt cqStmt = AST.CreateCqStatement(cqNameWithoutDomain, doReplace, dest_stream_name, Arrays.asList(field_name_list), select, selectText, uiConfig);
                    final ObjectName objectName = context.makeObjectName(dest_stream_name);
                    final String streamName = Compiler.getNameForTQL(context, objectName);
                    cqStmt.sourceText = Utility.createCQStatementText(cqNameWithoutDomain, doReplace, streamName, field_name_list, tqlText);
                    compiler.compileStmt(cqStmt);
                }
            });
        }
        finally {
            context.setReadOnly(oldVal);
        }
    }
    
    public void DropStatement(final AuthToken token, final String entityType, final String entityName, final boolean doCascade, final boolean doForce) throws MetaDataRepositoryException {
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("DropStatement (token " + token + ", entityType " + entityType + ", entityName " + entityName + ", entityName " + doCascade + " , doForce " + doForce + ")"));
        }
        final EntityType what = EntityType.forObject(entityType);
        DropMetaObject.DropRule rule = DropMetaObject.DropRule.NONE;
        if (what == EntityType.UNKNOWN) {
            throw new RuntimeException("unknown object type <" + entityType + ">");
        }
        if (doCascade && doForce) {
            rule = DropMetaObject.DropRule.ALL;
        }
        if (doCascade) {
            rule = DropMetaObject.DropRule.CASCADE;
        }
        if (doForce) {
            rule = DropMetaObject.DropRule.FORCE;
        }
        this.compile(token, AST.DropStatement(what, entityName, rule));
    }
    
    public String CreateShowStreamStatement(final AuthToken token, final String streamName, final int lineCount) throws MetaDataRepositoryException {
        this.compile(token, AST.CreateShowStmt(streamName, lineCount, false));
        return "consoleQueue" + token.toString();
    }
    
    public List<MetaInfo.MetaObject> getAllObjectsByEntityType(final String[] eTypes, final AuthToken token) throws MetaDataRepositoryException {
        if (eTypes instanceof String[]) {
            final List<MetaInfo.MetaObject> mObjects = new ArrayList<MetaInfo.MetaObject>();
            for (final String eType : eTypes) {
                final Set<MetaInfo.MetaObject> metaObjectSet = (Set<MetaInfo.MetaObject>)this.metadataRepository.getByEntityType(EntityType.forObject(eType), token);
                if (metaObjectSet != null) {
                    mObjects.addAll(metaObjectSet);
                }
            }
            if (mObjects != null) {
                final Set<? extends MetaInfo.MetaObject> result = Utility.removeInternalApplications(new HashSet<MetaInfo.MetaObject>(mObjects));
                if (result != null) {
                    return new ArrayList<MetaInfo.MetaObject>(result);
                }
            }
            return null;
        }
        throw new RuntimeException("Expected String Array, Passed : " + eTypes.getClass().toString());
    }
    
    private MetaInfo.Query compileQuery(final AuthToken token, final String text) throws Exception {
        final ObjectReference<MetaInfo.Query> type = new ObjectReference<MetaInfo.Query>();
        if (QueryValidator.logger.isDebugEnabled()) {
            QueryValidator.logger.debug((Object)("Compiling text: " + text));
        }
        final Context context = this.getContext(token);
        final boolean oldVal = context.setReadOnly(!this.updateMode);
        try {
            Compiler.compile(text, context, new Compiler.ExecutionCallback() {
                @Override
                public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                    if (QueryValidator.logger.isDebugEnabled()) {
                        QueryValidator.logger.debug((Object)("Compiling stmt" + stmt.toString()));
                    }
                    if (stmt instanceof CreateAdHocSelectStmt) {
                        final CreateAdHocSelectStmt adhoc = (CreateAdHocSelectStmt)stmt;
                        final MetaInfo.Query q = compiler.compileCreateAdHocSelect(adhoc);
                        type.set(q);
                        if (!(stmt instanceof CreatePropertySetStmt) && !(stmt instanceof CreateUserStmt)) {
                            if (!(stmt instanceof ConnectStmt)) {
                                QueryValidator.this.storeCommand(token, QueryValidator.this.getUserId(token), stmt.sourceText);
                            }
                        }
                    }
                }
            });
        }
        finally {
            context.setReadOnly(oldVal);
        }
        return type.get();
    }
    
    public MetaInfo.Query compileAdHocText(final AuthToken token, final String text) throws Exception {
        return this.compileQuery(token, text);
    }
    
    @Override
    public MetaInfo.Query createNamedQuery(final AuthToken token, final String queryDefinition) throws Exception {
        return this.compileQuery(token, queryDefinition);
    }
    
    @Override
    public MetaInfo.Query createAdhocQuery(final AuthToken token, final String queryDefinition) throws Exception {
        return this.compileQuery(token, queryDefinition);
    }
    
    @Override
    public List<MetaInfo.Query> listAllQueries(final AuthToken authToken) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        return context.listQueries(new TYPE[] { TYPE.ADHOC, TYPE.NAMEDQUERY }, authToken);
    }
    
    @Override
    public List<MetaInfo.Query> listAdhocQueries(final AuthToken authToken) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        return context.listQueries(new TYPE[] { TYPE.ADHOC }, authToken);
    }
    
    @Override
    public List<MetaInfo.Query> listNamedQueries(final AuthToken authToken) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        return context.listQueries(new TYPE[] { TYPE.NAMEDQUERY }, authToken);
    }
    
    public MetaInfo.Query createNamedQuery(final AuthToken token, final String queryDefinition, final String alias) throws Exception {
        return this.createNamedQuery(token, "CREATE NAMEDQUERY " + alias + " " + queryDefinition);
    }
    
    @Override
    public MetaInfo.Query createParameterizedQuery(final boolean doReplace, final AuthToken authToken, final String query, final String alias) throws Exception {
        if (query == null) {
            throw new IllegalArgumentException("Invalid query.");
        }
        final Context context = this.getContext(authToken);
        if (alias.indexOf(".") == -1) {
            throw new RuntimeException("Wrong query name to create.");
        }
        final String nsname = Utility.splitDomain(alias);
        final String name = Utility.splitName(alias);
        MetaInfo.Namespace ns = null;
        if (nsname == null) {
            ns = context.getCurNamespace();
        }
        else {
            final List<MetaInfo.Namespace> nss = this.metadataRepository.getAllNamespaces(authToken);
            for (final MetaInfo.Namespace ans : nss) {
                if (ans.name.equalsIgnoreCase(nsname)) {
                    ns = ans;
                    break;
                }
            }
            if (ns == null) {
                throw new RuntimeException("Namespace " + nsname + " for query " + name + " does not exist");
            }
        }
        final MetaInfo.Query queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, nsname, name, null, authToken);
        if (queryMetaObject != null && !doReplace) {
            throw new RuntimeException("Query already exists, user create or replace");
        }
        if (queryMetaObject != null && doReplace) {
            this.deleteNamedQueryByUUID(authToken, queryMetaObject.getUuid());
        }
        MetaInfo.Query queryInstance = null;
        try {
            queryInstance = this.createNamedQuery(authToken, query, alias);
        }
        catch (Exception e) {
            queryInstance = new MetaInfo.Query(name, ns, null, null, null, null, query, false);
            queryInstance.getMetaInfoStatus().setValid(false);
            this.metadataRepository.putMetaObject(queryInstance, authToken);
            throw new RuntimeException("Cannot compile the query, query " + nsname + "." + name + " invalidated, due to: " + e.getMessage());
        }
        return queryInstance;
    }
    
    public MetaInfo.Query execParameterizedQuery(final AuthToken authToken, final UUID queryId, final Map<String, String>[] params) throws Exception {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = (MetaInfo.Query)context.getObject(queryId);
        final MetaInfo.Query dup = context.prepareQuery(query, makePropList(params));
        if (QueryValidator.logger.isDebugEnabled()) {
            QueryValidator.logger.debug((Object)("Query duplicated ready to run:  " + dup.getFullName() + " : UUID " + dup.appUUID));
        }
        context.startAdHocQuery(dup);
        return dup;
    }
    
    public MetaInfo.Query prepareZoomedQuery(final AuthToken authToken, final UUID queryId, final Map<String, Object> params, final String fieldName, final String startTime, final String endTime) throws Exception {
        final MetaInfo.Query query = this.prepareQuery(authToken, queryId, params);
        final MetaInfo.Flow app = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
        if (app != null) {
            final Set<UUID> wastoreviews = app.getObjects().get(EntityType.WASTOREVIEW);
            if (wastoreviews.size() == 1) {
                final UUID uuid = wastoreviews.iterator().next();
                final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)this.metadataRepository.getMetaObjectByUUID(uuid, authToken);
                JsonNode queryJson = AST2JSON.deserialize(wastoreview.query);
                queryJson = timeFilterZoom(queryJson, fieldName, startTime, endTime);
                final byte[] updatedQuery = AST2JSON.serialize(queryJson);
                wastoreview.setQuery(updatedQuery);
                this.metadataRepository.updateMetaObject(wastoreview, authToken);
            }
        }
        return query;
    }
    
    public MetaInfo.Query prepareLikeQuery(final AuthToken authToken, final UUID queryId, final Map<String, Object> params, final String likeField) throws Exception {
        final MetaInfo.Query query = this.prepareQuery(authToken, queryId, params);
        final MetaInfo.Flow app = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
        if (app != null) {
            final Set<UUID> wastoreviews = app.getObjects().get(EntityType.WASTOREVIEW);
            if (wastoreviews.size() == 1) {
                final UUID uuid = wastoreviews.iterator().next();
                final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)this.metadataRepository.getMetaObjectByUUID(uuid, authToken);
                final JsonNode queryJson = AST2JSON.deserialize(wastoreview.query);
                AST2JSON.addLike(queryJson, likeField);
                final byte[] updatedQuery = AST2JSON.serialize(queryJson);
                wastoreview.setQuery(updatedQuery);
                this.metadataRepository.updateMetaObject(wastoreview, authToken);
            }
        }
        return query;
    }
    
    public MetaInfo.Query prepareFilteredQuery(final AuthToken authToken, final UUID queryId, final List<String>[] attributes) throws Exception {
        final MetaInfo.Query query = this.prepareQuery(authToken, queryId, new HashMap<String, Object>());
        final MetaInfo.Flow app = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
        if (app != null) {
            final Set<UUID> wastoreviews = app.getObjects().get(EntityType.WASTOREVIEW);
            if (wastoreviews.size() == 1) {
                final UUID uuid = wastoreviews.iterator().next();
                final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)this.metadataRepository.getMetaObjectByUUID(uuid, authToken);
                JsonNode queryJson = AST2JSON.deserialize(wastoreview.query);
                for (int i = 0; i < attributes.length; ++i) {
                    if (attributes[i].get(0).equalsIgnoreCase("anyattrlike")) {
                        AST2JSON.addLike(queryJson, attributes[i].get(1));
                    }
                    else if (attributes[i].get(0).equalsIgnoreCase("rangefilter")) {
                        queryJson = timeFilterZoom(queryJson, attributes[i].get(1), attributes[i].get(2), attributes[i].get(3));
                    }
                    else if (attributes[i].get(0).equalsIgnoreCase("attrfilter")) {
                        AST2JSON.addAttrFilter(queryJson, attributes[i].get(1), attributes[i].get(2));
                    }
                }
                final byte[] updatedQuery = AST2JSON.serialize(queryJson);
                wastoreview.setQuery(updatedQuery);
                this.metadataRepository.updateMetaObject(wastoreview, authToken);
            }
        }
        return query;
    }
    
    private static JsonNode timeFilterZoom(final JsonNode jsonNode, final String fieldName, final String startAmount, final String endAmount) throws IOException {
        AST2JSON.addTimeBounds(jsonNode, fieldName, Long.parseLong(startAmount), Long.parseLong(endAmount));
        return jsonNode;
    }
    
    @Override
    public MetaInfo.Query prepareQuery(final AuthToken authToken, final UUID queryId, final Map<String, Object> params) throws Exception {
        final Context context = this.getContext(authToken);
        Map<String, String>[] paramArgs = null;
        if (queryId == null) {
            throw new RuntimeException("Query doesn't exist.");
        }
        MetaInfo.Query query = (MetaInfo.Query)context.getObject(queryId);
        if (query == null) {
            throw new RuntimeException("Query doesn't exist.");
        }
        if (!query.getMetaInfoStatus().isValid()) {
            try {
                this.CreateAlterRecompileStmt(authToken, "QUERY", query.getFullName());
                query = (MetaInfo.Query)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.QUERY, query.getNsName(), query.getName(), null, authToken);
            }
            catch (Exception e) {
                throw new RuntimeException("Query " + query.getFullName() + " is not valid, potentially queried object doesn't exist.");
            }
        }
        if (params.isEmpty()) {
            final MetaInfo.CQ cq = (MetaInfo.CQ)this.metadataRepository.getMetaObjectByUUID(query.cqUUID, authToken);
            if (cq != null && cq.plan != null && cq.plan.paramsDesc != null && cq.plan.paramsDesc.size() > 0) {
                paramArgs = (Map<String, String>[])new HashMap[cq.plan.paramsDesc.size()];
                int paramCounter = 0;
                for (final ParamDesc d : cq.plan.paramsDesc) {
                    final Map<String, String> paramMapping = new HashMap<String, String>();
                    paramMapping.put(d.paramName, null);
                    paramArgs[paramCounter] = paramMapping;
                    ++paramCounter;
                }
            }
            else {
                paramArgs = (Map<String, String>[])new HashMap[0];
            }
        }
        else {
            paramArgs = (Map<String, String>[])new HashMap[] { (HashMap)params };
        }
        paramArgs = this.checkIfAllParametersSet(query, paramArgs, authToken);
        final MetaInfo.Query dup = context.prepareQuery(query, makePropList(paramArgs));
        if (QueryValidator.logger.isDebugEnabled()) {
            QueryValidator.logger.debug((Object)("ORIGINAL QUERY : " + query));
            QueryValidator.logger.debug((Object)("DUPLICATED QUERY :  " + dup.getFullName() + " : UUID " + dup.getUuid()));
            QueryValidator.logger.debug((Object)("QUERY OBJECTS : " + this.metadataRepository.getMetaObjectByUUID(query.appUUID, HSecurityManager.TOKEN).getDependencies()));
            QueryValidator.logger.debug((Object)("DUPLICATED QUERY OBJECTS :  " + dup.getFullName() + " : UUID " + this.metadataRepository.getMetaObjectByUUID(dup.appUUID, HSecurityManager.TOKEN).getDependencies()));
            QueryValidator.logger.debug((Object)("ORIGINAL CQ : " + this.metadataRepository.getMetaObjectByUUID(query.cqUUID, HSecurityManager.TOKEN)));
            QueryValidator.logger.debug((Object)("DUPLICATED CQ :  " + this.metadataRepository.getMetaObjectByUUID(dup.cqUUID, HSecurityManager.TOKEN)));
        }
        dup.projectionFields = query.projectionFields;
        this.metadataRepository.updateMetaObject(dup, authToken);
        return dup;
    }
    
    Map<String, String>[] checkIfAllParametersSet(final MetaInfo.Query query, final Map<String, String>[] paramArgs, final AuthToken authToken) throws MetaDataRepositoryException {
        final Set<String> expected = new HashSet<String>();
        final Set<String> calculated = new HashSet<String>();
        final MetaInfo.CQ cq = (MetaInfo.CQ)this.metadataRepository.getMetaObjectByUUID(query.cqUUID, authToken);
        if (cq != null && cq.plan != null && cq.plan.paramsDesc != null && cq.plan.paramsDesc.size() > 0) {
            for (final ParamDesc d : cq.plan.paramsDesc) {
                expected.add(d.paramName);
            }
        }
        for (final Map<String, String> paramMap : paramArgs) {
            for (final Map.Entry<String, String> params : paramMap.entrySet()) {
                calculated.add(params.getKey());
            }
        }
        final Set<String> missing = new HashSet<String>();
        for (final String string : expected) {
            if (!calculated.contains(string)) {
                missing.add(string);
            }
        }
        if (!missing.isEmpty()) {
            final Map<String, String>[] result = (Map<String, String>[])new HashMap[paramArgs.length + missing.size()];
            int i;
            for (i = 0; i < paramArgs.length; ++i) {
                result[i] = paramArgs[i];
            }
            for (final String missingParam : missing) {
                final HashMap map = new HashMap();
                map.put(missingParam, null);
                result[i] = (Map<String, String>)map;
                ++i;
            }
            return result;
        }
        return paramArgs;
    }
    
    private static QueryParameters extractParametersInTQL(final String word) {
        final Pattern stringParamMatch = Pattern.compile("\"[:](\\w*)\"");
        final Matcher stringMatch = stringParamMatch.matcher(word);
        final Set<String> stringParams = new HashSet<String>();
        while (stringMatch.find()) {
            stringParams.add(stringMatch.group(1));
        }
        final Pattern paramMatch = Pattern.compile("(\\W)[:](\\w*)(\\W)");
        final Matcher match = paramMatch.matcher(word);
        final Set<String> params = new HashSet<String>();
        while (match.find()) {
            if (!match.group(1).equals("\"")) {
                params.add(match.group(2));
            }
        }
        final QueryParameters queryParameters = new QueryParameters();
        queryParameters.setStringParams(stringParams);
        queryParameters.setOtherParams(new HashSet<String>());
        for (final String string : params) {
            queryParameters.addOtherParams(string);
        }
        return queryParameters;
    }
    
    private static String setParametersInTQL(String word, final Map<String, Object> params) {
        if (params == null) {
            final QueryParameters queryParameters = extractParametersInTQL(word);
            final Set<String> stringParams = queryParameters.getStringParams();
            final Set<String> otherParams = queryParameters.getOtherParams();
            for (final String paramName : stringParams) {
                word = word.replaceAll("[:]" + paramName + " IS NULL", "FALSE");
                word = word.replaceAll("(\")[:]" + paramName + "(\")", "$1$2");
            }
            for (final String paramName : otherParams) {
                word = word.replaceAll("[:]" + paramName + " IS NULL", "FALSE");
                word = word.replaceAll("(\\W)[:]" + paramName + "(\\W)", "$10$2");
            }
            return word;
        }
        final Set<String> paramNames = extractParametersInTQL(word).allParams();
        for (final String paramName2 : paramNames) {
            if (params.containsKey(paramName2)) {
                word = word.replaceAll("[:]" + paramName2 + " IS NULL", "FALSE");
                word = word.replaceAll("(\\W)[:]" + paramName2 + "(\\W)", "$1" + params.get(paramName2) + "$2");
            }
            else {
                word = word.replaceAll("[:]" + paramName2 + " IS NULL", "TRUE");
                word = word.replaceAll("(\\W)[:]" + paramName2 + "(\\W)", "$1NULL$2");
            }
        }
        return word;
    }
    
    private MetaInfo.Query getQuery(final UUID queryId, final AuthToken authToken) throws MetaDataRepositoryException {
        return (MetaInfo.Query)this.metadataRepository.getMetaObjectByUUID(queryId, authToken);
    }
    
    private MetaInfo.Query getQuery(final String queryName, final AuthToken authToken) throws MetaDataRepositoryException {
        return (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(queryName), Utility.splitName(queryName), null, authToken);
    }
    
    @Override
    public void deleteAdhocQuery(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null || !query.isAdhocQuery()) {
            return;
        }
        context.deleteQuery(query, authToken);
    }
    
    @Override
    public void deleteNamedQueryByUUID(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null || query.isAdhocQuery()) {
            return;
        }
        context.deleteQuery(query, authToken);
    }
    
    @Override
    public void deleteNamedQueryByName(final AuthToken authToken, final String queryName) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryName, authToken);
        if (query == null || query.isAdhocQuery()) {
            return;
        }
        context.deleteQuery(query, authToken);
    }
    
    @Override
    public MetaInfo.Query cloneNamedQueryFromAdhoc(final AuthToken authToken, final UUID queryId, final String alias) throws Exception {
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null || !query.isAdhocQuery()) {
            throw new FatalException("Wrong adhoc query");
        }
        final String queryText = query.queryDefinition;
        this.stopAdhocQuery(authToken, queryId);
        this.deleteAdhocQuery(authToken, queryId);
        return this.createNamedQuery(authToken, "CREATE NAMEDQUERY " + alias + " " + queryText + ";");
    }
    
    @Override
    public boolean startAdhocQuery(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null) {
            throw new FatalException("No such query exists");
        }
        final MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
        if (applicationMetaObject == null) {
            throw new FatalException("No such application exists");
        }
        try {
            context.startAdHocQuery(query);
        }
        catch (Exception e) {
            context.deleteQuery(query, authToken);
            throw new RuntimeException(this.findRootThrowable(e).getMessage());
        }
        return true;
    }
    
    private Throwable findRootThrowable(final Throwable e) {
        if (e.getCause() == null) {
            return e;
        }
        return this.findRootThrowable(e.getCause());
    }
    
    @Override
    public boolean startNamedQuery(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null) {
            throw new FatalException("No such query exists");
        }
        final MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(query.appUUID, authToken);
        if (applicationMetaObject == null) {
            throw new FatalException("No such application exists");
        }
        context.startAdHocQuery(query);
        return true;
    }
    
    @Override
    public boolean stopAdhocQuery(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query != null) {
            context.stopAdHocQuery(query);
            context.deleteQuery(query, authToken);
        }
        return true;
    }
    
    @Override
    public boolean stopNamedQuery(final AuthToken authToken, final UUID queryId) throws MetaDataRepositoryException {
        final Context context = this.getContext(authToken);
        final MetaInfo.Query query = this.getQuery(queryId, authToken);
        if (query == null) {
            throw new FatalException("No such query exists");
        }
        context.stopAdHocQuery(query);
        return true;
    }
    
    public void createReportStartStmt(final AuthToken token, final String appID) throws Exception {
        final Context context = this.getContext(token);
        MetaInfo.Flow flowObj = null;
        final String[] sNames = context.splitNamespaceAndName(appID, EntityType.APPLICATION);
        flowObj = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, sNames[0], sNames[1], -1, token);
        if (flowObj == null) {
            throw new MetaDataRepositoryException("Unable to find object or no permissions for " + appID);
        }
        context.executeReportAction(flowObj.getUuid(), 1, null, true, token);
    }
    
    public JsonNode createReportEndStmt(final AuthToken token, final String appID) throws Exception {
        final Context context = this.getContext(token);
        MetaInfo.Flow flowObj = null;
        final String[] sNames = context.splitNamespaceAndName(appID, EntityType.APPLICATION);
        flowObj = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, sNames[0], sNames[1], -1, token);
        if (flowObj == null) {
            throw new MetaDataRepositoryException("Unable to find object or no permissions for " + appID);
        }
        return context.executeReportAction(flowObj.getUuid(), 2, null, true, token);
    }
    
    public boolean isReportRunning(final AuthToken token, final String appName) throws Exception {
        final Context context = this.getContext(token);
        if (context == null) {
            return false;
        }
        MetaInfo.Flow flowObj = null;
        final String[] sNames = context.splitNamespaceAndName(appName, EntityType.APPLICATION);
        flowObj = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByName(EntityType.APPLICATION, sNames[0], sNames[1], -1, token);
        if (flowObj == null) {
            throw new MetaDataRepositoryException("Unable to find object or no permissions for " + appName);
        }
        return context.isReportInProgress(flowObj.getUuid());
    }
    
    @Override
    public MetaInfo.Query createAdhocQueryFromJSON(final AuthToken authToken, final String queryDefinition) throws Exception {
        return this.createAdhocQuery(authToken, CQUtility.convertJSONToSQL(queryDefinition));
    }
    
    @Override
    public MetaInfo.Query createNamedQueryFromJSON(final AuthToken authToken, final String queryDefinition) throws Exception {
        return this.createNamedQuery(authToken, CQUtility.convertJSONToSQL(queryDefinition));
    }
    
    @Override
    public String exportDashboard(final AuthToken authToken, final String dashboardName) throws Exception {
        final VisualizationArtifacts va = new VisualizationArtifacts();
        final MetaInfo.Dashboard dashboard = (MetaInfo.Dashboard)this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, Utility.splitDomain(dashboardName), Utility.splitName(dashboardName), null, authToken);
        if (dashboard == null) {
            throw new RuntimeException("No such dashboard exists");
        }
        va.setDashboard(dashboard);
        for (final String pageName : dashboard.getPages()) {
            MetaInfo.Page page = null;
            if (pageName.indexOf(".") == -1) {
                page = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(dashboardName), pageName, null, authToken);
            }
            else {
                page = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(pageName), Utility.splitName(pageName), null, authToken);
            }
            va.addPages(page);
            if (page == null) {
                throw new RuntimeException("No such Page exists");
            }
            for (final String queryVisualizationName : page.getQueryVisualizations()) {
                MetaInfo.QueryVisualization queryVisualization = null;
                if (queryVisualizationName.indexOf(".") == -1) {
                    queryVisualization = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(dashboardName), queryVisualizationName, null, authToken);
                }
                else {
                    queryVisualization = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(queryVisualizationName), Utility.splitName(queryVisualizationName), null, authToken);
                }
                if (queryVisualization == null) {
                    throw new RuntimeException("No such QueryVisualization exists");
                }
                va.addQueryVisualization(queryVisualization);
                final String queryName = queryVisualization.getQuery();
                MetaInfo.Query queryMetaObject = null;
                if (queryName == null) {
                    continue;
                }
                if (queryName.isEmpty()) {
                    continue;
                }
                if (queryName.indexOf(".") == -1) {
                    queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(dashboardName), Utility.splitName(queryName), null, authToken);
                }
                else {
                    queryMetaObject = (MetaInfo.Query)this.metadataRepository.getMetaObjectByName(EntityType.QUERY, Utility.splitDomain(queryName), Utility.splitName(queryName), null, authToken);
                }
                if (queryName != null && !queryName.isEmpty() && queryMetaObject == null) {
                    throw new RuntimeException("No such Query exists");
                }
                va.addParametrizedQuery(queryMetaObject);
            }
        }
        return va.convertToJSON();
    }
    
    @Override
    public UUID importDashboard(final AuthToken authToken, final Boolean doReplace, final String fileName, final String fullJSON) throws Exception {
        final Context context = this.getContext(authToken);
        if (fullJSON == null || fullJSON.isEmpty()) {
            final String json = Compiler.readFile((CreateDashboardStatement)AST.CreateDashboardStatement(doReplace, fileName));
            UUID dashboardUUID = null;
            try {
                dashboardUUID = context.createDashboard(json, null);
            }
            catch (Exception e) {
                QueryValidator.logger.warn((Object)e.getMessage());
            }
            return dashboardUUID;
        }
        return context.createDashboard(fullJSON, null);
    }
    
    @Override
    public UUID createQueryIfNotExists(final AuthToken authToken, final String queryName, final String queryDefinition) throws Exception {
        final Context context = this.getContext(authToken);
        MetaInfo.Query query = context.get(queryName, EntityType.QUERY);
        if (query != null) {
            return query.getUuid();
        }
        final String queryTQL = "CREATE NAMEDQUERY " + queryName + " " + queryDefinition;
        try {
            query = this.createNamedQuery(authToken, queryTQL);
        }
        catch (Exception e) {
            QueryValidator.logger.warn((Object)e.getMessage());
        }
        return query.getUuid();
    }
    
    @Override
    public UUID importDashboard(final AuthToken authToken, final String toNamespace, final Boolean doReplace, final String fileName, final String fullJSON) throws Exception {
        final Context context = this.getContext(authToken);
        if (fullJSON == null || fullJSON.isEmpty()) {
            final String json = Compiler.readFile((CreateDashboardStatement)AST.CreateDashboardStatement(doReplace, fileName));
            UUID dashboardUUID = null;
            try {
                dashboardUUID = context.createDashboard(json, toNamespace);
            }
            catch (Exception e) {
                QueryValidator.logger.warn((Object)e.getMessage());
            }
            return dashboardUUID;
        }
        return context.createDashboard(fullJSON, toNamespace);
    }
    
    public void CreateDashboard(final AuthToken token, final String objectName, final String namespace, final Boolean cor, final ObjectNode data) throws JSONException, MetaDataRepositoryException {
        MetaInfo.Dashboard dashBoardMetaObject = (MetaInfo.Dashboard)this.metadataRepository.getMetaObjectByName(EntityType.DASHBOARD, namespace, objectName, null, token);
        boolean isNew;
        if (dashBoardMetaObject == null) {
            assert !cor;
            dashBoardMetaObject = new MetaInfo.Dashboard();
            final MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
            dashBoardMetaObject.construct(objectName, ns, EntityType.DASHBOARD);
            isNew = true;
        }
        else {
            assert cor;
            isNew = false;
        }
        if (data.has("title")) {
            final JsonNode titleNode = data.get("title");
            if (titleNode != null) {
                dashBoardMetaObject.setTitle(titleNode.asText());
            }
        }
        if (data.has("defaultLandingPage")) {
            final JsonNode defaultPageNode = data.get("defaultLandingPage");
            if (defaultPageNode != null) {
                dashBoardMetaObject.setDefaultLandingPage(defaultPageNode.asText());
            }
        }
        if (data.has("pages")) {
            final JsonNode pagesNode = data.get("pages");
            if (pagesNode != null) {
                assert pagesNode.isArray();
                final ArrayNode an = (ArrayNode)pagesNode;
                final Iterator<JsonNode> itr = (Iterator<JsonNode>)an.elements();
                final List<String> allPages = new ArrayList<String>();
                while (itr.hasNext()) {
                    final JsonNode pNode = itr.next().get("name");
                    if (pNode == null) {
                        throw new JSONException("Page Id can't be NULL");
                    }
                    allPages.add(pNode.asText());
                }
                dashBoardMetaObject.setPages(allPages);
            }
        }
        if (isNew) {
            this.metadataRepository.putMetaObject(dashBoardMetaObject, token);
        }
        else {
            this.metadataRepository.updateMetaObject(dashBoardMetaObject, token);
        }
    }
    
    public void CreatePage(final AuthToken token, final String objectName, final String namespace, final Boolean cor, final ObjectNode data) throws MetaDataRepositoryException, JSONException {
        MetaInfo.Page p = (MetaInfo.Page)this.metadataRepository.getMetaObjectByName(EntityType.PAGE, namespace, objectName, null, token);
        boolean isNew = false;
        if (p == null) {
            assert !cor;
            p = new MetaInfo.Page();
            final MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
            p.construct(objectName, ns, EntityType.PAGE);
            isNew = true;
        }
        else {
            assert cor;
            isNew = false;
        }
        if (data.has("title")) {
            final JsonNode titleNode = data.get("title");
            if (titleNode != null) {
                p.setTitle(titleNode.asText(" "));
            }
        }
        if (data.has("queryVisualizations")) {
            final ArrayNode visualizationsNode = (ArrayNode)data.get("queryVisualizations");
            if (visualizationsNode != null) {
                assert visualizationsNode.isArray();
                final Iterator<JsonNode> itr = (Iterator<JsonNode>)visualizationsNode.elements();
                final List<String> allVisualizations = new ArrayList<String>();
                while (itr.hasNext()) {
                    allVisualizations.add(itr.next().textValue());
                }
                p.setQueryVisualizations(allVisualizations);
            }
        }
        if (data.has("gridJSON")) {
            final JsonNode gridJSONNode = data.get("gridJSON");
            if (gridJSONNode != null) {
                p.setGridJSON(gridJSONNode.asText());
            }
        }
        if (isNew) {
            this.metadataRepository.putMetaObject(p, token);
        }
        else {
            this.metadataRepository.updateMetaObject(p, token);
        }
    }
    
    public void CreateQueryVisualization(final AuthToken token, final String objectName, final String namespace, final Boolean cor, final ObjectNode data) throws MetaDataRepositoryException, JSONException {
        MetaInfo.QueryVisualization uic = (MetaInfo.QueryVisualization)this.metadataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, namespace, objectName, null, token);
        boolean isNew = false;
        if (uic == null) {
            uic = new MetaInfo.QueryVisualization();
            final MetaInfo.Namespace ns = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
            uic.construct(objectName, ns, EntityType.QUERYVISUALIZATION);
            isNew = true;
        }
        if (data.has("title")) {
            final JsonNode titleNode = data.get("title");
            if (titleNode != null) {
                uic.setTitle(titleNode.asText());
            }
        }
        if (data.has("query")) {
            final JsonNode queryNode = data.get("query");
            String query = null;
            if (queryNode.has("name")) {
                String queryNSName = uic.nsName;
                MetaInfo.Namespace queryNS = null;
                if (queryNode.has("nsName")) {
                    queryNSName = queryNode.get("nsName").asText();
                    queryNS = (MetaInfo.Namespace)this.metadataRepository.getMetaObjectByName(EntityType.NAMESPACE, "Global", queryNSName, null, token);
                }
                if (queryNS != null && !queryNSName.isEmpty() && !queryNSName.equalsIgnoreCase(namespace)) {
                    query = queryNS.getName() + "." + "QUERY" + "." + queryNode.get("name").asText();
                }
                else {
                    query = queryNode.get("name").asText();
                }
            }
            if (query == null) {
                query = queryNode.asText();
            }
            if (query != null) {
                uic.setQuery(query);
            }
        }
        if (data.has("visualizationType")) {
            final JsonNode visualizationTypeNode = data.get("visualizationType");
            if (visualizationTypeNode != null) {
                uic.setVisualizationType(visualizationTypeNode.asText());
            }
        }
        if (data.has("config")) {
            final JsonNode configNode = data.get("config");
            if (configNode != null) {
                uic.setConfig(configNode.toString());
            }
        }
        if (isNew) {
            this.metadataRepository.putMetaObject(uic, token);
        }
        else {
            this.metadataRepository.updateMetaObject(uic, token);
        }
    }
    
    public ObjectNode[] CRUDHandler(final AuthToken token, final String appOrFlowName, final ClientOperations.CRUD operation, final ObjectNode node) throws Exception {
        if (token == null) {
            throw new SecurityException("Can't process a request without an authentication token");
        }
        if (operation == null) {
            throw new InvalidPropertiesFormatException("Can't process a request without knowing the kind of operation to be executed");
        }
        final JsonNode namespaceNode = node.get("nsName");
        final String namespace = (namespaceNode != null) ? namespaceNode.asText(" ") : null;
        final JsonNode entityNode = node.get("type");
        final EntityType entityType = (entityNode != null) ? EntityType.valueOf(entityNode.asText(" ")) : null;
        if (entityType == null) {
            throw new InvalidPropertiesFormatException("Entity Type can't be null");
        }
        if (entityType != null && entityType.equals(EntityType.UNKNOWN)) {
            throw new InvalidPropertiesFormatException(entityType + " is invalid Entity Type");
        }
        final JsonNode name = node.get("name");
        final StringBuilder stringToValidate = new StringBuilder();
        if (name != null) {
            if (StringUtils.isNotBlank((CharSequence)namespace)) {
                stringToValidate.append(namespace).append(".");
            }
            stringToValidate.append(name.asText());
            if (!isValidName(stringToValidate.toString())) {
                throw new InvalidPropertiesFormatException(stringToValidate.toString() + " is invalid name");
            }
        }
        return this.clientOperations.CRUDWorker(this, token, namespace, appOrFlowName, entityType, operation, node);
    }
    
    public Map<String, ObjectNode> actionHandler(final List<String> list, final AuthToken token) throws MetaDataRepositoryException, JsonProcessingException, SecurityException {
        if (HSecurityManager.get().isAuthenticated(token)) {
            final Map<String, ObjectNode> map = new HashMap<String, ObjectNode>();
            for (final String entityType_string : list) {
                MDConstants.checkNullParams("Entity Type and Auth token can't be NULL, EntityType: ".concat(entityType_string).concat(", Token: ").concat(token.toString()), entityType_string, token);
                final EntityType entityType = EntityType.valueOf(entityType_string);
                final ObjectNode entry = MetaInfo.getActions(entityType);
                map.put(entityType_string, entry);
            }
            return map;
        }
        throw new SecurityException("Unauthorized access to action handler");
    }
    
    public ObjectNode[] updateApplicationSettings(final AuthToken token, final UUID app_uuid, final ObjectNode data) throws Exception {
        final MetaInfo.Flow app_metaObject = (MetaInfo.Flow)this.metadataRepository.getMetaObjectByUUID(app_uuid, token);
        if (app_metaObject != null) {
            final List<Property> eHandlersList = Lists.newArrayList();
            final JsonNode eHandlers = data.get("eHandlers");
            if (eHandlers != null) {
                for (final ExceptionType exceptionType : ExceptionType.values()) {
                    final String exceptionName = exceptionType.name();
                    final JsonNode handlerValue = eHandlers.get(exceptionName);
                    if (handlerValue != null) {
                        eHandlersList.add(new Property(exceptionName, handlerValue.asText()));
                    }
                }
            }
            final ExceptionHandler exceptionHandler = new ExceptionHandler(eHandlersList);
            Boolean encrypt = false;
            if (data.get("encrypted") != null) {
                encrypt = data.get("encrypted").asBoolean();
            }
            final JsonNode recoveryType = data.get("recoveryType");
            final JsonNode recoveryPeriod = data.get("recoveryPeriod");
            int recovType = 0;
            long recovPeriod = 0L;
            if (recoveryType != null) {
                recovType = recoveryType.asInt();
            }
            if (recoveryPeriod != null) {
                recovPeriod = recoveryPeriod.asLong();
            }
            final JsonNode importStatements = data.get("importStatements");
            Set<String> importStatementsArray = null;
            if (importStatements != null) {
                assert importStatements.isArray();
                importStatementsArray = ClientCreateOperation.arrayNodeToSet(importStatements);
            }
            app_metaObject.setEncrypted(encrypt);
            app_metaObject.setImportStatements(importStatementsArray);
            app_metaObject.setRecoveryPeriod(recovPeriod);
            try {
                final Context temp_ctx = new Context(token);
                final JsonNode namespaceNode = data.get("nsName");
                final String namespace = (namespaceNode != null) ? namespaceNode.asText(" ") : null;
                if (namespace == null) {
                    throw new InvalidPropertiesFormatException("Namespace can't be null while updating app settings");
                }
                final Compiler temp_compiler = new Compiler(null, temp_ctx);
                temp_ctx.useNamespace(namespace);
                this.setUpdateMode(true);
                temp_ctx.setCurApp(app_metaObject);
                final Map<String, Object> ekseptionHandler = temp_compiler.combineProperties(exceptionHandler.props);
                app_metaObject.setEhandlers(ekseptionHandler);
                this.metadataRepository.updateMetaObject(app_metaObject, token);
            }
            catch (Exception e) {
                QueryValidator.logger.error((Object)"Unexpected error: ", (Throwable)e);
            }
            return this.clientOperations.formatResult(app_metaObject);
        }
        throw new MetaDataRepositoryException("No application found for ID: " + app_uuid);
    }
    
    public void LoadDataComponent(final AuthToken authToken, final MetaInfo.Flow application, final DeploymentRule deploymentRule) throws MetaDataRepositoryException {
        this.createOrGetContext(authToken);
        this.compile(authToken, AST.CreateDeployStmt(EntityType.APPLICATION, deploymentRule, new ArrayList<DeploymentRule>(), new ArrayList<Pair<String, String>>()));
        this.compile(authToken, AST.CreateStartStmt(application.getFullName(), EntityType.APPLICATION, null));
    }
    
    public void UnloadDataComponent(final AuthToken authToken, final Flow application, final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        this.createOrGetContext(authToken);
        this.compile(authToken, AST.CreateStopStmt(application.getMetaFullName(), EntityType.APPLICATION));
        this.compile(authToken, AST.CreateUndeployStmt(application.getMetaFullName(), EntityType.APPLICATION));
        this.metadataRepository.removeMetaObjectByUUID(application.getMetaID(), authToken);
        obj.getReverseIndexObjectDependencies().remove(application.getMetaID());
        this.metadataRepository.updateMetaObject(obj, authToken);
    }
    
    private Context createOrGetContext(final AuthToken authToken) {
        if (this.contexts.containsKey(authToken)) {
            return this.contexts.get(authToken);
        }
        return this.contexts.put(authToken, new Context(authToken));
    }
    
    public Map<String, Boolean> getAllowedPermissions(final AuthToken token, final String userid, final List<String> list) throws Exception {
        final Map<String, Boolean> map = new HashMap<String, Boolean>();
        final MetaInfo.User user = HSecurityManager.get().getUser(userid);
        if (!PermissionUtility.checkReadPermission(user, token, HSecurityManager.get())) {
            throw new SecurityException("Current user can't read :" + userid);
        }
        if (list == null || list.isEmpty()) {
            return map;
        }
        for (final String str : list) {
            final boolean bool = HSecurityManager.get().userAllowed(user, new ObjectPermission(str));
            map.put(str, new Boolean(bool));
        }
        return map;
    }
    
    public List<String> getPermissionObjectTypeList() {
        final List<String> vals = new ArrayList<String>();
        for (final ObjectPermission.ObjectType type : ObjectPermission.ObjectType.values()) {
            vals.add(type.name());
        }
        return vals;
    }
    
    public List<String> getPermissionActionList() {
        final List<String> vals = new ArrayList<String>();
        for (final ObjectPermission.Action type : ObjectPermission.Action.values()) {
            vals.add(type.name());
        }
        return vals;
    }
    
    public ArrayNode getMaskingTemplate() {
        return DataObfuscationLayer.getMaskingTemplate();
    }
    
    public List<String> getPermissionTypeList() {
        final List<String> vals = new ArrayList<String>();
        for (final ObjectPermission.PermissionType type : ObjectPermission.PermissionType.values()) {
            vals.add(type.name());
        }
        return vals;
    }
    
    public static boolean isReservedKeyword(final String text) {
        return Lexer.isKeyword(text);
    }
    
    public static boolean isValidName(final String name) throws IOException {
        final Lexer l = new Lexer(name, true);
        l.setAcceptNewLineInStringLiteral(false);
        boolean seenFirstIdentifier = false;
        boolean seenDot = false;
        boolean isValid = false;
        try {
            while (true) {
                final Symbol s = l.nextToken();
                if (s.sym == 227) {
                    if (isReservedKeyword(s.value.toString())) {
                        isValid = false;
                        break;
                    }
                    if (!seenFirstIdentifier) {
                        seenFirstIdentifier = true;
                    }
                    if (seenFirstIdentifier && seenDot) {
                        isValid = true;
                        break;
                    }
                    continue;
                }
                else {
                    if (s.sym != 205) {
                        isValid = false;
                        break;
                    }
                    if (!seenFirstIdentifier) {
                        isValid = false;
                        break;
                    }
                    seenDot = true;
                }
            }
        }
        catch (RuntimeException | IOException ex2) {
            isValid = false;
        }
        return l.nextToken().sym == 0 && isValid;
    }
    
    public void AlterStreamStatement(final AuthToken requestToken, final String objectName, final Boolean partitionEnable, final String[] streamPartitionFields, final Boolean persistEnable, final StreamPersistencePolicy spp) throws MetaDataRepositoryException {
        final Stmt stmt = AST.CreateAlterStmt(objectName, partitionEnable, Arrays.asList(streamPartitionFields), persistEnable, spp);
        if (QueryValidator.logger.isInfoEnabled()) {
            QueryValidator.logger.info((Object)("AlterStreamStatement is called - with arguments " + objectName + " , " + partitionEnable + " , " + Arrays.toString(streamPartitionFields) + " , " + persistEnable + " , " + spp));
        }
        this.compile(requestToken, stmt);
    }
    
    static {
        QueryValidator.logger = Logger.getLogger((Class)QueryValidator.class);
        typePat = Pattern.compile("(\\[+)?(L)?([\\w\\.\\_\\$]+)(\\s+[Kk][Ee][Yy])?");
        QueryValidator.SOURCE_SIDE_FILTERING_VALIDATION_ERROR = "Creating Source Failed, %s cannot be %s";
    }
}
