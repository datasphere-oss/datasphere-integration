package com.datasphere.runtime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

import com.datasphere.anno.NotSet;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.classloading.StriimClassLoader;
import com.datasphere.classloading.WALoader;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.ServerException;
import com.datasphere.exception.Warning;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.hazelcast.core.IMap;
import com.datasphere.kafka.KWCheckpoint;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.kinesis.KinesisOffset;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataDBOps;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.StatusDataStore;
import com.datasphere.persistence.KWCheckpointPersistenceLayer;
import com.datasphere.proc.BaseProcess;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.Stmt;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.upgrades.UpgradeController;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;

public class ServerUpgradeUtility
{
    private static MODE action;
    private static String jsonFileName;
    private MDRepository metaDataRepository;
    private static Logger logger;
    private static String UPGRADE_ERROR;
    private static Map<String, Exception> errors;
    static boolean needsRecompile;
    static boolean needsQueryCompile;
    List<String> flows;
    private Set<MetaInfo.MetaObject> moSet;
    static Context ctx;
    public static boolean isUpgrading;
    public static boolean dropFailedRecompile;
    public static boolean skipFailedRecompile;
    static String fromVersion;
    static String toVersion;
    
    public ServerUpgradeUtility() {
        this.flows = new CopyOnWriteArrayList<String>();
        if (ServerUpgradeUtility.needsRecompile) {
            this.loadPropertyTemplates();
        }
        ServerUpgradeUtility.isUpgrading = true;
    }
    
    protected MetaInfo.MetaObject getObject(final EntityType type, final String namespace, final String name) throws MetaDataRepositoryException {
        if (this.metaDataRepository != null) {
            return this.metaDataRepository.getMetaObjectByName(type, namespace, name, null, HSecurityManager.TOKEN);
        }
        return null;
    }
    
    public MetaInfo.MetaObject getObject(final UUID uuid) throws MetaDataRepositoryException {
        return this.metaDataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
    }
    
    public <T extends MetaInfo.MetaObject> T getObjectInfo(final UUID uuid, final EntityType type) throws ServerException, MetaDataRepositoryException {
        final MetaInfo.MetaObject o = this.getObject(uuid);
        if (o != null && o.type == type) {
            return (T)o;
        }
        throw new ServerException("cannot find metadata for " + type.name() + " " + uuid);
    }
    
    private void genDefaultClasses(final PropertyTemplate pt) throws MetaDataRepositoryException {
        final Map<String, String> fields = new LinkedHashMap<String, String>();
        final Class<?> inputType = pt.inputType();
        final Class<?> outputType = pt.outputType();
        MetaInfo.Type type = null;
        if (!inputType.equals(NotSet.class)) {
            final Field[] declaredFields;
            final Field[] cFields = declaredFields = inputType.getDeclaredFields();
            for (final Field f : declaredFields) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            if (this.getObject(EntityType.TYPE, "Global", inputType.getSimpleName()) == null) {
                type = new MetaInfo.Type();
                type.construct("Global." + inputType.getSimpleName(), MetaInfo.GlobalNamespace, inputType.getName(), fields, null, false);
            }
        }
        else {
            final Field[] declaredFields2;
            final Field[] cFields = declaredFields2 = outputType.getDeclaredFields();
            for (final Field f : declaredFields2) {
                if (Modifier.isPublic(f.getModifiers())) {
                    fields.put(f.getName(), f.getType().getCanonicalName());
                }
            }
            if (this.getObject(EntityType.TYPE, "Global", outputType.getSimpleName()) == null) {
                type = new MetaInfo.Type();
                type.construct("Global." + outputType.getSimpleName(), MetaInfo.GlobalNamespace, outputType.getName(), fields, null, false);
            }
        }
        if (type != null) {
            this.putObject(type);
        }
    }
    
    protected MetaInfo.MetaObject putObject(final MetaInfo.MetaObject obj) throws MetaDataRepositoryException {
        if (this.metaDataRepository != null) {
            this.metaDataRepository.putMetaObject(obj, HSecurityManager.TOKEN);
            return this.metaDataRepository.getMetaObjectByUUID(obj.getUuid(), HSecurityManager.TOKEN);
        }
        return null;
    }
    
    public void loadPropertyTemplates() {
        final String homeList = System.getProperty("com.datasphere.platform.home");
        String[] platformHome = new String[2];
        if (homeList != null && homeList.contains(";")) {
            platformHome = homeList.split(";");
        }
        else {
            platformHome[0] = homeList;
        }
        final Set<URL> result = Sets.newHashSet();
        for (final String aPlatformHome : platformHome) {
            File platformHomeFile;
            if (aPlatformHome == null) {
                platformHomeFile = new File(".");
            }
            else {
                platformHomeFile = new File(aPlatformHome);
            }
            String platformHomeAbs;
            try {
                platformHomeAbs = platformHomeFile.getCanonicalPath();
            }
            catch (IOException e2) {
                platformHomeAbs = platformHomeFile.getAbsolutePath();
            }
            final ClassLoader[] classLoaders;
            final ClassLoader[] loaders = classLoaders = ClasspathHelper.classLoaders(new ClassLoader[0]);
            for (ClassLoader classLoader : classLoaders) {
                while (classLoader != null) {
                    if (classLoader instanceof URLClassLoader) {
                        final URL[] urls = ((URLClassLoader)classLoader).getURLs();
                        if (urls != null) {
                            for (final URL url : urls) {
                                if (url.getPath().contains(platformHomeAbs)) {
                                    result.add(url);
                                }
                            }
                        }
                    }
                    classLoader = classLoader.getParent();
                }
            }
        }
        final Reflections refs = new Reflections(result.toArray());
        final Set<Class<?>> annotatedClasses = (Set<Class<?>>)refs.getTypesAnnotatedWith((Class)PropertyTemplate.class);
        for (final Class<?> c : annotatedClasses) {
            final PropertyTemplate pt = c.getAnnotation(PropertyTemplate.class);
            final PropertyTemplateProperty[] ptp = pt.properties();
            final Map<String, MetaInfo.PropertyDef> props = Factory.makeMap();
            for (final PropertyTemplateProperty val : ptp) {
                final MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
                def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
                props.put(val.name(), def);
            }
            final Lock propTemplateLock = (Lock)HazelcastSingleton.get().getLock("propTemplateLock");
            propTemplateLock.lock();
            try {
                this.genDefaultClasses(pt);
                MetaInfo.PropertyTemplateInfo pti = (MetaInfo.PropertyTemplateInfo)this.getObject(EntityType.PROPERTYTEMPLATE, "Global", pt.name());
                boolean isUnique = false;
                if (pti != null) {
                    final Map<String, MetaInfo.PropertyDef> defCurrent = pti.propertyMap;
                    for (final Map.Entry<String, MetaInfo.PropertyDef> entry : props.entrySet()) {
                        if (!entry.getValue().equals(defCurrent.get(entry.getKey()))) {
                            isUnique = true;
                            break;
                        }
                    }
                }
                else {
                    isUnique = true;
                }
                if (!isUnique) {
                    continue;
                }
                pti = new MetaInfo.PropertyTemplateInfo();
                pti.construct(pt.name(), pt.type(), props, pt.inputType().getName(), pt.outputType().getName(), c.getName(), pt.requiresParser(), pt.requiresFormatter(), pt.version(), pt.isParallelizable());
                this.putObject(pti);
            }
            catch (Exception e) {
                ServerUpgradeUtility.logger.error((Object)e.getLocalizedMessage());
            }
            finally {
                propTemplateLock.unlock();
            }
        }
    }
    
    private void initialize() throws MetaDataRepositoryException {
        BaseServer.setMetaDataDbProviderDetails();
        final NodeStartUp nsu = new NodeStartUp(true);
        final MetaInfo.Initializer ini = nsu.getInitializer();
        HazelcastSingleton.setDBDetailsForMetaDataRepository(ini.MetaDataRepositoryLocation, ini.MetaDataRepositoryDBname, ini.MetaDataRepositoryUname, ini.MetaDataRepositoryPass);
        printf("Getting a metadata repository instance and initializing.....\n");
        (this.metaDataRepository = MetadataRepository.getINSTANCE()).initialize();
        final IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
        final String clusterName = HazelcastSingleton.getClusterName();
        startUpMap.put(clusterName, ini);
    }
    
    private void upgradeMetaObjects() {
        printf("Starting upgrade process for meta objects.... \n");
        this.moSet = getMetaObjectsFromJson(ServerUpgradeUtility.jsonFileName);
        try {
            this.checkImportStmts();
            this.moSet = this.adapterFixes(this.moSet);
        }
        catch (Exception e) {
            ServerUpgradeUtility.logger.warn((Object)e);
        }
        if (ServerUpgradeUtility.errors.size() > 0) {
            printf(ServerUpgradeUtility.UPGRADE_ERROR = "Error importing json. Aborting upgrade process. Total errors :" + ServerUpgradeUtility.errors.size() + "\n");
            for (final String key : ServerUpgradeUtility.errors.keySet()) {
                printf("node[" + key + "] : " + ServerUpgradeUtility.errors.get(key).getMessage() + "\n");
            }
            printf("\n\nUpgrade process went bad. Look at above errors.\n\n");
        }
        else {
            printf("\n\nUpgrade process went OK without errors.\n\n");
            this.deleteMetadata();
            this.importMetadata(this.moSet);
            if (ServerUpgradeUtility.needsRecompile) {
                this.fixSubscription(this.moSet);
            }
        }
    }
    
    private void upgradeCheckpoints() {
        printf("Starting upgrade process for checkpoints.... \n");
        final String filename = "checkpoints-" + ServerUpgradeUtility.jsonFileName;
        final File f = new File(filename);
        if (!f.exists()) {
            System.out.println("No checkpoint data file (" + filename + ") found, so no checkpoints will be imported");
        }
        else {
            final String json = getJsonString(filename);
            if (json == null || json.isEmpty()) {
                System.out.println("No checkpoint data found, so no checkpoints will be imported");
            }
            else {
                final Set<Path> pathSet = getPathsFromJson(json);
                final Map<UUID, Set<Path>> uuidToPaths = new HashMap<UUID, Set<Path>>();
                for (final Path p : pathSet) {
                    Set<Path> pathsForUuid = uuidToPaths.get(p.applicationUuid);
                    if (pathsForUuid == null) {
                        pathsForUuid = new HashSet<Path>();
                        uuidToPaths.put(p.applicationUuid, pathsForUuid);
                    }
                    pathsForUuid.add(p);
                }
                final StringBuilder result = new StringBuilder("");
                final Map<UUID, Position> uuidToPosition = new HashMap<UUID, Position>();
                for (final Map.Entry<UUID, Set<Path>> entry : uuidToPaths.entrySet()) {
                    final boolean success = StatusDataStore.getInstance().putAppCheckpoint(entry.getKey(), new Position((Set)entry.getValue()));
                    if (success) {
                        result.append("\nFor application ").append(entry.getKey()).append(" checkpoint upgrade was SUCCESSFUL");
                    }
                    else {
                        result.append("\nFor application ").append(entry.getKey()).append(" checkpoint upgrade FAILED");
                    }
                }
                System.out.println("Writing checkpoint paths during upgrade resulted in: " + (Object)result);
            }
        }
    }
    
    private void upgradeKWCheckpoint() throws Exception {
        printf("Starting upgrade process for KWCheckpoint.... \n");
        final String filename = "KWcheckpoints-" + ServerUpgradeUtility.jsonFileName;
        final File f = new File(filename);
        if (!f.exists()) {
            System.out.println("No checkpoint data file (" + filename + ") found, so no checkpoints will be imported");
        }
        else {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode jsonNode = mapper.readTree(f);
            final String json = (jsonNode != null) ? jsonNode.toString() : null;
            if (json == null || json.isEmpty()) {
                System.out.println("No checkpoint data found, so no checkpoints will be imported");
            }
            else {
                final Iterator<JsonNode> jsonNodeIterator = (Iterator<JsonNode>)jsonNode.iterator();
                final List<KWCheckpoint> kwCheckpointsToBePersisted = new ArrayList<KWCheckpoint>();
                while (jsonNodeIterator.hasNext()) {
                    final JsonNode kafkaCheckpointNode = jsonNodeIterator.next();
                    final String checkpointKey = kafkaCheckpointNode.findValue("checkpointkey").textValue();
                    final JsonNode checkpointvalueNode = kafkaCheckpointNode.findValue("checkpointvalue");
                    final JsonNode pathsNode = checkpointvalueNode.findValue("Paths");
                    final Iterator<JsonNode> pathsIterator = (Iterator<JsonNode>)pathsNode.iterator();
                    final List<Path> pathsFromJson = new ArrayList<Path>();
                    while (pathsIterator.hasNext()) {
                        final Path path = Path.fromStandardJSON((JsonNode)pathsIterator.next());
                        pathsFromJson.add(path);
                    }
                    final PathManager pm = new PathManager((Collection)pathsFromJson);
                    Offset offset = null;
                    try {
                        offset = (Offset)new KafkaLongOffset(Long.parseLong(checkpointvalueNode.findValue("Offset").toString()));
                    }
                    catch (NumberFormatException e) {
                        offset = (Offset)new KinesisOffset(checkpointvalueNode.findValue("Offset").toString());
                    }
                    final OffsetPosition of = new OffsetPosition(pm.toPosition(), offset, -1L);
                    final byte[] checkpointdata = KryoSingleton.write(of, false);
                    final KWCheckpoint kwCheckPt = new KWCheckpoint(checkpointKey, checkpointdata);
                    kwCheckpointsToBePersisted.add(kwCheckPt);
                }
                if (!kwCheckpointsToBePersisted.isEmpty()) {
                    try {
                        final Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put("javax.persistence.jdbc.user", MetaDataDBOps.DBUname);
                        properties.put("javax.persistence.jdbc.password", MetaDataDBOps.DBPassword);
                        properties.put("javax.persistence.jdbc.url", BaseServer.getMetaDataDBProviderDetails().getJDBCURL(MetaDataDBOps.DBLocation, MetaDataDBOps.DBName, MetaDataDBOps.DBUname, MetaDataDBOps.DBPassword));
                        properties.put("javax.persistence.jdbc.driver", BaseServer.getMetaDataDBProviderDetails().getJDBCDriver());
                        properties.put("eclipselink.weaving", "STATIC");
                        properties.put("eclipselink.ddl-generation", "create-tables");
                        properties.put("eclipselink.ddl-generation.output-mode", "database");
                        final KWCheckpointPersistenceLayer jpaPersistanceLayer = new KWCheckpointPersistenceLayer("KafkaWriterCheckpoint", properties);
                        jpaPersistanceLayer.init();
                        jpaPersistanceLayer.putAllKWCheckpoint(kwCheckpointsToBePersisted);
                    }
                    catch (Exception e2) {
                        ServerUpgradeUtility.logger.error((Object)"Problem while persisting data back into KWCheckpoint table, this will affect E1P on restart.");
                    }
                }
            }
        }
    }
    
    private Set<MetaInfo.MetaObject> adapterFixes(final Set<MetaInfo.MetaObject> moSet) throws Exception {
        final Set<MetaInfo.MetaObject> result = new LinkedHashSet<MetaInfo.MetaObject>();
        for (final MetaInfo.MetaObject obj : moSet) {
            if (obj.type == EntityType.SOURCE) {
                final String adapterClassName = ((MetaInfo.Source)obj).adapterClassName;
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(adapterClassName);
                final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
                final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(proc.getClass().getClassLoader());
                final MetaInfo.MetaObject upgradedObject = proc.onUpgrade(obj, ServerUpgradeUtility.fromVersion, ServerUpgradeUtility.toVersion);
                Thread.currentThread().setContextClassLoader(origialCl);
                if (upgradedObject != null) {
                    result.add(upgradedObject);
                }
                else {
                    result.add(obj);
                }
            }
            else if (obj.type == EntityType.TARGET) {
                final String adapterClassName = ((MetaInfo.Target)obj).adapterClassName;
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(adapterClassName);
                final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
                final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(proc.getClass().getClassLoader());
                final MetaInfo.MetaObject upgradedObject = proc.onUpgrade(obj, ServerUpgradeUtility.fromVersion, ServerUpgradeUtility.toVersion);
                Thread.currentThread().setContextClassLoader(origialCl);
                if (upgradedObject != null) {
                    result.add(upgradedObject);
                }
                else {
                    result.add(obj);
                }
            }
            else if (obj.type == EntityType.CACHE) {
                final String adapterClassName = ((MetaInfo.Cache)obj).adapterClassName;
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(adapterClassName);
                final BaseProcess proc = (BaseProcess)adapterFactory.newInstance();
                final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(proc.getClass().getClassLoader());
                final MetaInfo.MetaObject upgradedObject = proc.onUpgrade(obj, ServerUpgradeUtility.fromVersion, ServerUpgradeUtility.toVersion);
                Thread.currentThread().setContextClassLoader(origialCl);
                if (upgradedObject != null) {
                    result.add(upgradedObject);
                }
                else {
                    result.add(obj);
                }
            }
            else {
                result.add(obj);
            }
        }
        return result;
    }
    
    public void checkImportStmts() throws Exception {
        if (this.moSet == null || this.moSet.isEmpty()) {
            return;
        }
        for (final MetaInfo.MetaObject obj : this.moSet) {
            if (obj.getType() == EntityType.APPLICATION) {
                final Set<String> importStmtsList = ((MetaInfo.Flow)obj).importStatements;
                if (ServerUpgradeUtility.logger.isDebugEnabled()) {
                    ServerUpgradeUtility.logger.debug((Object)importStmtsList);
                }
                for (final String imp : importStmtsList) {
                    try {
                        Compiler.compile(imp, ServerUpgradeUtility.ctx, new Compiler.ExecutionCallback() {
                            @Override
                            public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                                try {
                                    compiler.compileStmt(stmt);
                                }
                                catch (Warning e) {
                                    ServerUpgradeUtility.logger.warn((Object)(">>>>" + e.getMessage()));
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        ServerUpgradeUtility.logger.warn((Object)(">>" + e.getMessage()));
                    }
                }
            }
        }
    }
    
    public static String convertNewQueries(final String selectQuery) {
        Pattern stringParamMatch = Pattern.compile("(\")(:)(\\w+)(\")");
        Matcher matcher = stringParamMatch.matcher(selectQuery);
        String tempSelectQuery = selectQuery;
        while (matcher.find()) {
            tempSelectQuery = tempSelectQuery.replaceAll(matcher.group(), matcher.group(2) + matcher.group(3));
        }
        stringParamMatch = Pattern.compile("(')(:)(\\w+)(')");
        matcher = stringParamMatch.matcher(selectQuery);
        while (matcher.find()) {
            tempSelectQuery = tempSelectQuery.replaceAll(matcher.group(), matcher.group(2) + matcher.group(3));
        }
        return tempSelectQuery;
    }
    
    private void fixQueries() {
        if (this.moSet == null) {
            return;
        }
        for (final MetaInfo.MetaObject mo : this.moSet) {
            try {
                if (mo.type != EntityType.QUERY) {
                    continue;
                }
                final MetaInfo.Query query = (MetaInfo.Query)mo;
                ServerUpgradeUtility.ctx.useNamespace(mo.nsName);
                if (this.metaDataRepository.getMetaObjectByName(EntityType.QUERY, ServerUpgradeUtility.ctx.getCurNamespace().getName(), query.name, null, HSecurityManager.TOKEN) != null) {
                    continue;
                }
                if (query.projectionFields.isEmpty()) {
                    query.getMetaInfoStatus().setValid(false);
                    this.metaDataRepository.updateMetaObject(query, HSecurityManager.TOKEN);
                }
                else {
                    Compiler.compile("CREATE NAMEDQUERY " + query.name + " " + convertNewQueries(query.queryDefinition), ServerUpgradeUtility.ctx, new Compiler.ExecutionCallback() {
                        @Override
                        public void execute(final Stmt stmt, final Compiler compiler) throws MetaDataRepositoryException {
                            try {
                                compiler.compileStmt(stmt);
                                ServerUpgradeUtility.this.flows.add(Compiler.NAMED_QUERY_PREFIX + query.name);
                            }
                            catch (Warning e) {
                                System.out.println("Exception while compiling " + stmt);
                            }
                        }
                    });
                }
            }
            catch (Exception e2) {
                try {
                    final MetaInfo.Query query2 = (MetaInfo.Query)mo;
                    if (query2 != null) {
                        query2.getMetaInfoStatus().setValid(false);
                        this.metaDataRepository.putMetaObject(query2, HSecurityManager.TOKEN);
                    }
                }
                catch (MetaDataRepositoryException e1) {
                    if (ServerUpgradeUtility.logger.isDebugEnabled()) {
                        ServerUpgradeUtility.logger.debug((Object)e1.getMessage());
                    }
                }
                if (!ServerUpgradeUtility.logger.isDebugEnabled()) {
                    continue;
                }
                ServerUpgradeUtility.logger.debug((Object)e2.getMessage());
            }
        }
    }
    
    private void fixSubscription(final Set<MetaInfo.MetaObject> moSet) {
        for (final MetaInfo.MetaObject mo : moSet) {
            try {
                if (mo.type != EntityType.TARGET || !((MetaInfo.Target)mo).isSubscription()) {
                    continue;
                }
                final Class<?> adapterFactory = ClassLoader.getSystemClassLoader().loadClass(((MetaInfo.Target)mo).adapterClassName);
                final PropertyTemplate pt = adapterFactory.getAnnotation(PropertyTemplate.class);
                final MetaInfo.MetaObject input = this.metaDataRepository.getMetaObjectByUUID(((MetaInfo.Target)mo).getInputStream(), HSecurityManager.TOKEN);
                mo.setSourceText("CREATE SUBSCRIPTION " + mo.getName() + " USING " + pt.name() + " ( " + Utility.prettyPrintMap(this.makePropList(new HashMap[] { (HashMap)((MetaInfo.Target)mo).properties })) + " ) INPUT FROM " + input.getName() + "");
                this.metaDataRepository.updateMetaObject(mo, HSecurityManager.TOKEN);
            }
            catch (Exception e) {
                ServerUpgradeUtility.logger.warn((Object)e.getMessage());
            }
        }
    }
    
    private void deleteMetadata() {
        this.metaDataRepository.clear(true);
    }
    
    public static String getJsonString(final String filename) {
        String json = null;
        try {
            final BufferedReader br = new BufferedReader(new FileReader(filename));
            json = "";
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                json += sCurrentLine;
            }
            br.close();
        }
        catch (IOException iox) {
            ServerUpgradeUtility.logger.error((Object)("error reading file : " + filename), (Throwable)iox);
            ServerUpgradeUtility.errors.put("error reading file : " + filename, iox);
            printf("error reading file : " + filename);
        }
        return json;
    }
    
    private List<Property> makePropList(final Map<String, String>[] props) {
        if (props == null) {
            return null;
        }
        final List<Property> plist = new ArrayList<Property>();
        for (final Map<String, String> prop : props) {
            assert prop.size() == 1;
            for (final Map.Entry<String, String> e : prop.entrySet()) {
                final Property p = new Property(e.getKey(), e.getValue());
                plist.add(p);
            }
        }
        return plist;
    }
    
    private static Set<Path> getPathsFromJson(final String json) {
        final Set<Path> result = new HashSet<Path>();
        final String pattern = "\\{.*?flowUuid.*?\\}";
        final Pattern r = Pattern.compile(pattern);
        final Matcher m = r.matcher(json);
        try {
            while (m.find()) {
                final String pathJson = m.group(0);
                final Path p = Path.fromJson(pathJson);
                if (p == null) {
                    System.out.println("Found an unexpected null path...");
                }
                else {
                    result.add(p);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Upgrade Utility found this many checkpoint paths to transfer: " + result.size());
        return result;
    }
    
    private static Set<MetaInfo.MetaObject> getMetaObjectsFromJson(final String jsonFileName) {
        Set<MetaInfo.MetaObject> moSet = null;
        int nodecntr = 1;
        ServerUpgradeUtility.errors.clear();
        try {
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            final JsonNode listNode = jsonMapper.readTree(new File(jsonFileName));
            if (!listNode.isContainerNode()) {
                ServerUpgradeUtility.logger.warn((Object)"JSON MetaData is not in list format");
                return null;
            }
            if (ServerUpgradeUtility.logger.isInfoEnabled()) {
                ServerUpgradeUtility.logger.info((Object)("Number of objects : " + listNode.size()));
            }
            moSet = new HashSet<MetaInfo.MetaObject>();
            final Iterator<JsonNode> it = (Iterator<JsonNode>)listNode.elements();
            while (it.hasNext()) {
                final JsonNode moNode = it.next();
                final String className = moNode.get("metaObjectClass").asText();
                ++nodecntr;
                if (ServerUpgradeUtility.logger.isDebugEnabled()) {
                    ServerUpgradeUtility.logger.debug((Object)(nodecntr + " : Building MetaInfo object : " + className));
                }
                try {
                    MetaInfo.MetaObject mo;
                    boolean addFlag;
                    if (className.equalsIgnoreCase(MetaInfo.Query.class.getName())) {
                        mo = MetaInfo.Query.deserialize(moNode);
                        addFlag = true;
                    }
                    else if (className.equalsIgnoreCase(MetaInfo.Role.class.getName())) {
                        mo = MetaInfo.Role.deserialize(moNode);
                        addFlag = true;
                    }
                    else if (className.equalsIgnoreCase(MetaInfo.User.class.getName())) {
                        mo = MetaInfo.User.deserialize(moNode);
                        addFlag = true;
                    }
                    else {
                        final String moNodeText = moNode.toString();
                        final Class<?> moClass = Class.forName(className, false, ClassLoader.getSystemClassLoader());
                        mo = (MetaInfo.MetaObject)jsonMapper.readValue(moNodeText, (Class)moClass);
                        if (mo instanceof MetaInfo.Source) {
                            final Map properties = ((MetaInfo.Source)mo).getProperties();
                            final Map caseInsensitiveMap = Factory.makeCaseInsensitiveMap();
                            caseInsensitiveMap.putAll(properties);
                            final Map parserProperties = ((MetaInfo.Source)mo).getParserProperties();
                            final Map caseInsensitiveParserPropertiesMap = Factory.makeCaseInsensitiveMap();
                            caseInsensitiveParserPropertiesMap.putAll(parserProperties);
                            ((MetaInfo.Source)mo).setProperties(caseInsensitiveMap);
                            ((MetaInfo.Source)mo).setParserProperties(caseInsensitiveParserPropertiesMap);
                        }
                        addFlag = true;
                    }
                    if (!addFlag) {
                        continue;
                    }
                    if (mo == null) {
                        ServerUpgradeUtility.errors.put(Integer.toString(nodecntr), new Exception("Building " + className + " from below JSON failed:\n" + moNode.asText()));
                    }
                    else {
                        moSet.add(mo);
                    }
                }
                catch (Exception e) {
                    ServerUpgradeUtility.logger.warn((Object)(nodecntr + " : Error building MetaInfo object : " + className), (Throwable)e);
                    ServerUpgradeUtility.errors.put(Integer.toString(nodecntr), e);
                }
            }
        }
        catch (Exception e2) {
            ServerUpgradeUtility.logger.error((Object)"error creating metadata objects from json string.", (Throwable)e2);
        }
        return moSet;
    }
    
    public static String exportMetadataAsJson(final MetaInfo.MetaObject metaObject) {
        try {
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            final String json = jsonMapper.writeValueAsString((Object)metaObject);
            return json;
        }
        catch (IOException e) {
            ServerUpgradeUtility.logger.error((Object)e);
            return null;
        }
    }
    
    private void importMetadata(final Set<MetaInfo.MetaObject> moSet) {
        if (ServerUpgradeUtility.logger.isInfoEnabled()) {
            ServerUpgradeUtility.logger.info((Object)"Started inserting MetaData to database.");
        }
        for (final MetaInfo.MetaObject mo : moSet) {
            if (mo instanceof MetaInfo.Namespace) {
                try {
                    this.metaDataRepository.putMetaObject(mo, HSecurityManager.TOKEN);
                }
                catch (MetaDataRepositoryException e) {
                    e.printStackTrace();
                }
            }
        }
        for (final MetaInfo.MetaObject mo : moSet) {
            if ((mo.getName().startsWith(Compiler.NAMED_QUERY_PREFIX) || mo.type == EntityType.QUERY) && ServerUpgradeUtility.needsQueryCompile) {
                continue;
            }
            if (mo instanceof MetaInfo.User) {
                final String passwordBefore = ((MetaInfo.User)mo).getEncryptedPassword();
                if (passwordBefore != null) {
                    ((MetaInfo.User)mo).setEncryptedPassword(((MetaInfo.User)mo).getEncryptedPassword().trim());
                    final String passwordAfter = ((MetaInfo.User)mo).getEncryptedPassword();
                    if (!passwordBefore.equals(passwordAfter) && ServerUpgradeUtility.logger.isDebugEnabled()) {
                        ServerUpgradeUtility.logger.debug((Object)("Password has escape characters, password is changed from " + StringEscapeUtils.escapeJava(passwordBefore) + " to " + StringEscapeUtils.escapeJava(passwordAfter)));
                    }
                }
            }
            try {
                if (mo.getName().startsWith(Compiler.NAMED_QUERY_PREFIX) || mo.type == EntityType.QUERY) {
                    continue;
                }
                this.metaDataRepository.putMetaObject(mo, HSecurityManager.TOKEN);
                if (mo.type != EntityType.TYPE || !ServerUpgradeUtility.needsQueryCompile || mo.getMetaInfoStatus().isDropped()) {
                    continue;
                }
                ((MetaInfo.Type)mo).generateClass();
            }
            catch (Exception e2) {
                ServerUpgradeUtility.logger.error((Object)("error inserting metadata object: " + mo.metaObjectClass), (Throwable)e2);
            }
        }
        System.out.println("Done inserting all metadata objects.");
    }
    
    private void exportMetaData() throws MetaDataRepositoryException {
        final String json = this.metaDataRepository.exportMetadataAsJson();
        if (json == null || json.isEmpty()) {
            ServerUpgradeUtility.logger.warn((Object)"Exporting metadata to json failed.");
            return;
        }
        final File f = new File(ServerUpgradeUtility.jsonFileName);
        try {
            final FileWriter fw = new FileWriter(f);
            fw.write(json);
            fw.close();
        }
        catch (IOException e) {
            ServerUpgradeUtility.logger.error((Object)"error writing json string to file", (Throwable)e);
        }
    }
    
    private void exportCheckpoints() throws MetaDataRepositoryException {
        final String json = StatusDataStore.getInstance().getAllAppCheckpointData();
        if (json == null || json.isEmpty()) {
            System.out.println("No Checkpoint data to export");
            return;
        }
        final File f = new File("checkpoints-" + ServerUpgradeUtility.jsonFileName);
        try {
            final FileWriter fw = new FileWriter(f);
            fw.write(json);
            fw.close();
        }
        catch (IOException e) {
            ServerUpgradeUtility.logger.error((Object)"error writing json string to file", (Throwable)e);
        }
    }
    
    private void exportKWCheckpointTable() {
        String json = null;
        try {
            KWCheckpointPersistenceLayer checkpointPersistenceLayer = null;
            try {
                final Map<String, Object> properties = new HashMap<String, Object>();
                properties.put("javax.persistence.jdbc.user", MetaDataDBOps.DBUname);
                properties.put("javax.persistence.jdbc.password", MetaDataDBOps.DBPassword);
                properties.put("javax.persistence.jdbc.url", BaseServer.getMetaDataDBProviderDetails().getJDBCURL(MetaDataDBOps.DBLocation, MetaDataDBOps.DBName, MetaDataDBOps.DBUname, MetaDataDBOps.DBPassword));
                properties.put("javax.persistence.jdbc.driver", BaseServer.getMetaDataDBProviderDetails().getJDBCDriver());
                properties.put("eclipselink.weaving", "STATIC");
                properties.put("eclipselink.ddl-generation", "create-tables");
                properties.put("eclipselink.ddl-generation.output-mode", "database");
                checkpointPersistenceLayer = new KWCheckpointPersistenceLayer("KafkaWriterCheckpoint", properties);
                checkpointPersistenceLayer.init();
                json = checkpointPersistenceLayer.getAllKWCheckpointData();
            }
            catch (Exception e) {
                throw e;
            }
            finally {
                if (checkpointPersistenceLayer != null) {
                    checkpointPersistenceLayer.close();
                    checkpointPersistenceLayer = null;
                }
            }
        }
        catch (Exception e2) {
            ServerUpgradeUtility.logger.error((Object)("Problem while exporting KWCheckpoint data to KWcheckpoints-" + ServerUpgradeUtility.jsonFileName + "." + e2));
        }
        if (json == null || json.isEmpty()) {
            System.out.println("No Checkpoint data to export");
            return;
        }
        final File f = new File("KWcheckpoints-" + ServerUpgradeUtility.jsonFileName);
        try {
            final FileWriter fw = new FileWriter(f);
            fw.write(json);
            fw.close();
        }
        catch (IOException e3) {
            ServerUpgradeUtility.logger.error((Object)"error writing json string to file", (Throwable)e3);
        }
    }
    
    private static void printf(final String toPrint) {
        try {
            if (System.console() != null) {
                System.console().printf(toPrint, new Object[0]);
            }
            else {
                System.out.printf(toPrint, new Object[0]);
            }
        }
        catch (Exception ex) {
            System.out.println("Exception while printing " + toPrint);
        }
        System.out.flush();
    }
    
    private static void EXTRACTINPUTS(final String[] args) {
        if (args == null || args.length < 2) {
            printf("\n>>> missing input parameters '<import / export>   <filename>'\n");
            System.exit(0);
        }
        if (args[0].equalsIgnoreCase("import")) {
            ServerUpgradeUtility.action = MODE.IMPORT;
        }
        else if (args[0].equalsIgnoreCase("export")) {
            ServerUpgradeUtility.action = MODE.EXPORT;
        }
        else {
            printf("\n>>> missing input parameters '<import / export>   <filename>'\n");
            System.exit(0);
        }
        ServerUpgradeUtility.jsonFileName = args[1];
        if (args.length > 2 && args[2].equalsIgnoreCase("yes")) {
            ServerUpgradeUtility.needsRecompile = true;
        }
        if (args.length > 3 && args[3].equalsIgnoreCase("yes")) {
            ServerUpgradeUtility.needsQueryCompile = true;
        }
        if (args.length > 4 && args[3].equalsIgnoreCase("yes")) {
            ServerUpgradeUtility.skipFailedRecompile = true;
        }
        if (args.length > 5 && args[4].equalsIgnoreCase("no")) {
            ServerUpgradeUtility.skipFailedRecompile = false;
        }
        if (args.length > 6 && args[5].equalsIgnoreCase("no")) {
            ServerUpgradeUtility.dropFailedRecompile = false;
        }
        if (args.length > 7) {
            ServerUpgradeUtility.fromVersion = args[6];
        }
        if (args.length > 8) {
            ServerUpgradeUtility.toVersion = args[6];
        }
    }
    
    public static boolean testCompatibility(String jsonFile) throws Exception {
        if (jsonFile == null || jsonFile.isEmpty()) {
            final String json = MetadataRepository.getINSTANCE().exportMetadataAsJson();
            jsonFile = new String("tmpMDR.json");
            final File f = new File(jsonFile);
            try {
                final FileWriter fw = new FileWriter(f);
                fw.write(json);
                fw.close();
            }
            catch (IOException e) {
                ServerUpgradeUtility.logger.error((Object)"error writing json string to file", (Throwable)e);
            }
        }
        final Set<MetaInfo.MetaObject> moSet = getMetaObjectsFromJson(jsonFile);
        if (ServerUpgradeUtility.errors.size() > 0) {
            printf(ServerUpgradeUtility.UPGRADE_ERROR = "Error importing json. Aborting upgrade process. Total errors :" + ServerUpgradeUtility.errors.size() + "\n");
            for (final String key : ServerUpgradeUtility.errors.keySet()) {
                printf("node[" + key + "] : " + ServerUpgradeUtility.errors.get(key).getMessage() + "\n");
            }
            ServerUpgradeUtility.logger.warn((Object)"UPGRADE_ERROR");
            return false;
        }
        printf("no of meta obejcts imported :" + moSet.size() + "\n");
        ServerUpgradeUtility.logger.warn((Object)"basic compatibility test passed.");
        printf("basic compatibility test passed. \n");
        return true;
    }
    
    void updateSourceTextForMetaInfo(final MetaInfo.MetaObject target, final String adapterClassName, final UUID streamUUID, final Map formatterOrParserProp, final Map properties) throws Exception {
        Class<?> parserCls = null;
        PropertyTemplate adapterAnno = null;
        PropertyTemplate parserAnno = null;
        try {
            final Class<?> adapterCls = Class.forName(adapterClassName, false, ClassLoader.getSystemClassLoader());
            if (target != null && formatterOrParserProp != null && formatterOrParserProp.get("handler") != null) {
                parserCls = Class.forName((String)formatterOrParserProp.get("handler"), false, ClassLoader.getSystemClassLoader());
            }
            adapterAnno = adapterCls.getAnnotation(PropertyTemplate.class);
            if (parserCls != null) {
                parserAnno = parserCls.getAnnotation(PropertyTemplate.class);
            }
        }
        catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        final MetaInfo.Stream stream = (MetaInfo.Stream)this.metaDataRepository.getMetaObjectByUUID(streamUUID, HSecurityManager.TOKEN);
        if (target.type == EntityType.SOURCE) {
            target.setSourceText(Utility.createSourceStatementText(target.getName(), false, adapterAnno.name(), Objects.equals(adapterAnno.version(), "0.0.0") ? null : adapterAnno.version(), this.makePropList(new HashMap[] { (HashMap)properties }), (parserAnno == null) ? null : parserAnno.name(), null, (formatterOrParserProp == null) ? null : this.makePropList(new HashMap[] { (HashMap)formatterOrParserProp }), stream.getName(), null));
        }
        if (target.type == EntityType.TARGET) {
            target.setSourceText(Utility.createTargetStatementText(target.getName(), false, adapterAnno.name(), Objects.equals(adapterAnno.version(), "0.0.0") ? null : adapterAnno.version(), this.makePropList(new HashMap[] { (HashMap)properties }), (parserAnno == null) ? null : parserAnno.name(), null, (formatterOrParserProp == null) ? null : this.makePropList(new HashMap[] { (HashMap)formatterOrParserProp }), stream.getName()));
        }
        if (ServerUpgradeUtility.logger.isDebugEnabled()) {
            ServerUpgradeUtility.logger.debug((Object)target.getSourceText());
        }
        this.metaDataRepository.updateMetaObject(target, HSecurityManager.TOKEN);
    }
    
    public static void main(final String[] args) throws Exception {
        System.out.println("Server upgrade utility started....");
        EXTRACTINPUTS(args);
        if (ServerUpgradeUtility.action.equals(MODE.IMPORT)) {
            final File jFile = new File(ServerUpgradeUtility.jsonFileName);
            if (!jFile.isFile()) {
                System.exit(-1);
            }
        }
        ServerUpgradeUtility.ctx = Context.createContext(HSecurityManager.TOKEN);
        final ServerUpgradeUtility suu = new ServerUpgradeUtility();
        suu.initialize();
        final StriimClassLoader scl = (StriimClassLoader)Thread.currentThread().getContextClassLoader();
        try {
            scl.scanModulePath();
        }
        catch (IOException ioe) {
            ServerUpgradeUtility.logger.warn((Object)("striim.modules.path is not set correctly, reason: " + ioe.getMessage()), (Throwable)ioe);
        }
        WALoader.get();
        if (ServerUpgradeUtility.action.equals(MODE.EXPORT)) {
            suu.exportMetaData();
            suu.exportCheckpoints();
            suu.exportKWCheckpointTable();
            System.exit(1);
        }
        else if (ServerUpgradeUtility.action.equals(MODE.IMPORT)) {
            suu.upgradeKWCheckpoint();
            suu.upgradeMetaObjects();
            suu.upgradeCheckpoints();
            printf("\n\nDone upgrading. Start server.");
            System.out.println("Unexpected, unsupported action type " + ServerUpgradeUtility.action);
        }
        if (ServerUpgradeUtility.UPGRADE_ERROR != null) {
            printf(">>>> " + ServerUpgradeUtility.UPGRADE_ERROR);
            System.exit(-1);
        }
        ServerUpgradeUtility.logger.warn((Object)("Done " + ServerUpgradeUtility.action + "ing. Now setting recompile flag"));
        try {
            if (ServerUpgradeUtility.needsRecompile) {
                final Set<MetaInfo.Flow> flows = (Set<MetaInfo.Flow>)suu.metaDataRepository.getByEntityType(EntityType.APPLICATION, HSecurityManager.TOKEN);
                final Set<MetaInfo.Flow> nestedFlows = (Set<MetaInfo.Flow>)suu.metaDataRepository.getByEntityType(EntityType.FLOW, HSecurityManager.TOKEN);
                for (final MetaInfo.Flow flow : nestedFlows) {
                    resetFlowDeploymentStrategy(flow);
                }
                final Set<MetaInfo.Flow> allFlows = new HashSet<MetaInfo.Flow>();
                if (flows != null) {
                    allFlows.addAll(flows);
                }
                if (nestedFlows != null) {
                    allFlows.addAll(nestedFlows);
                }
                for (final MetaInfo.Flow flow2 : allFlows) {
                    if (suu.flows.contains(flow2.getName())) {
                        continue;
                    }
                    resetFlowDeploymentStrategy(flow2);
                    final List<UUID> allObjects = new ArrayList<UUID>();
                    allObjects.add(flow2.getUuid());
                    try {
                        if (flow2.getMetaInfoStatus().isDropped()) {
                            continue;
                        }
                        flow2.getMetaInfoStatus().setValid(false);
                        suu.metaDataRepository.updateMetaObject(flow2, HSecurityManager.TOKEN);
                        final LinkedHashSet<UUID> cqs = flow2.getObjects().get(EntityType.CQ);
                        final LinkedHashSet<UUID> sources = flow2.getObjects().get(EntityType.SOURCE);
                        if (cqs != null) {
                            allObjects.addAll(cqs);
                        }
                        if (sources != null) {
                            allObjects.addAll(sources);
                        }
                        for (final UUID uuid : allObjects) {
                            final MetaInfo.MetaObject obj = suu.metaDataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                            if (obj != null) {
                                obj.getMetaInfoStatus().setValid(false);
                                suu.metaDataRepository.updateMetaObject(obj, HSecurityManager.TOKEN);
                            }
                        }
                    }
                    catch (Throwable e) {
                        if (!ServerUpgradeUtility.logger.isInfoEnabled()) {
                            continue;
                        }
                        ServerUpgradeUtility.logger.info((Object)e.getMessage(), e);
                    }
                }
                for (final MetaInfo.Flow flow2 : flows) {
                    try {
                        if (ServerUpgradeUtility.logger.isInfoEnabled()) {
                            ServerUpgradeUtility.logger.info((Object)("Trying to recompile the application " + flow2.getFullName() + ""));
                        }
                        ServerUpgradeUtility.ctx.useNamespace(flow2.nsName);
                        final Set<String> tempList = flow2.importStatements;
                        ServerUpgradeUtility.ctx.alterAppOrFlow(EntityType.APPLICATION, flow2.getFullName(), true);
                        final MetaInfo.Flow currApp = ServerUpgradeUtility.ctx.getFlowInCurSchema(flow2.getFullName(), EntityType.APPLICATION);
                        currApp.importStatements.addAll(tempList);
                        ServerUpgradeUtility.ctx.put(currApp);
                    }
                    catch (Throwable e2) {
                        if (!ServerUpgradeUtility.logger.isInfoEnabled()) {
                            continue;
                        }
                        ServerUpgradeUtility.logger.info((Object)("Application " + flow2.getFullName() + " failed to recompile"));
                        ServerUpgradeUtility.logger.info((Object)e2.getMessage(), e2);
                    }
                }
            }
            if (ServerUpgradeUtility.needsQueryCompile) {
                suu.fixQueries();
            }
            UpgradeController.upgrade(System.getProperty("com.datasphere.config.upgradeFrom"), System.getProperty("com.datasphere.config.upgradeTo"));
        }
        catch (Exception ex) {
            ServerUpgradeUtility.logger.error((Object)"error setting recompile flag", (Throwable)ex);
            System.exit(ServerUpgradeUtility.errors.size());
        }
        System.exit(0);
    }
    
    private static void resetFlowDeploymentStrategy(final MetaInfo.Flow flow) throws MetaDataRepositoryException {
        if (flow == null) {
            return;
        }
        if (flow.getFlowStatus() != MetaInfo.StatusInfo.Status.CREATED || flow.getDeploymentPlan() != null) {
            flow.setFlowStatus(MetaInfo.StatusInfo.Status.CREATED);
            flow.setDeploymentPlan(null);
            MetadataRepository.getINSTANCE().updateMetaObject(flow, HSecurityManager.TOKEN);
        }
    }
    
    static {
        ServerUpgradeUtility.action = MODE.UNKNOWN;
        ServerUpgradeUtility.jsonFileName = "mcd.json";
        ServerUpgradeUtility.logger = Logger.getLogger((Class)ServerUpgradeUtility.class);
        ServerUpgradeUtility.UPGRADE_ERROR = null;
        ServerUpgradeUtility.errors = new TreeMap<String, Exception>(String.CASE_INSENSITIVE_ORDER);
        ServerUpgradeUtility.needsRecompile = false;
        ServerUpgradeUtility.needsQueryCompile = false;
        ServerUpgradeUtility.ctx = null;
        ServerUpgradeUtility.dropFailedRecompile = true;
        ServerUpgradeUtility.skipFailedRecompile = true;
        ServerUpgradeUtility.fromVersion = "0.0.0";
        ServerUpgradeUtility.toVersion = "0.0.0";
    }
    
    private enum MODE
    {
        UNKNOWN, 
        EXPORT, 
        IMPORT;
    }
    
    public static class DashboardConverter extends MetaInfo.MetaObject
    {
        static ObjectMapper jsonMapper;
        static HashMap<String, LegacyQueryVisualization> oldQVs;
        
        public static List<MetaInfo.MetaObject> convert(final List<JsonNode> qvObjects, final List<JsonNode> pageObjects) throws IOException {
            (DashboardConverter.jsonMapper = ObjectMapperFactory.newInstance()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            DashboardConverter.oldQVs = new HashMap<String, LegacyQueryVisualization>();
            final List<MetaInfo.MetaObject> dashboardMetaObjects = new ArrayList<MetaInfo.MetaObject>();
            for (final JsonNode qvJson : qvObjects) {
                final List<MetaInfo.QueryVisualization> newQVs = convertCompositeQV(qvJson);
                dashboardMetaObjects.addAll(newQVs);
            }
            for (final JsonNode pageJson : pageObjects) {
                final MetaInfo.Page newPage = convertPage(pageJson);
                dashboardMetaObjects.add(newPage);
            }
            return dashboardMetaObjects;
        }
        
        private static List<MetaInfo.QueryVisualization> convertCompositeQV(final JsonNode jsonNode) throws IOException {
            final List<MetaInfo.QueryVisualization> newQVs = new ArrayList<MetaInfo.QueryVisualization>();
            final LegacyQueryVisualization oldQV = (LegacyQueryVisualization)DashboardConverter.jsonMapper.readValue(jsonNode.toString(), (Class)LegacyQueryVisualization.class);
            DashboardConverter.oldQVs.put(oldQV.getName(), oldQV);
            if (oldQV != null && oldQV.dataVisualizations != null && oldQV.dataVisualizations.size() > 0) {
                ServerUpgradeUtility.logger.debug((Object)("Creating " + oldQV.dataVisualizations.size() + " single-panel Visualizations out of " + oldQV.name + ""));
                for (final String dvJsonString : oldQV.dataVisualizations) {
                    final ObjectNode newQVJSON = (ObjectNode)DashboardConverter.jsonMapper.readTree(jsonNode.toString());
                    final JsonNode dvJson = DashboardConverter.jsonMapper.readTree(dvJsonString);
                    newQVJSON.put("name", dvJson.get("id").asText());
                    newQVJSON.put("uri", jsonNode.get("uri").asText());
                    newQVJSON.put("visualizationType", dvJson.get("type").asText());
                    newQVJSON.put("config", dvJson.get("config").toString());
                    final MetaInfo.QueryVisualization newQV = jsonToQV((JsonNode)newQVJSON);
                    newQVs.add(newQV);
                }
            }
            else {
                ServerUpgradeUtility.logger.warn((Object)("Warning: " + oldQV.name + " had no visualizations. Cannot determine Visualization type."));
            }
            return newQVs;
        }
        
        private static MetaInfo.QueryVisualization jsonToQV(final JsonNode newQvJson) throws IOException {
            final MetaInfo.QueryVisualization qv = (MetaInfo.QueryVisualization)DashboardConverter.jsonMapper.readValue(newQvJson.toString(), (Class)MetaInfo.QueryVisualization.class);
            final MetaInfo.QueryVisualization newQV = new MetaInfo.QueryVisualization(qv.getName(), null, qv.getNsName(), qv.getNamespaceId());
            newQV.setVisualizationType(qv.getVisualizationType());
            newQV.setUri(qv.getUri());
            newQV.setConfig(qv.getConfig());
            newQV.setQuery(qv.getQuery());
            newQV.setTitle(qv.getTitle());
            return newQV;
        }
        
        private static MetaInfo.Page convertPage(final JsonNode pageJson) throws IOException {
            final MetaInfo.Page page = (MetaInfo.Page)DashboardConverter.jsonMapper.readValue(pageJson.toString(), (Class)MetaInfo.Page.class);
            ServerUpgradeUtility.logger.debug((Object)("\nConverting Page " + page.getTitle() + " with " + page.getQueryVisualizations().size() + " original QVs..."));
            convertPageQVArray(page);
            convertPageGrid(page);
            return page;
        }
        
        private static void convertPageQVArray(final MetaInfo.Page metaObject) throws IOException {
            final List<String> oldPageQVNames = metaObject.getQueryVisualizations();
            final List<String> newPageQVNames = new ArrayList<String>();
            for (final String oldQVName : oldPageQVNames) {
                final LegacyQueryVisualization oldQV = DashboardConverter.oldQVs.get(oldQVName);
                if (oldQV != null && oldQV.dataVisualizations != null) {
                    for (final String dvJson : oldQV.dataVisualizations) {
                        final ObjectNode dv = (ObjectNode)DashboardConverter.jsonMapper.readTree(dvJson);
                        final String dvName = dv.get("id").asText();
                        newPageQVNames.add(dvName);
                    }
                }
                metaObject.setQueryVisualizations(newPageQVNames);
            }
        }
        
        private static void convertPageGrid(final MetaInfo.Page metaObject) throws IOException {
            final ObjectNode gridJson = (ObjectNode)DashboardConverter.jsonMapper.readTree(metaObject.getGridJSON());
            ServerUpgradeUtility.logger.debug((Object)("Flattening page grid with " + gridJson.get("components").size() + " components..."));
            flattenGrid(gridJson, 0, 0, 12);
            final ArrayNode newComponents = DashboardConverter.jsonMapper.createArrayNode();
            for (final JsonNode oldQVComponent : gridJson.get("components")) {
                final ArrayNode unwrappedDVComponents = convertQVGrid((ObjectNode)oldQVComponent);
                newComponents.addAll(unwrappedDVComponents);
            }
            gridJson.set("components", (JsonNode)newComponents);
            metaObject.setGridJSON(gridJson.toString());
            ServerUpgradeUtility.logger.debug((Object)("   Done flattening page grid. Page now has " + gridJson.get("components").size() + " components (all QVs)."));
        }
        
        private static ArrayNode convertQVGrid(final ObjectNode oldComponent) throws IOException {
            final String oldQVNameInComponent = oldComponent.get("content_id").asText();
            final LegacyQueryVisualization oldQV = DashboardConverter.oldQVs.get(oldQVNameInComponent);
            final ObjectNode oldQVGridJson = (ObjectNode)DashboardConverter.jsonMapper.readTree(oldQV.gridJSON);
            flattenGrid(oldQVGridJson, oldComponent.get("x").asInt(), oldComponent.get("y").asInt(), oldComponent.get("width").asInt());
            return (ArrayNode)oldQVGridJson.get("components");
        }
        
        private static void flattenGrid(final ObjectNode gridJson, final Integer parentX, final Integer parentY, final Integer parentWidth) throws IOException {
            final ArrayNode components = (ArrayNode)gridJson.get("components");
            final ArrayNode newComponents = DashboardConverter.jsonMapper.createArrayNode();
            for (final JsonNode jsonComponent : components) {
                final ObjectNode component = (ObjectNode)jsonComponent;
                final String componentType = (component.get("content_id") != null) ? "CONTENT" : ((component.get("grid") != null) ? "LAYOUT" : null);
                if (componentType == null) {
                    throw new IOException("Bad component type: " + component);
                }
                if ("LAYOUT".equals(componentType)) {
                    ServerUpgradeUtility.logger.debug((Object)("  -- Nested Layout with " + component.get("grid").get("components").size() + " components"));
                    flattenGrid((ObjectNode)component.get("grid"), component.get("x").asInt(), component.get("y").asInt(), component.get("width").asInt());
                    ServerUpgradeUtility.logger.debug((Object)("     Translating components inside nested grid at " + component.get("width") + "x" + component.get("height") + " (" + component.get("x") + "," + component.get("y") + ")"));
                    newComponents.addAll((ArrayNode)component.get("grid").get("components"));
                }
                else {
                    newComponents.add((JsonNode)component);
                    ServerUpgradeUtility.logger.debug((Object)("  -- " + newComponents.size() + " Adding new component " + component.get("content_id") + " " + component.get("width") + "x" + component.get("height") + " " + component.get("x") + "," + component.get("y")));
                }
            }
            for (final JsonNode nestedJsonComponent : newComponents) {
                translateLayout((ObjectNode)nestedJsonComponent, parentX, parentY, parentWidth);
            }
            gridJson.set("components", (JsonNode)newComponents);
        }
        
        private static void translateLayout(final ObjectNode nestedComponent, final Integer parentX, final Integer parentY, final Integer parentWidth) {
            final Double widthRatio = parentWidth / 12.0;
            final Double origWidth = nestedComponent.get("width").asDouble();
            final Double origHeight = nestedComponent.get("height").asDouble();
            final Double origX = nestedComponent.get("x").asDouble();
            final Double origY = nestedComponent.get("y").asDouble();
            final Double newWidth = origWidth * widthRatio;
            final Double newX = parentX + origX * widthRatio;
            final Double newY = parentY + origY;
            nestedComponent.put("width", newWidth);
            nestedComponent.put("x", newX);
            nestedComponent.put("y", newY);
            ServerUpgradeUtility.logger.info((Object)("Scaling " + nestedComponent.get("content_id") + " out from grid at (" + parentX + "," + parentY + ") with width ratio " + widthRatio + " : " + origWidth + "x" + origHeight + " (" + origX + "," + origY + ") --> " + nestedComponent.get("width") + "x" + origHeight + " (" + newX + "," + newY + ")"));
        }
        
        private static class LegacyQueryVisualization extends MetaInfo.MetaObject
        {
            String title;
            String query;
            List<String> dataVisualizations;
            String gridJSON;
            boolean composite;
            
            public void setComposite(final boolean composite) {
                this.composite = composite;
            }
            
            public void setTitle(final String title) {
                this.title = title;
            }
            
            public void setQuery(final String query) {
                this.query = query;
            }
            
            public void setDataVisualizations(final List<String> dataVisualizations) {
                this.dataVisualizations = dataVisualizations;
            }
            
            public void setGridJSON(final String gridJSON) {
                this.gridJSON = gridJSON;
            }
        }
    }
}
