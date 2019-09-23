package com.datasphere.runtime.meta;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.NotSet;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.appmanager.FlowUtil;
import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.WALoader;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.exception.SecurityException;
import com.datasphere.exceptionhandling.ExceptionType;
import com.datasphere.intf.QueryManager;
import com.datasphere.license.LicenseManager;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.actions.ActionableProperties;
import com.datasphere.metaRepository.actions.ActionablePropertiesFactory;
import com.datasphere.metaRepository.actions.BooleanProperties;
import com.datasphere.metaRepository.actions.EnumProperties;
import com.datasphere.metaRepository.actions.MetaObjectProperties;
import com.datasphere.metaRepository.actions.ObjectProperties;
import com.datasphere.metaRepository.actions.TextProperties;
import com.datasphere.persistence.HStore;
import com.datasphere.runtime.ActionType;
import com.datasphere.runtime.DeploymentStrategy;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.stmts.GracePeriod;
import com.datasphere.runtime.compiler.stmts.InputOutputSink;
import com.datasphere.runtime.compiler.stmts.OutputClause;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.components.MetaObjectPermissionChecker;
import com.datasphere.runtime.components.PropertyVariablePermissionChecker;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.FieldToObject;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.Permissable;
import com.datasphere.security.Roleable;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.tungsten.CluiMonitorView;
import com.datasphere.utility.GraphUtility;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

import flexjson.JSON;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class MetaInfo
{
    private static Logger logger;
    public static UUID GlobalUUID;
    public static final String GlobalSchemaName = "Global";
    public static final String AdminUserName = "admin";
    public static SimpleDateFormat sdf;
    public static Namespace GlobalNamespace;
    private static ActionablePropertiesFactory actionablePropertiesFactory;
    private static MDRepository metadataRepository;
    
    public static MetaObject makeGlobalMetaObject(final String name, final EntityType t) {
        final MetaObject o = new MetaObject();
        o.construct(name, MetaInfo.GlobalNamespace, t);
        return o;
    }
    
    public static ObjectNode getActions(final EntityType entityType) throws JsonProcessingException {
        Map actions_map = new HashMap();
        switch (entityType) {
            case APPLICATION: {
                actions_map = Flow.buildActions();
                break;
            }
            case STREAM: {
                actions_map = Stream.buildActions();
                break;
            }
            case WINDOW: {
                actions_map = Window.buildActions();
                break;
            }
            case TYPE: {
                actions_map = Type.buildActions();
                break;
            }
            case CQ: {
                actions_map = CQ.buildActions();
                break;
            }
            case SOURCE: {
                actions_map = Source.buildActions();
                break;
            }
            case TARGET: {
                actions_map = Target.buildActions();
                break;
            }
            case FLOW: {
                actions_map = Flow.buildActions();
                break;
            }
            case PROPERTYSET: {
                actions_map = PropertySet.buildActions();
                break;
            }
            case HDSTORE: {
                actions_map = HDStore.buildActions();
            }
            case CACHE: {
                actions_map = Cache.buildActions();
            }
            case WI: {}
            case ALERTSUBSCRIBER: {}
            case USER: {
                actions_map = User.buildActions();
                break;
            }
            case ROLE: {
                actions_map = Role.buildActions();
            }
            case INITIALIZER: {}
            case DG: {}
            case NAMESPACE: {
                actions_map = Namespace.buildActions();
            }
            case STREAM_GENERATOR: {}
            case SORTER: {}
            case WASTOREVIEW: {}
            case AGENT: {}
            case DASHBOARD: {}
            case PAGE: {}
            case QUERY: {
                actions_map = Query.buildActions();
                break;
            }
        }
        final ObjectNode action_node = MetaObject.convertToJson(actions_map);
        MetaObject.clearActionsMap();
        return action_node;
    }
    
    static {
        MetaInfo.logger = Logger.getLogger((Class)MetaInfo.class);
        MetaInfo.GlobalUUID = new UUID("01e2c3ec-e7fc-0e51-b069-28cfe9165d2d");
        MetaInfo.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        MetaInfo.GlobalNamespace = new Namespace();
        MetaInfo.actionablePropertiesFactory = new ActionablePropertiesFactory();
        MetaInfo.metadataRepository = MetadataRepository.getINSTANCE();
        MetaInfo.GlobalNamespace.construct("Global", MetaInfo.GlobalUUID);
    }
    
    public static class MetaObjectInfo implements Serializable
    {
        private static final long serialVersionUID = -6201744360586692853L;
        public UUID uuid;
        public EntityType type;
        public String name;
        public String nsName;
        
        public MetaObjectInfo() {
        }
        
        private MetaObjectInfo(final MetaObject o) {
            this.uuid = o.uuid;
            this.type = o.type;
            this.name = o.name;
            this.nsName = o.nsName;
        }
    }
    
    public static class MetaObject implements Serializable, Cloneable
    {
        private static final long serialVersionUID = -5924386859283245529L;
        public String metaObjectClass;
        public String name;
        public String uri;
        public UUID uuid;
        public int version;
        public int owner;
        public long ctime;
        public String nsName;
        public UUID namespaceId;
        public EntityType type;
        public String description;
        private Set<UUID> reverseIndexObjectDependencies;
        private String sourceText;
        private MetaInfoStatus metaInfoStatus;
        public Set<UUID> coDependentObjects;
        protected static Map<String, ActionableProperties> actions;
        @JsonIgnore
        protected ObjectMapper jsonMapper;
        
        public MetaObject() {
            this.metaObjectClass = this.getClass().getName();
            this.reverseIndexObjectDependencies = new LinkedHashSet<UUID>();
            this.metaInfoStatus = new MetaInfoStatus();
            this.coDependentObjects = new LinkedHashSet<UUID>(1);
            this.jsonMapper = ObjectMapperFactory.getInstance();
            this.name = null;
            this.nsName = null;
            this.namespaceId = null;
            this.type = null;
            this.ctime = 0L;
            this.uuid = null;
        }
        
        public String prettyPrintMap(final Map<String, Object> propertyMap) {
            String str = "";
            final Iterator<Map.Entry<String, Object>> i = propertyMap.entrySet().iterator();
            while (i.hasNext()) {
                final Map.Entry<String, Object> e = i.next();
                final String key = e.getKey();
                final Object value = (e.getValue() == null) ? "<NOTSET>" : e.getValue();
                if (!i.hasNext()) {
                    if (value == null) {
                        continue;
                    }
                    if (!key.equals("directory")) {
                        str = str + "   " + key + ": " + StringEscapeUtils.escapeJava(value.toString()) + "\n";
                    }
                    else {
                        str = str + "   " + key + ": " + value + "\n";
                    }
                }
                else if (!key.equals("directory")) {
                    str = str + "   " + key + ": " + StringEscapeUtils.escapeJava(value.toString()) + ",\n";
                }
                else {
                    str = str + "   " + key + ": " + value + ",\n";
                }
            }
            this.printDependencyAndStatus();
            return str;
        }
        
        public List<String> convertToNewSyntax(final List<ObjectPermission> permissions) {
            final List<String> resultSet = new ArrayList<String>();
            if (permissions == null) {
                return resultSet;
            }
            for (final ObjectPermission permission : permissions) {
                final String permissionString = permission.toString();
                final String[] splitArray = permissionString.split(":");
                if (splitArray.length < 4) {
                    continue;
                }
                final String[] subTypes = splitArray[2].split(",");
                final StringBuffer stringBuffer = new StringBuffer();
                for (int i = 0; i < subTypes.length; ++i) {
                    stringBuffer.append((splitArray.length == 4) ? "GRANT " : "REVOKE ");
                    if (splitArray[1].equalsIgnoreCase("*")) {
                        stringBuffer.append("ALL ");
                    }
                    else {
                        stringBuffer.append(splitArray[1].toUpperCase() + " ");
                    }
                    stringBuffer.append("ON ");
                    stringBuffer.append(subTypes[i] + " ");
                    stringBuffer.append(splitArray[0] + "." + splitArray[3]);
                    if (i != subTypes.length - 1) {
                        stringBuffer.append(", ");
                    }
                }
                resultSet.add(stringBuffer.toString());
            }
            return resultSet;
        }
        
        public void construct(final String name, final Namespace ns, final EntityType objtype) {
            this.construct(name, null, ns, objtype);
        }
        
        public void construct(final String name, final UUID uuid, final Namespace ns, final EntityType objtype) {
            this.construct(name, uuid, ns.name, ns.uuid, objtype);
        }
        
        private void construct(final String name, final UUID uuid, final String nsName, final UUID namespaceId, final EntityType objtype) {
            final int dot = name.lastIndexOf(46);
            if (dot != -1 && objtype != EntityType.SERVER && objtype != EntityType.PROPERTYTEMPLATE) {
                this.name = name.substring(dot + 1);
            }
            else {
                this.name = name;
            }
            this.nsName = nsName;
            this.namespaceId = namespaceId;
            this.type = objtype;
            this.ctime = System.currentTimeMillis();
            if (uuid == null) {
                this.uuid = new UUID(this.ctime);
            }
            else {
                this.uuid = uuid;
            }
        }
        
        public String getName() {
            return this.name;
        }
        
        public void setName(final String name) {
            this.name = name;
        }
        
        public UUID getUuid() {
            return this.uuid;
        }
        
        public void setUuid(final UUID uuid) {
            this.uuid = uuid;
        }
        
        public long getCtime() {
            return this.ctime;
        }
        
        public void setCtime(final long ctime) {
            this.ctime = ctime;
        }
        
        public String getNsName() {
            return this.nsName;
        }
        
        public void setNsName(final String nsName) {
            this.nsName = nsName;
        }
        
        public UUID getNamespaceId() {
            return this.namespaceId;
        }
        
        public void setNamespaceId(final UUID namespaceId) {
            this.namespaceId = namespaceId;
        }
        
        public void setType(final EntityType type) {
            this.type = type;
        }
        
        public EntityType getType() {
            return this.type;
        }
        
        @JSON(include = false)
        @JsonIgnore
        public static MetaObject obtainMetaObject(final UUID uuid) {
            try {
                return MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.info((Object)("Couldn't find the object with UUID " + uuid));
                return null;
            }
        }
        
        @JSON(include = false)
        @JsonIgnore
        public List<UUID> getDependencies() {
            return Collections.emptyList();
        }
        
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            graph.get(this.getUuid());
            return graph;
        }
        
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            graph.get(this.getUuid());
            return graph;
        }
        
        public void addReverseIndexObjectDependencies(final UUID uuid) {
            this.reverseIndexObjectDependencies.add(uuid);
        }
        
        public String metaToString() {
            return this.name + " " + this.type + " " + this.version + " " + this.uuid;
        }
        
        @Override
        public String toString() {
            return this.metaToString();
        }
        
        public String getUri() {
            return this.uri;
        }
        
        public void setUri(final String uri) {
            this.uri = uri;
        }
        
        public void setDescription(final String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return this.description;
        }
        
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            return this.startDescribe();
        }
        
        public String startDescribe() {
            String desc = this.type + " " + (this.type.isGlobal() ? this.name : (this.nsName + "." + this.name)) + " CREATED " + MetaInfo.sdf.format(new Date(this.ctime));
            desc += "\n";
            if (this.description != null) {
                desc = desc + this.description + "\n";
            }
            return desc;
        }
        
        public void printDependencyAndStatus() {
            if (MetaInfo.logger.isDebugEnabled()) {
                MetaInfo.logger.debug((Object)("getReverseIndexObjectDependencies() => \n" + this.getReverseIndexObjectDependencies()));
                MetaInfo.logger.debug((Object)("getMetaInfoStatus()=> \n" + this.getMetaInfoStatus()));
                MetaInfo.logger.debug((Object)("getSourceText()=> " + this.getSourceText()));
            }
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof MetaObject) {
                final MetaObject other = (MetaObject)obj;
                return this.uuid != null && other.uuid != null && this.uuid.equals((Object)other.uuid);
            }
            return false;
        }
        
        public MetaObject clone() throws CloneNotSupportedException {
            return (MetaObject)super.clone();
        }
        
        @JsonIgnore
        public String getFullName() {
            return this.nsName + "." + this.name;
        }
        
        @JsonIgnore
        public String getFQN() {
            return this.nsName + "." + this.type + "." + this.name;
        }
        
        @JsonIgnore
        public String convertNameToFullQualifiedName(final String nsName, final String fullQualifiedObjectName, final EntityType type) {
            String objectNamespace = Utility.splitDomain(fullQualifiedObjectName);
            final String objectName = Utility.splitName(fullQualifiedObjectName);
            if (objectNamespace == null) {
                objectNamespace = nsName;
            }
            return objectNamespace + "." + type + "." + objectName;
        }
        
        public MetaInfoStatus getMetaInfoStatus() {
            return this.metaInfoStatus;
        }
        
        public void setMetaInfoStatus(final MetaInfoStatus metaInfoStatus) {
            this.metaInfoStatus = metaInfoStatus;
        }
        
        public Set<UUID> getReverseIndexObjectDependencies() {
            return this.reverseIndexObjectDependencies;
        }
        
        public String getSourceText() {
            return this.sourceText;
        }
        
        public void setSourceText(final String sourceText) {
            this.sourceText = sourceText;
        }
        
        @Override
        public int hashCode() {
            return this.uuid.hashCode();
        }
        
        @JSON(include = false)
        @JsonIgnore
        public Flow getCurrentApp() throws MetaDataRepositoryException {
            return getCurrentApp(this);
        }
        
        @JSON(include = false)
        @JsonIgnore
        public Dashboard getCurrentDashBoard() throws MetaDataRepositoryException {
            return getCurrentDashBoard(this);
        }
        
        @JSON(include = false)
        @JsonIgnore
        public Flow getCurrentFlow() throws MetaDataRepositoryException {
            return getCurrentFlow(this);
        }
        
        public static Dashboard getCurrentDashBoard(final MetaObject obj) throws MetaDataRepositoryException {
            if (obj == null) {
                return null;
            }
            if (obj.type.equals(EntityType.DASHBOARD)) {
                return (Dashboard)obj;
            }
            if (!obj.type.canBePartOfDashboard()) {
                return null;
            }
            final Set<Dashboard> dashboardSet = (Set<Dashboard>)MetadataRepository.getINSTANCE().getByEntityType(EntityType.DASHBOARD, HSecurityManager.TOKEN);
            for (final Dashboard dashboard : dashboardSet) {
                final List<String> pages = dashboard.getPages();
                if (pages == null) {
                    continue;
                }
                for (final String pageName : pages) {
                    if (obj.getType().equals(EntityType.PAGE)) {
                        if (pageName.equals(obj.getName()) && dashboard.getNsName().equals(obj.getNsName())) {
                            return dashboard;
                        }
                        continue;
                    }
                    else {
                        if (!obj.getType().equals(EntityType.QUERYVISUALIZATION)) {
                            continue;
                        }
                        final Page pageMetaObject = (Page)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PAGE, obj.getNsName(), pageName, null, HSecurityManager.TOKEN);
                        final List<String> queryVisualizations = pageMetaObject.getQueryVisualizations();
                        if (queryVisualizations == null) {
                            continue;
                        }
                        for (final String queryVisualization : pageMetaObject.getQueryVisualizations()) {
                            if (queryVisualization.equals(obj.getName()) && dashboard.getNsName().equals(obj.getNsName())) {
                                return dashboard;
                            }
                        }
                    }
                }
            }
            return null;
        }
        
        public static Flow getCurrentFlow(final MetaObject obj) throws MetaDataRepositoryException {
            if (obj == null) {
                return null;
            }
            if (obj.type.equals(EntityType.APPLICATION) || obj.type.equals(EntityType.FLOW)) {
                return (Flow)obj;
            }
            if (!obj.type.canBePartOfApp()) {
                return null;
            }
            final Set<UUID> parents = obj.getReverseIndexObjectDependencies();
            if (parents != null) {
                for (final UUID aUUID : parents) {
                    final MetaObject parent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(aUUID, HSecurityManager.TOKEN);
                    if (parent != null && parent.type.equals(EntityType.FLOW)) {
                        return (Flow)parent;
                    }
                }
            }
            return null;
        }
        
        public static Flow getCurrentApp(final MetaObject obj) throws MetaDataRepositoryException {
            if (obj == null) {
                return null;
            }
            if (obj.type.equals(EntityType.APPLICATION)) {
                return (Flow)obj;
            }
            if (!obj.type.canBePartOfApp()) {
                return null;
            }
            final Set<UUID> parents = obj.getReverseIndexObjectDependencies();
            if (parents != null) {
                for (final UUID aUUID : parents) {
                    final MetaObject parent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(aUUID, HSecurityManager.TOKEN);
                    if (parent != null && parent.type == EntityType.APPLICATION) {
                        return (Flow)parent;
                    }
                }
            }
            return null;
        }
        
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = new JSONObject();
            json.put("metaObjectClass", this.metaObjectClass);
            json.put("id", this.getFullName());
            json.put("name", this.getName());
            json.put("uri", this.getUri());
            json.put("uuid", this.getUuid().getUUIDString());
            json.put("nsName", this.getNsName());
            json.put("namespaceId", this.getNamespaceId().getUUIDString());
            json.put("type", this.getType());
            json.put("description", this.getDescription());
            json.put("ctime", this.getCtime());
            final MetaInfoStatus mis = this.getMetaInfoStatus();
            final JSONObject misJ = new JSONObject();
            misJ.put("isValid", mis.isValid());
            misJ.put("isAnonymous", mis.isAnonymous());
            misJ.put("isDropped", mis.isDropped());
            misJ.put("isAdhoc", mis.isAdhoc());
            json.put("metaInfoStatus", misJ);
            return json;
        }
        
        public String JSONifyString() {
            try {
                return this.JSONify().toString();
            }
            catch (JSONException e) {
                e.printStackTrace();
                return "{}";
            }
        }
        
        public static ObjectNode convertToJson(final Map<String, ActionableProperties> actions) throws JsonProcessingException {
            final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
            final ObjectNode node = jsonMapper.createObjectNode();
            for (final Map.Entry<String, ActionableProperties> entry : actions.entrySet()) {
                node.set((String)entry.getKey(), (JsonNode)entry.getValue().getJsonObject());
            }
            return node;
        }
        
        public static Map buildBaseActions() {
            final TextProperties textProperties = MetaInfo.actionablePropertiesFactory.createTextProperties();
            textProperties.setIsRequired(true);
            MetaObject.actions.put("name", textProperties);
            MetaObject.actions.put("nsName", textProperties);
            return MetaObject.actions;
        }
        
        public static void clearActionsMap() {
            MetaObject.actions.clear();
        }
        
        @JsonIgnore
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode rootNode = this.jsonMapper.createObjectNode();
            rootNode.put("id", this.getFQN());
            rootNode.put("name", this.name);
            rootNode.put("nsName", this.nsName);
            rootNode.put("uuid", this.uuid.toString());
            rootNode.put("type", this.type.toString());
            rootNode.put("ctime", this.ctime);
            final MetaInfoStatus metaInfoStatus = this.getMetaInfoStatus();
            final ObjectNode metaInfoStatus_JSON = this.jsonMapper.createObjectNode();
            metaInfoStatus_JSON.put("isValid", metaInfoStatus.isValid());
            metaInfoStatus_JSON.put("isAnonymous", metaInfoStatus.isAnonymous());
            metaInfoStatus_JSON.put("isDropped", metaInfoStatus.isDropped());
            metaInfoStatus_JSON.put("isAdhoc", metaInfoStatus.isAdhoc());
            rootNode.set("metaInfoStatus", (JsonNode)metaInfoStatus_JSON);
            final Set<UUID> reverseDependencies = this.getReverseIndexObjectDependencies();
            boolean isEditable = true;
            if (reverseDependencies != null) {
                for (final UUID uuid : reverseDependencies) {
                    MetaObject metaObject = null;
                    try {
                        metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                        if (metaObject == null || !metaObject.getType().equals(EntityType.APPLICATION)) {
                            continue;
                        }
                        final Flow app = (Flow)metaObject;
                        final StatusInfo statusInfo = FlowUtil.getCurrentStatus(app.getUuid());
                        if (statusInfo != null && statusInfo.getStatus() != StatusInfo.Status.CREATED) {
                            isEditable = false;
                            break;
                        }
                        continue;
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn(e.getMessage(), (Throwable)e);
                    }
                    catch (Exception e2) {
                        MetaInfo.logger.error(("Problem processing metaobject:" + metaObject), (Throwable)e2);
                    }
                }
                rootNode.put("isEditable", isEditable);
            }
            return rootNode;
        }
        
        public MetaObjectInfo makeMetaObjectInfo() {
            return new MetaObjectInfo(this);
        }
        
        public static MetaObject deserialize(final String json) throws JsonParseException, JsonMappingException, IOException {
            if (json == null) {
                return null;
            }
            return MetaInfoJsonSerializer.MetaObjectJsonSerializer.deserialize(json);
        }
        
        public void addCoDependentObject(final UUID uuid) {
            if (uuid != null) {
                this.coDependentObjects.add(uuid);
            }
        }
        
        static {
            MetaObject.actions = new HashMap<String, ActionableProperties>();
        }
    }
    
    public static class Namespace extends MetaObject
    {
        private static final long serialVersionUID = 2589468991471181011L;
        
        public void construct(final String name, final UUID namespaceId) {
            super.construct(name, namespaceId, "Global", MetaInfo.GlobalUUID, EntityType.NAMESPACE);
        }
        
        @Override
        public String toString() {
            return this.name + " NAMESPACE " + this.uuid;
        }
        
        @Override
        public String describe(final AuthToken token) {
            this.printDependencyAndStatus();
            final StringBuilder buffer = new StringBuilder();
            buffer.append(this.startDescribe()).append("CONTAINS OBJECTS (\n");
            final Set<UUID> objects = HazelcastSingleton.get().getSet("#" + this.getName());
            if (objects != null) {
                for (final UUID uuid : objects) {
                    try {
                        final MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, token);
                        if (obj == null) {
                            continue;
                        }
                        buffer.append("\t").append(obj.type.toString().toUpperCase()).append(" ").append(obj.name.toUpperCase()).append(", \n");
                    }
                    catch (MetaDataRepositoryException e) {
                        if (!MetaInfo.logger.isInfoEnabled()) {
                            continue;
                        }
                        MetaInfo.logger.info(e.getMessage());
                    }
                }
            }
            buffer.append(")\n");
            return buffer.toString();
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            return MetaObject.buildBaseActions();
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class Type extends MetaObject
    {
        private static final long serialVersionUID = -7638834212660192367L;
        public Boolean generated;
        public String className;
        public UUID extendsType;
        public List<UUID> extendedBy;
        public int classId;
        public List<String> keyFields;
        public Map<String, String> fields;
        
        public Type() {
            this.extendedBy = null;
            this.classId = -1;
            this.generated = false;
            this.className = null;
            this.extendsType = null;
            this.keyFields = Collections.emptyList();
            this.fields = Collections.emptyMap();
        }
        
        public void construct(final String name, final Namespace ns, final String className, final Map<String, String> fields, final List<String> keyFields, final boolean generated) {
            this.construct(name, ns, className, null, fields, keyFields, generated);
        }
        
        public void construct(final String name, final Namespace ns, final String className, final UUID extendsType, final Map<String, String> fields, final List<String> keyFields, final boolean generated) {
            super.construct(name, ns, EntityType.TYPE);
            this.className = className;
            this.extendsType = extendsType;
            this.keyFields = ((keyFields == null) ? Collections.emptyList() : keyFields);
            this.fields = ((fields == null) ? Collections.emptyMap() : preprocessFieldNames(fields));
            this.generated = generated;
            if (generated) {
                this.classId = WALoader.get().getClassId(className);
            }
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " class:" + this.className + " key:" + this.keyFields;
        }
        
        public int getClassId() {
            return this.classId;
        }
        
        public synchronized void generateClass() throws Exception {
            final WALoader loader = WALoader.get();
            loader.setClassId(this.className, this.classId);
            if (this.generated) {
                final AtomicBoolean isSuccesful = new AtomicBoolean(true);
                loader.lockClass(this.className);
                try {
                    final byte[] bytecode = loader.getClassBytes(this.className);
                    if (bytecode == null) {
                        if (MetaInfo.logger.isDebugEnabled()) {
                            MetaInfo.logger.debug(("Generating class for " + this.className + " fields = " + this.fields));
                        }
                        if (loader.getTypeBundleDef(this.nsName, this.className) == null) {
                            loader.addTypeClass(this.nsName, this.className, this.fields);
                        }
                    }
                }
                catch (Exception e) {
                    loader.removeBundle(this.nsName, BundleDefinition.Type.type, this.className);
                    isSuccesful.set(false);
                }
                finally {
                    loader.unlockClass(this.className);
                    if (!isSuccesful.get()) {
                        throw new Exception("Problem creating type: " + this.getFullName());
                    }
                }
            }
            else {
                try {
                    final Class<?> clazz = loader.loadClass(this.className);
                    if (!KryoSingleton.get().isClassRegistered(clazz)) {
                        final int classId = loader.getClassId(this.className);
                        KryoSingleton.get().addClassRegistration(clazz, classId);
                    }
                }
                catch (ClassNotFoundException e2) {
                    MetaInfo.logger.error(("Could not locate class " + this.className + " for type " + this.nsName + "." + this.name));
                }
            }
        }
        
        public synchronized void removeClass() {
            if (this.generated) {
                final WALoader loader = WALoader.get();
                loader.lockClass(this.className);
                try {
                    loader.removeTypeClass(this.nsName, this.className);
                }
                catch (Exception e) {
                    MetaInfo.logger.error(("Problem removing class for existing type: " + this.type), (Throwable)e);
                }
                finally {
                    loader.unlockClass(this.className);
                }
            }
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe() + "ATTRIBUTES (\n";
            for (final Map.Entry<String, String> attr : this.fields.entrySet()) {
                final String fieldName = attr.getKey();
                final boolean isKey = this.keyFields.contains(fieldName);
                str = str + "  " + fieldName + " " + attr.getValue() + (isKey ? " KEY " : "") + "\n";
            }
            str += ")\n";
            this.printDependencyAndStatus();
            if (this.extendsType != null) {
                str = str + "EXTENDS " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.extendsType, HSecurityManager.TOKEN).name + "\n";
            }
            if (this.extendedBy != null) {
                str += "EXTENDED BY [\n";
                boolean first = true;
                for (final UUID ex : this.extendedBy) {
                    if (!first) {
                        str += ",\n";
                    }
                    else {
                        first = false;
                    }
                    str = str + "  " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(ex, HSecurityManager.TOKEN).name;
                }
                str += "\n]";
            }
            return str;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final ObjectProperties objectProperties_1 = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            objectProperties_1.setIsRequired(true);
            final ObjectProperties objectProperties_2 = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            Type.actions.put("fields", objectProperties_1);
            Type.actions.put("keyField", objectProperties_2);
            return Type.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("generated", this.generated);
            final ArrayNode fieldArray = this.jsonMapper.createArrayNode();
            for (final Map.Entry<String, String> entry : this.fields.entrySet()) {
                final ObjectNode fieldNode = this.jsonMapper.createObjectNode();
                fieldNode.put("name", (String)entry.getKey());
                fieldNode.put("type", (String)entry.getValue());
                if (this.keyFields.contains(entry.getKey())) {
                    fieldNode.put("isKey", true);
                }
                else {
                    fieldNode.put("isKey", false);
                }
                fieldArray.add((JsonNode)fieldNode);
            }
            parent.set("fields", (JsonNode)fieldArray);
            return parent;
        }
        
        @Override
        public String JSONifyString() {
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            try {
                final String JSON = jsonMapper.writeValueAsString((Object)this);
                final JsonNode actualObj = jsonMapper.readTree(JSON);
                return actualObj.toString();
            }
            catch (IOException e) {
                if (MetaInfo.logger.isInfoEnabled()) {
                    MetaInfo.logger.info((Object)e.getMessage());
                }
                return null;
            }
        }
        
        private static Map<String, String> preprocessFieldNames(final Map<String, String> fields) {
            final Map<String, String> ret = Factory.makeLinkedMap();
            for (final Map.Entry<String, String> e : fields.entrySet()) {
                String fieldName = e.getKey();
                final String typeName = e.getValue();
                if (CompilerUtils.isJavaKeyword(fieldName)) {
                    fieldName = CompilerUtils.capitalize(fieldName);
                }
                ret.put(fieldName, typeName);
            }
            return ret;
        }
    }
    
    public static class Stream extends MetaObject
    {
        private static final long serialVersionUID = 6543159810445090302L;
        public UUID dataType;
        public List<String> partitioningFields;
        public Interval gracePeriodInterval;
        public String gracePeriodField;
        public String pset;
        public PropertySet propertySet;
        public String avroSchema;
        
        public Stream() {
            this.dataType = null;
            this.partitioningFields = null;
            this.gracePeriodInterval = null;
            this.gracePeriodField = null;
            this.propertySet = null;
        }
        
        public void construct(final String name, final Namespace ns, final UUID dataType, final List<String> partitioningFields, final GracePeriod gp, final String pset) {
            this.construct(name, null, ns, dataType, partitioningFields, gp, pset);
        }
        
        public void construct(final String name, final UUID streamUUID, final Namespace ns, final UUID dataType, final List<String> partitioningFields, final GracePeriod gp, final String pset) {
            super.construct(name, streamUUID, ns, EntityType.STREAM);
            this.dataType = dataType;
            this.partitioningFields = ((partitioningFields == null) ? Collections.emptyList() : partitioningFields);
            this.gracePeriodInterval = ((gp == null) ? null : gp.sortTimeInterval);
            this.gracePeriodField = ((gp == null) ? null : gp.fieldName);
            this.pset = pset;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final MetaObjectProperties metaObjectProperties = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            metaObjectProperties.setIsRequired(true);
            final ObjectProperties objectProperties = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final BooleanProperties persist = MetaInfo.actionablePropertiesFactory.createBooleanProperties();
            final TextProperties propertySet = MetaInfo.actionablePropertiesFactory.createTextProperties();
            Stream.actions.put("dataType", metaObjectProperties);
            Stream.actions.put("partitioningFields", objectProperties);
            Stream.actions.put("persist", persist);
            Stream.actions.put("propertySet", propertySet);
            return Stream.actions;
        }
        
        public UUID getDataType() {
            return this.dataType;
        }
        
        public void setDataType(final UUID dataType) {
            this.dataType = dataType;
        }
        
        public List<String> getPartitioningFields() {
            return this.partitioningFields;
        }
        
        public void setPartitioningFields(final List<String> partitioningFields) {
            this.partitioningFields = partitioningFields;
        }
        
        public Interval getGracePeriodInterval() {
            return this.gracePeriodInterval;
        }
        
        public void setGracePeriodInterval(final Interval gracePeriodInterval) {
            this.gracePeriodInterval = gracePeriodInterval;
        }
        
        public String getGracePeriodField() {
            return this.gracePeriodField;
        }
        
        public void setGracePeriodField(final String gracePeriodField) {
            this.gracePeriodField = gracePeriodField;
        }
        
        public void setPropertySet(final PropertySet propertySet) {
            this.propertySet = propertySet;
        }
        
        public void setPset(final String pset) {
            this.pset = pset;
        }
        
        public void setAvroSchema(final String avroSchema) {
            this.avroSchema = avroSchema;
        }
        
        @Override
        public List<UUID> getDependencies() {
            return Collections.singletonList(this.dataType);
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " dataType:" + this.dataType;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe();
            str += "OF TYPE ";
            if (MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.dataType, HSecurityManager.TOKEN) != null) {
                str += MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.dataType, HSecurityManager.TOKEN).name;
            }
            str = str + " PARTITION BY " + this.partitioningFields;
            if (this.pset != null) {
                str = str + " PERSIST USING " + this.pset + " WITH PROPERTIES " + this.propertySet;
            }
            this.printDependencyAndStatus();
            return str;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            final ArrayNode pfArray = this.jsonMapper.createArrayNode();
            for (final String ss : this.partitioningFields) {
                pfArray.add(ss);
            }
            parent.set("partitioningFields", (JsonNode)pfArray);
            if (this.dataType != null) {
                try {
                    final MetaObject typeDef = MetaInfo.metadataRepository.getMetaObjectByUUID(this.dataType, HSecurityManager.TOKEN);
                    if (typeDef != null) {
                        parent.put("dataType", typeDef.getFQN());
                    }
                    else {
                        parent.putNull("dataType");
                    }
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("dataType");
            }
            if (this.propertySet != null) {
                parent.put("persist", true);
                parent.put("propertySet", this.propertySet.getFQN());
            }
            else {
                parent.put("persist", false);
                parent.putNull("propertySet");
            }
            return parent;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            return super.inEdges(graph);
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            final MetaObject obj = MetaObject.obtainMetaObject(this.dataType);
            if (obj == null) {
                return graph;
            }
            if (!obj.getMetaInfoStatus().isAnonymous()) {
                graph.get(this.dataType).add(this.getUuid());
            }
            return graph;
        }
    }
    
    public static class StreamGenerator extends MetaObject
    {
        private static final long serialVersionUID = 245647756917065648L;
        public UUID dataType;
        public String className;
        public Object[] args;
        
        public StreamGenerator() {
            this.dataType = null;
            this.className = null;
            this.args = null;
        }
        
        public void construct(final String name, final Namespace ns, final UUID dataType, final String className, final Object[] args) {
            super.construct(name, ns, EntityType.STREAM_GENERATOR);
            assert dataType != null;
            this.dataType = dataType;
            this.className = className;
            this.args = args.clone();
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " class:" + this.className + " args:" + this.args;
        }
        
        @Override
        public List<UUID> getDependencies() {
            return Collections.singletonList(this.dataType);
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class Window extends MetaObject
    {
        private static final long serialVersionUID = -5342738459832800581L;
        public UUID stream;
        public boolean jumping;
        public boolean persistent;
        public boolean implicit;
        public List<String> partitioningFields;
        public IntervalPolicy windowLen;
        public IntervalPolicy slidePolicy;
        public Object options;
        
        public Window() {
            this.stream = null;
            this.jumping = false;
            this.persistent = false;
            this.implicit = false;
            this.partitioningFields = null;
            this.windowLen = null;
            this.slidePolicy = null;
            this.options = null;
        }
        
        public void construct(final String name, final Namespace ns, final UUID stream, final Pair<IntervalPolicy, IntervalPolicy> windowLen, final boolean jumping, final boolean persistent, final List<String> partitioningFields, final boolean implicit, final Object options) {
            super.construct(name, ns, EntityType.WINDOW);
            this.stream = stream;
            this.jumping = jumping;
            this.persistent = persistent;
            this.partitioningFields = ((partitioningFields == null) ? Collections.emptyList() : partitioningFields);
            this.windowLen = windowLen.first;
            this.slidePolicy = windowLen.second;
            this.implicit = implicit;
            this.options = options;
        }
        
        @Override
        public List<UUID> getDependencies() {
            return Collections.singletonList(this.stream);
        }
        
        @Override
        public String toString() {
            return this.metaToString() + (this.jumping ? "jumping" : "sliding") + " stream:" + this.stream + " keep:" + this.windowLen + " partition:" + this.partitioningFields;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            this.printDependencyAndStatus();
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(this.startDescribe()).append(" ON STREAM ").append(MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, HSecurityManager.TOKEN).name);
            if (this.jumping) {
                if (this.isSessionWindow()) {
                    stringBuilder.append(" SESSION");
                }
                else {
                    stringBuilder.append(" JUMPING");
                }
            }
            else {
                stringBuilder.append(" SLIDING");
            }
            stringBuilder.append(" WINDOW CONDITIONS" + this.windowLen + " PARTITIONED BY:").append(this.partitioningFields);
            return stringBuilder.toString();
        }
        
        @JsonIgnore
        public boolean isLikeCache() {
            return !this.jumping && this.windowLen != null && this.partitioningFields != null && this.windowLen.isCount1() && !this.partitioningFields.isEmpty();
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            graph.get(this.stream).add(this.getUuid());
            return super.inEdges(graph);
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            graph.get(this.stream).add(this.getUuid());
            return graph;
        }
        
        public static Window deserialize(final JsonNode jsonNode) throws JsonParseException, JsonMappingException, IOException {
            if (jsonNode == null) {
                return null;
            }
            final String className = jsonNode.get("metaObjectClass").asText();
            final String expect = Window.class.getCanonicalName();
            if (!className.equalsIgnoreCase(expect)) {
                MetaInfo.logger.warn((Object)("wrong node passed. Expecting :" + Window.class.getCanonicalName() + " and got :" + className));
                return null;
            }
            final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
            jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            final SimpleModule module = new SimpleModule("IntervalPolicyDeserializer");
            final IntervalPolicy.IntervalPolicyDeserializer des = new IntervalPolicy.IntervalPolicyDeserializer();
            module.addDeserializer((Class)IntervalPolicy.class, (JsonDeserializer)des);
            jsonMapper.registerModule((Module)module);
            return (Window)jsonMapper.readValue(jsonNode.toString(), (Class)Window.class);
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            stream_p.setIsRequired(true);
            final ObjectProperties partitionFields_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties windowLen_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            windowLen_p.setIsRequired(true);
            final EnumProperties windowMode_p = MetaInfo.actionablePropertiesFactory.createEnumProperties();
            windowMode_p.setIsRequired(true);
            Window.actions.put("stream", stream_p);
            Window.actions.put("partitioningFields", partitionFields_p);
            Window.actions.put("size", windowLen_p);
            Window.actions.put("mode", windowMode_p);
            return Window.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            if (this.stream != null) {
                try {
                    final MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.stream, HSecurityManager.TOKEN);
                    if (streamInfo != null) {
                        parent.put("stream", streamInfo.getFQN());
                    }
                    else {
                        parent.putNull("stream");
                    }
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("stream");
            }
            if (this.jumping) {
                if (this.isSessionWindow()) {
                    parent.put("mode", "session");
                }
                else {
                    parent.put("mode", "jumping");
                }
            }
            else {
                parent.put("mode", "sliding");
            }
            final ArrayNode pNode = this.jsonMapper.createArrayNode();
            for (final String pF : this.partitioningFields) {
                pNode.add(pF);
            }
            parent.set("partitioningFields", (JsonNode)pNode);
            final Map<String, IntervalPolicy> policyMap = Maps.newHashMap();
            policyMap.put("size", this.windowLen);
            policyMap.put("slidePolicy", this.slidePolicy);
            final ObjectNode windowPolicy = this.jsonMapper.createObjectNode();
            if (this.windowLen != null) {
                final String windowType = Utility.getWindowType(this.windowLen);
                if (StringUtils.isNotBlank((CharSequence)windowType)) {
                    windowPolicy.put("type", windowType);
                }
                if (this.windowLen.getCountPolicy() != null) {
                    windowPolicy.put("count", this.windowLen.getCountPolicy().getCountInterval());
                }
                if (this.windowLen.getTimePolicy() != null) {
                    if ("hybrid".equals(windowType) && this.windowLen.getAttrPolicy() == null) {
                        windowPolicy.put("timeout", this.windowLen.getTimePolicy().getTimeInterval());
                    }
                    else {
                        windowPolicy.put("time", this.windowLen.getTimePolicy().time.toHumanReadable());
                    }
                }
                if (this.windowLen.getAttrPolicy() != null) {
                    windowPolicy.put("onField", this.windowLen.getAttrPolicy().getAttrName());
                    if ("time".equals(windowType) && this.windowLen.getTimePolicy() == null) {
                        windowPolicy.put("time", new Interval(this.windowLen.getAttrPolicy().getAttrValueRange()).toHumanReadable());
                    }
                    else {
                        windowPolicy.put("timeout", this.windowLen.getAttrPolicy().getAttrValueRange());
                    }
                }
                if (this.slidePolicy != null) {
                    if (("time".equals(windowType) || "hybrid".equals(windowType)) && this.slidePolicy.getTimePolicy() != null) {
                        windowPolicy.put("outputInterval", this.slidePolicy.getTimePolicy().getTimeInterval());
                    }
                    if (("time".equals(windowType) || "hybrid".equals(windowType)) && this.slidePolicy.getTimePolicy() == null && this.slidePolicy.getAttrPolicy() != null) {
                        windowPolicy.put("outputInterval", this.slidePolicy.getAttrPolicy().getAttrValueRange());
                    }
                    if (("count".equals(windowType) || "hybrid".equals(windowType)) && this.slidePolicy.getCountPolicy() != null) {
                        windowPolicy.put("outputInterval", this.slidePolicy.getCountPolicy().getCountInterval());
                    }
                }
                parent.set("size", (JsonNode)windowPolicy);
            }
            else {
                parent.putNull("size");
            }
            return parent;
        }
        
        private Boolean isSessionWindow() {
            if (this.jumping && this.windowLen != null && this.windowLen.getCountPolicy() != null && this.windowLen.getCountPolicy().getCountInterval() == -1) {
                return true;
            }
            return false;
        }
    }
    
    public static class CQ extends MetaObject
    {
        private static final long serialVersionUID = 3340037099551644462L;
        public UUID stream;
        public CQExecutionPlan plan;
        public String select;
        public List<String> fieldList;
        public String uiConfig;
        
        public CQ() {
            this.stream = null;
            this.plan = null;
            this.select = null;
            this.fieldList = null;
            this.uiConfig = null;
        }
        
        public void construct(final String name, final Namespace ns, final UUID stream, final CQExecutionPlan plan, final String select, final List<String> fieldList) {
            super.construct(name, ns, EntityType.CQ);
            this.stream = stream;
            this.plan = plan;
            this.select = select;
            this.fieldList = fieldList;
        }
        
        public void construct(final String name, final Namespace ns, final UUID stream, final CQExecutionPlan plan, final String select, final List<String> fieldList, final String uiConfig) {
            super.construct(name, ns, EntityType.CQ);
            this.stream = stream;
            this.plan = plan;
            this.select = select;
            this.fieldList = fieldList;
            this.uiConfig = uiConfig;
        }
        
        public UUID getStream() {
            return this.stream;
        }
        
        public void setStream(final UUID stream) {
            this.stream = stream;
        }
        
        public CQExecutionPlan getPlan() {
            return this.plan;
        }
        
        public void setPlan(final CQExecutionPlan plan) {
            this.plan = plan;
        }
        
        public String getSelect() {
            return this.select;
        }
        
        public void setSelect(final String select) {
            this.select = select;
        }
        
        public List<String> getFieldList() {
            return this.fieldList;
        }
        
        public void setFieldList(final List<String> fieldList) {
            this.fieldList = fieldList;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            if (this.stream != null) {
                graph.get(this.stream);
                graph.get(this.getUuid()).add(this.stream);
            }
            if (this.plan != null) {
                for (final UUID dataSource : this.plan.getDataSources()) {
                    graph.get(dataSource).add(this.getUuid());
                }
            }
            return super.inEdges(graph);
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            final MetaObject obj = MetaObject.obtainMetaObject(this.stream);
            graph.get(this.stream).add(this.getUuid());
            if (obj instanceof Stream) {
                final MetaObject typeObj = MetaObject.obtainMetaObject(((Stream)obj).getDataType());
                if (typeObj != null) {
                    graph.get(typeObj.getUuid()).add(this.stream);
                    graph.get(typeObj.getUuid()).add(this.uuid);
                }
            }
            if (this.plan != null) {
                for (final UUID dataSource : this.plan.getDataSources()) {
                    final MetaObject metaObject = MetaObject.obtainMetaObject(dataSource);
                    if (metaObject != null) {
                        if (metaObject.type == EntityType.WASTOREVIEW) {
                            final WAStoreView hdStoreView = (WAStoreView)metaObject;
                            graph.get(hdStoreView.wastoreID).add(this.getUuid());
                        }
                        else {
                            graph.get(dataSource).add(this.getUuid());
                        }
                    }
                }
            }
            return graph;
        }
        
        @Override
        public List<UUID> getDependencies() {
            final List<UUID> deps = new ArrayList<UUID>();
            if (this.stream != null) {
                deps.add(this.stream);
            }
            if (this.plan != null) {
                deps.addAll(this.plan.getDataSources());
            }
            return deps;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " outputStream:" + this.stream + " plan:" + this.plan;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe();
            str += "INSERT INTO ";
            if (this.stream != null && MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, HSecurityManager.TOKEN) != null) {
                str = str + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.stream, HSecurityManager.TOKEN).name + "\n";
            }
            str = str + this.select + "\n";
            this.printDependencyAndStatus();
            return str;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            stream_p.setIsRequired(true);
            final ObjectProperties select_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            select_p.setIsRequired(true);
            CQ.actions.put("output", stream_p);
            CQ.actions.put("select", select_p);
            final ObjectProperties uiconfig_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            uiconfig_p.setIsRequired(false);
            CQ.actions.put("uiconfig", uiconfig_p);
            return CQ.actions;
        }
        
        private void extractInputs(final ArrayNode dataSourceArray, final CQ cq) throws MetaDataRepositoryException {
            final List<CQExecutionPlan.DataSource> dataSourcesList = cq.getPlan().getDataSourceList();
            for (final CQExecutionPlan.DataSource dataSource : dataSourcesList) {
                if (dataSource != null) {
                    if (dataSource.getDataSourceID() == null) {
                        continue;
                    }
                    MetaObject metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(dataSource.getDataSourceID(), HSecurityManager.TOKEN);
                    if (metaObject instanceof WAStoreView) {
                        metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(((WAStoreView)metaObject).getWastoreID(), HSecurityManager.TOKEN);
                    }
                    if (!metaObject.getMetaInfoStatus().isAnonymous()) {
                        dataSourceArray.add(metaObject.getFQN());
                    }
                    else if (metaObject instanceof CQ) {
                        this.extractInputs(dataSourceArray, (CQ)metaObject);
                    }
                    else {
                        dataSourceArray.add(metaObject.getFQN());
                    }
                }
            }
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("select", this.select);
            if (this.uiConfig != null) {
                parent.put("uiconfig", this.jsonMapper.readTree(this.uiConfig));
            }
            try {
                final ArrayNode dataSourceArray = this.jsonMapper.createArrayNode();
                this.extractInputs(dataSourceArray, this);
                parent.set("inputs", (JsonNode)dataSourceArray);
                final MetaObject streamMetaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(this.stream, HSecurityManager.TOKEN);
                parent.put("output", streamMetaObject.getFQN());
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.error((Object)e.getMessage(), (Throwable)e);
            }
            final ArrayNode fNode = this.jsonMapper.createArrayNode();
            for (final String field : this.fieldList) {
                fNode.add(field);
            }
            parent.set("destinationFields", (JsonNode)fNode);
            return parent;
        }
        
        public synchronized void removeClass() {
            final WALoader wal = WALoader.get();
            final String uri = wal.getBundleUri(this.nsName, BundleDefinition.Type.query, this.getName());
            try {
                wal.lockBundle(uri);
                if (wal.isExistingBundle(uri)) {
                    wal.removeBundle(uri);
                }
            }
            finally {
                wal.unlockBundle(uri);
            }
        }
    }
    
    public static class PropertySet extends MetaObject
    {
        private static final long serialVersionUID = 1128712638716283L;
        public Map<String, Object> properties;
        
        public PropertySet() {
            this.properties = null;
        }
        
        public void construct(final String name, final Namespace ns, final Map<String, Object> properties) {
            super.construct(name, ns, EntityType.PROPERTYSET);
            this.properties = properties;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " set:" + this.properties;
        }
        
        @Override
        public String describe(final AuthToken token) {
            return this.startDescribe() + "PROPERTIES = " + this.properties;
        }
        
        public static Map buildActions() {
            return MetaObject.buildBaseActions();
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class PropertyVariable extends MetaObject
    {
        private static final long serialVersionUID = 1128712638719756L;
        public Map<String, Object> properties;
        
        public PropertyVariable() {
            this.properties = null;
        }
        
        public void construct(final String name, final Namespace ns, final Map<String, Object> properties) {
            super.construct(name, ns, EntityType.PROPERTYVARIABLE);
            this.properties = properties;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " set:" + this.properties;
        }
        
        @Override
        public String describe(final AuthToken token) {
            for (final String key : this.properties.keySet()) {
                if (!key.contains("encrypted")) {
                    return this.startDescribe() + "PropertyVariable     : " + key;
                }
            }
            return "Please check if the property variable exists and you have right permissions to describe it";
        }
    }
    
    public static class Source extends MetaObject implements MetaObjectPermissionChecker
    {
        private static final long serialVersionUID = 1901374789247928L;
        public String adapterClassName;
        public UUID outputStream;
        public Map<String, Object> properties;
        public Map<String, Object> parserProperties;
        public List<OutputClause> outputClauses;
        
        public Source() {
            this.outputClauses = new ArrayList<OutputClause>();
            this.adapterClassName = null;
            this.outputStream = null;
            this.properties = null;
            this.parserProperties = null;
        }
        
        public void construct(final String name, final Namespace ns, final String adapterClassName, final Map<String, Object> properties, final Map<String, Object> parserProperties, final UUID outputStream) {
            super.construct(name, ns, EntityType.SOURCE);
            this.adapterClassName = adapterClassName;
            this.properties = properties;
            this.parserProperties = parserProperties;
            this.outputStream = outputStream;
        }
        
        public String getAdapterClassName() {
            return this.adapterClassName;
        }
        
        public void setAdapterClassName(final String adapterClassName) {
            this.adapterClassName = adapterClassName;
        }
        
        public UUID getOutputStream() {
            return this.outputStream;
        }
        
        public void setOutputStream(final UUID outputStream) {
            this.outputStream = outputStream;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        public Map<String, Object> getParserProperties() {
            return this.parserProperties;
        }
        
        public void setParserProperties(final Map<String, Object> parserProperties) {
            this.parserProperties = parserProperties;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " adapter:" + this.adapterClassName + " props:" + this.properties + " parser:" + this.parserProperties + " output:" + this.outputStream;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            PropertyTemplate anno = null;
            try {
                final Class<?> cls = Class.forName(this.adapterClassName, false, ClassLoader.getSystemClassLoader());
                anno = cls.getAnnotation(PropertyTemplate.class);
            }
            catch (ClassNotFoundException e1) {
                MetaInfo.logger.error((Object)"Unexpected class not found", (Throwable)e1);
            }
            String str = this.startDescribe() + "USING " + anno.name() + (anno.version().equals("0.0.0") ? "" : (" VERSION " + anno.version())) + " WITH PROPERTIES ";
            str += "(\n";
            str += this.prettyPrintMap(this.properties);
            str += ")\n";
            if (this.parserProperties != null && !this.parserProperties.isEmpty()) {
                str += " WITH PARSER PROPERTIES ";
                str += "(\n";
                str += this.prettyPrintMap(this.parserProperties);
                str += ")\n";
            }
            if (this.outputClauses != null) {
                for (final OutputClause outputClause : this.outputClauses) {
                    str = str + "OUTPUTS TO STREAM " + outputClause + "\n";
                }
            }
            else {
                str += "OUTPUTS TO STREAM ";
                str += MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.outputStream, HSecurityManager.TOKEN).name;
            }
            return str;
        }
        
        @Override
        public List<UUID> getDependencies() {
            final List<UUID> result = new CopyOnWriteArrayList<UUID>();
            result.add(this.outputStream);
            result.addAll(this.coDependentObjects);
            return result;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            super.inEdges(graph);
            graph.get(this.getUuid()).add(this.outputStream);
            return graph;
        }
        
        public List<OutputClause> getOutputClauses() {
            return this.outputClauses;
        }
        
        public void setOutputClauses(final List<OutputClause> outputClauses) {
            final List ll = new ArrayList();
            ll.addAll(outputClauses);
            this.outputClauses = (List<OutputClause>)ll;
        }
        
        public void injectOutputClauses(final List<InputOutputSink> outputClauses) {
            final List ll = new ArrayList();
            ll.addAll(outputClauses);
            this.outputClauses = (List<OutputClause>)ll;
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            final MetaObject obj = MetaObject.obtainMetaObject(this.outputStream);
            if (obj.getMetaInfoStatus().isGenerated()) {
                graph.get(this.getUuid()).add(this.outputStream);
            }
            else {
                graph.get(this.outputStream).add(this.getUuid());
            }
            return graph;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        @Override
        public boolean checkPermissionForMetaPropertyVariable(final AuthToken token) throws MetaDataRepositoryException {
            final String namespace = this.getNsName();
            final Map<String, Object> readerProperties = this.getProperties();
            final Map<String, Object> parserProperties = this.getParserProperties();
            return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, readerProperties, parserProperties);
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            stream_p.setIsRequired(true);
            final ObjectProperties properties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            properties_p.setIsRequired(true);
            final ObjectProperties parserProperties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            parserProperties_p.setIsRequired(false);
            final ObjectProperties outputClausesProperties = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            outputClausesProperties.setIsRequired(true);
            Source.actions.put("outputStream", stream_p);
            Source.actions.put("adapter", properties_p);
            Source.actions.put("parser", parserProperties_p);
            Source.actions.put("outputclause", outputClausesProperties);
            return Source.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            final Map<String, Object> propertiesCopy = new HashMap<String, Object>(this.properties);
            final StringBuilder tempBuffer = new StringBuilder();
            if (this.adapterClassName != null) {
                PropertyTemplate anno = null;
                try {
                    final Class cls = Class.forName(this.adapterClassName, false, ClassLoader.getSystemClassLoader());
                    anno = (PropertyTemplate)cls.getAnnotation(PropertyTemplate.class);
                }
                catch (Exception e1) {
                    e1.printStackTrace();
                }
                final ObjectNode adapterProps = this.jsonMapper.createObjectNode();
                final String simpleHandlerName = (String)this.properties.get("adapterName");
                tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
                if (anno != null && anno.version() != null && !"0.0.0".equals(anno.version())) {
                    tempBuffer.append("_").append(anno.version());
                }
                adapterProps.put("handler", tempBuffer.toString());
                final JsonNode node = (JsonNode)this.jsonMapper.convertValue((Object)propertiesCopy, (Class)JsonNode.class);
                adapterProps.set("properties", node);
                parent.set("adapter", (JsonNode)adapterProps);
                tempBuffer.setLength(0);
            }
            final Map parserPropertiesCopy = new HashMap(this.parserProperties);
            final String simpleHandlerNameInParser = (String)parserPropertiesCopy.get("parserName");
            if (simpleHandlerNameInParser != null) {
                final ObjectNode parserProps = this.jsonMapper.createObjectNode();
                tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
                parserProps.put("handler", tempBuffer.toString());
                final JsonNode node = (JsonNode)this.jsonMapper.convertValue((Object)parserPropertiesCopy, (Class)JsonNode.class);
                parserProps.set("properties", node);
                parent.set("parser", (JsonNode)parserProps);
                tempBuffer.setLength(0);
            }
            if (this.outputStream != null) {
                try {
                    final MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.outputStream, HSecurityManager.TOKEN);
                    if (streamInfo != null) {
                        parent.put("outputStream", streamInfo.getFQN());
                    }
                    else {
                        parent.putNull("outputStream");
                    }
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("outputStream");
            }
            if (this.outputClauses != null) {
                final ArrayNode outputClausesJSON = this.jsonMapper.createArrayNode();
                for (final OutputClause outputClause : this.outputClauses) {
                    final ObjectNode outputClauseNode = this.jsonMapper.createObjectNode();
                    if (outputClause.getStreamName() != null) {
                        outputClauseNode.put("outputStream", this.convertNameToFullQualifiedName(this.nsName, outputClause.getStreamName(), EntityType.STREAM));
                    }
                    if (outputClause.getFilterText() != null) {
                        outputClauseNode.put("select", outputClause.getFilterText());
                    }
                    if (outputClause.getTypeDefinition() != null) {
                        final ArrayNode fieldArray = this.jsonMapper.createArrayNode();
                        for (final TypeField typeField : outputClause.getTypeDefinition()) {
                            final ObjectNode fieldNode = this.jsonMapper.createObjectNode();
                            fieldNode.put("name", typeField.fieldName);
                            fieldNode.put("type", typeField.fieldType.name);
                            fieldNode.put("isKey", typeField.isPartOfKey);
                            fieldArray.add((JsonNode)fieldNode);
                        }
                        outputClauseNode.set("fields", (JsonNode)fieldArray);
                    }
                    if (outputClause.getGeneratedStream() != null) {
                        final ObjectNode mappingNode = this.jsonMapper.createObjectNode();
                        final Map mappingPropertiesCopy = new HashMap(outputClause.getGeneratedStream().mappingProperties);
                        mappingNode.put("streamName", this.convertNameToFullQualifiedName(this.nsName, outputClause.getGeneratedStream().streamName, EntityType.STREAM));
                        final JsonNode node2 = (JsonNode)this.jsonMapper.convertValue((Object)mappingPropertiesCopy, (Class)JsonNode.class);
                        mappingNode.set("mappingProperties", node2);
                        outputClauseNode.set("map", (JsonNode)mappingNode);
                    }
                    outputClausesJSON.add((JsonNode)outputClauseNode);
                }
                parent.set("outputclause", (JsonNode)outputClausesJSON);
            }
            return parent;
        }
    }
    
    public static class Target extends MetaObject implements MetaObjectPermissionChecker
    {
        private static final long serialVersionUID = 1901374789247928L;
        public String adapterClassName;
        public UUID inputStream;
        public Map<String, Object> properties;
        public Map<String, Object> formatterProperties;
        public Map<String, Object> parallelismProperties;
        
        public Target() {
            this.adapterClassName = null;
            this.inputStream = null;
            this.properties = null;
            this.formatterProperties = null;
            this.parallelismProperties = null;
        }
        
        public void construct(final String name, final Namespace ns, final String adapterClassName, final Map<String, Object> properties, final Map<String, Object> formatterProperties, final Map parallelismProperties, final UUID inputStream) {
            super.construct(name, ns, EntityType.TARGET);
            this.adapterClassName = adapterClassName;
            this.properties = properties;
            this.formatterProperties = formatterProperties;
            this.parallelismProperties = (Map<String, Object>)parallelismProperties;
            this.inputStream = inputStream;
        }
        
        @JsonIgnore
        public boolean isSubscription() {
            return this.properties.get("isSubscription") != null;
        }
        
        @JsonIgnore
        public String getChannelName() {
            final Map<String, Object> temp = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
            temp.putAll(this.properties);
            return (String)temp.get("channelName");
        }
        
        public String getAdapterClassName() {
            return this.adapterClassName;
        }
        
        public void setAdapterClassName(final String adapterClassName) {
            this.adapterClassName = adapterClassName;
        }
        
        public UUID getInputStream() {
            return this.inputStream;
        }
        
        public void setInputStream(final UUID inputStream) {
            this.inputStream = inputStream;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        public Map<String, Object> getFormatterProperties() {
            return this.formatterProperties;
        }
        
        public void setFormatterProperties(final Map<String, Object> formatterProperties) {
            this.formatterProperties = formatterProperties;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " adapter:" + this.adapterClassName + " props:" + this.properties + " formatter:" + this.formatterProperties + " input:" + this.inputStream;
        }
        
        @Override
        public String startDescribe() {
            String str = super.startDescribe();
            if (this.isSubscription()) {
                str = str.replaceFirst(EntityType.TARGET.name(), "SUBSCRIPTION");
            }
            return str;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            PropertyTemplate anno = null;
            try {
                final Class<?> cls = Class.forName(this.adapterClassName, false, ClassLoader.getSystemClassLoader());
                anno = cls.getAnnotation(PropertyTemplate.class);
            }
            catch (ClassNotFoundException e1) {
                e1.printStackTrace();
            }
            String str = this.startDescribe() + " USING " + anno.name() + ("0.0.0".equals(anno.version()) ? "" : (" VERSION " + anno.version())) + " WITH PROPERTIES ";
            str += "(\n";
            str += this.prettyPrintMap(this.properties);
            str += ")\n";
            if (this.formatterProperties != null && !this.formatterProperties.isEmpty()) {
                str += " WITH FORMATTER PROPERTIES ";
                str += "(\n";
                str += this.prettyPrintMap(this.formatterProperties);
                str += ")\n";
            }
            str = str + "INPUT FROM STREAM " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.inputStream, HSecurityManager.TOKEN).name;
            this.printDependencyAndStatus();
            return str;
        }
        
        @Override
        public List<UUID> getDependencies() {
            return Collections.singletonList(this.inputStream);
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final MetaObjectProperties stream_p = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            stream_p.setIsRequired(true);
            final ObjectProperties properties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            properties_p.setIsRequired(true);
            final ObjectProperties formatterProperties_p = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            formatterProperties_p.setIsRequired(false);
            Target.actions.put("inputStream", stream_p);
            Target.actions.put("adapter", properties_p);
            Target.actions.put("formatter", formatterProperties_p);
            return Target.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            final StringBuilder tempBuffer = new StringBuilder();
            if (this.properties != null) {
                final Map propertiesCopy = new HashMap(this.properties);
                if (this.adapterClassName != null) {
                    PropertyTemplate anno = null;
                    try {
                        final Class cls = Class.forName(this.adapterClassName, false, ClassLoader.getSystemClassLoader());
                        anno = (PropertyTemplate)cls.getAnnotation(PropertyTemplate.class);
                    }
                    catch (Exception e1) {
                        e1.printStackTrace();
                    }
                    final ObjectNode adapterProps = this.jsonMapper.createObjectNode();
                    final String simpleHandlerName = (String)propertiesCopy.get("adapterName");
                    tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
                    if (anno != null && anno.version() != null && !"0.0.0".equals(anno.version())) {
                        tempBuffer.append("_").append(anno.version());
                    }
                    adapterProps.put("handler", tempBuffer.toString());
                    final JsonNode pNode = (JsonNode)this.jsonMapper.convertValue((Object)propertiesCopy, (Class)JsonNode.class);
                    adapterProps.set("properties", pNode);
                    parent.set("adapter", (JsonNode)adapterProps);
                    tempBuffer.setLength(0);
                }
            }
            else {
                parent.putNull("adapter");
            }
            if (this.formatterProperties != null) {
                final Map formatterPropertiesCopy = new HashMap(this.formatterProperties);
                final String simpleHandlerNameInParser = (String)formatterPropertiesCopy.get("formatterName");
                if (simpleHandlerNameInParser != null) {
                    final ObjectNode parserProps = this.jsonMapper.createObjectNode();
                    tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
                    parserProps.put("handler", tempBuffer.toString());
                    final JsonNode fNode = (JsonNode)this.jsonMapper.convertValue((Object)formatterPropertiesCopy, (Class)JsonNode.class);
                    parserProps.set("properties", fNode);
                    parent.set("formatter", (JsonNode)parserProps);
                }
            }
            else {
                parent.putNull("formatter");
            }
            if (this.inputStream != null) {
                try {
                    final MetaObject streamInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.inputStream, HSecurityManager.TOKEN);
                    parent.put("inputStream", streamInfo.getFQN());
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("outputStream");
            }
            return parent;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            graph.get(this.inputStream).add(this.getUuid());
            return super.inEdges(graph);
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            final MetaObject obj = MetaObject.obtainMetaObject(this.inputStream);
            graph.get(this.inputStream).add(this.getUuid());
            return graph;
        }
        
        @Override
        public boolean checkPermissionForMetaPropertyVariable(final AuthToken token) throws MetaDataRepositoryException {
            final String namespace = this.getNsName();
            final Map<String, Object> readerProperties = this.getProperties();
            final Map<String, Object> formatterProperties = this.getFormatterProperties();
            return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, readerProperties, formatterProperties);
        }
        
        public Map<String, Object> getParallelismProperties() {
            return this.parallelismProperties;
        }
    }
    
    public static class Query extends MetaObject
    {
        private static final long serialVersionUID = -7406372777141475415L;
        private boolean adhocQuery;
        public String queryDefinition;
        public UUID appUUID;
        public UUID streamUUID;
        public UUID cqUUID;
        public Map<String, Long> typeInfo;
        public List<String> queryParameters;
        public List<Property> bindParameters;
        public List<QueryManager.QueryProjection> projectionFields;
        
        public Query() {
            this.queryParameters = new ArrayList<String>();
            this.projectionFields = new ArrayList<QueryManager.QueryProjection>();
        }
        
        public Query(final String queryName, final Namespace nameSpace, final UUID appUUID, final UUID streamUUID, final UUID cqUUID, final Map<String, Long> typeInfo, final String queryDefinition, final Boolean isAdhoc) {
            this.queryParameters = new ArrayList<String>();
            this.projectionFields = new ArrayList<QueryManager.QueryProjection>();
            super.construct(queryName, nameSpace, EntityType.QUERY);
            this.adhocQuery = isAdhoc;
            this.appUUID = appUUID;
            this.streamUUID = streamUUID;
            this.cqUUID = cqUUID;
            this.queryDefinition = queryDefinition;
            this.typeInfo = typeInfo;
        }
        
        public boolean isAdhocQuery() {
            return this.adhocQuery;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            json.put("queryDefinition", (Object)this.queryDefinition);
            json.put("appUUID", (Object)this.appUUID);
            json.put("streamUUID", (Object)this.streamUUID);
            json.put("cqUUID", (Object)this.cqUUID);
            json.put("typeInfo", (Map)this.typeInfo);
            json.put("queryParameters", (Collection)this.queryParameters);
            json.put("adhocQuery", this.adhocQuery);
            final List<String> projectionCopy = new ArrayList<String>();
            if (this.projectionFields != null) {
                for (final QueryManager.QueryProjection qp : this.projectionFields) {
                    projectionCopy.add(qp.JSONify().toString());
                }
            }
            json.put("projectionFields", (Collection)projectionCopy);
            return json;
        }
        
        @Override
        public String JSONifyString() {
            try {
                return this.JSONify().toString();
            }
            catch (JSONException e) {
                e.printStackTrace();
                return "{}";
            }
        }
        
        public void setProjectionFields(final List<QueryManager.QueryProjection> projectionFields) {
            this.projectionFields = projectionFields;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe();
            str = str + "Query TQL = " + this.queryDefinition;
            return str;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final TextProperties query_def_properties = MetaInfo.actionablePropertiesFactory.createTextProperties();
            query_def_properties.setIsRequired(true);
            Query.actions.put("queryDefinition", query_def_properties);
            return Query.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            final ArrayNode projectionArray = this.jsonMapper.createArrayNode();
            if (this.projectionFields != null) {
                for (final QueryManager.QueryProjection qp : this.projectionFields) {
                    projectionArray.add((JsonNode)qp.getJsonForClient());
                }
            }
            parent.set("projectionFields", (JsonNode)projectionArray);
            parent.put("queryDefinition", this.queryDefinition);
            final ArrayNode qNode = this.jsonMapper.createArrayNode();
            for (final String param : this.queryParameters) {
                qNode.add(param);
            }
            parent.set("queryParameters", (JsonNode)qNode);
            try {
                this.addFilterZoomRelatedFields(parent);
            }
            catch (Exception e) {
                MetaInfo.logger.warn((Object)e.getMessage(), (Throwable)e);
            }
            final ObjectNode metaInfoStatus = (ObjectNode)parent.get("metaInfoStatus");
            if (metaInfoStatus != null && !metaInfoStatus.get("isAdhoc").asBoolean()) {
                metaInfoStatus.put("isAnonymous", Boolean.TRUE.toString());
            }
            return parent;
        }
        
        void addFilterZoomRelatedFields(final ObjectNode parent) {
            final Flow app = (Flow)MetaObject.obtainMetaObject(this.appUUID);
            if (app != null) {
                final Set<UUID> wastoreviews = app.getObjects().get(EntityType.WASTOREVIEW);
                if (wastoreviews == null || wastoreviews.isEmpty() || wastoreviews.size() > 1) {
                    parent.put("canBeFiltered", false);
                    return;
                }
                if (wastoreviews.size() == 1) {
                    final UUID wastoreviewUUID = wastoreviews.iterator().next();
                    final WAStoreView wastoreview = (WAStoreView)MetaObject.obtainMetaObject(wastoreviewUUID);
                    if (!wastoreview.hasQuery()) {
                        parent.put("canBeFiltered", false);
                    }
                    else {
                        final UUID hdStoreUUID = wastoreview.wastoreID;
                        final HDStore hdStore = (HDStore)MetaObject.obtainMetaObject(hdStoreUUID);
                        final Type hdStoreField = (Type)MetaObject.obtainMetaObject(hdStore.contextType);
                        final ArrayNode filterAttributes = this.jsonMapper.createArrayNode();
                        for (final Map.Entry<String, String> param : hdStoreField.fields.entrySet()) {
                            final QueryManager.QueryProjection queryProjection = new QueryManager.QueryProjection();
                            queryProjection.name = param.getKey();
                            queryProjection.type = param.getValue();
                            filterAttributes.add((JsonNode)queryProjection.getJsonForClient());
                        }
                        parent.put("filterAttributes", (JsonNode)filterAttributes);
                        parent.put("canBeFiltered", true);
                    }
                }
            }
        }
        
        public void setBindParameters(final List<Property> bindParameters) {
            this.bindParameters = bindParameters;
        }
        
        public List<Property> getBindParameters() {
            return this.bindParameters;
        }
        
        public static Query deserialize(final JsonNode jsonNode) throws JsonParseException, JsonMappingException, IOException {
            if (jsonNode == null) {
                return null;
            }
            return MetaInfoJsonSerializer.QueryJsonSerializer.deserialize(jsonNode.toString());
        }
    }
    
    public static class Flow extends MetaObject
    {
        private static final long serialVersionUID = 7905072382713419190L;
        public Set<String> importStatements;
        public Map<EntityType, LinkedHashSet<UUID>> objects;
        public List<Detail> deploymentPlan;
        public int recoveryType;
        public long recoveryPeriod;
        public boolean encrypted;
        public StatusInfo.Status flowStatus;
        public Map<String, Object> ehandlers;
        
        public Flow() {
            this.importStatements = new LinkedHashSet<String>();
            this.encrypted = false;
            this.ehandlers = Maps.newHashMap();
            this.objects = null;
            this.recoveryType = 0;
            this.recoveryPeriod = 0L;
        }
        
        public void construct(final EntityType type, final String name, final Namespace ns, final Map<EntityType, LinkedHashSet<UUID>> objects, final int recoveryType, final long recoveryPeriod) {
            super.construct(name, ns, type);
            this.objects = new HashMap<EntityType, LinkedHashSet<UUID>>();
            if (objects != null) {
                this.objects.putAll(objects);
            }
            this.recoveryType = recoveryType;
            this.recoveryPeriod = recoveryPeriod;
            this.flowStatus = StatusInfo.Status.CREATED;
        }
        
        public boolean isEncrypted() {
            return this.encrypted;
        }
        
        public void setEncrypted(final boolean encrypted) {
            this.encrypted = encrypted;
        }
        
        public void setEhandlers(final Map<String, Object> ehandlers) {
            this.ehandlers = ehandlers;
        }
        
        public Map<String, Object> getEhandlers() {
            return this.ehandlers;
        }
        
        public Map<EntityType, LinkedHashSet<UUID>> getObjects() {
            return this.objects;
        }
        
        public void setObjects(final Map<EntityType, LinkedHashSet<UUID>> objects) {
            this.objects = objects;
        }
        
        public int getRecoveryType() {
            return this.recoveryType;
        }
        
        public void setRecoveryType(final int recoveryType) {
            this.recoveryType = recoveryType;
        }
        
        public long getRecoveryPeriod() {
            return this.recoveryPeriod;
        }
        
        public void setRecoveryPeriod(final long recoveryPeriod) {
            if (recoveryPeriod > 0L) {
                this.recoveryType = 2;
            }
            else {
                this.recoveryType = 0;
            }
            this.recoveryPeriod = recoveryPeriod;
        }
        
        public List<Detail> getDeploymentPlan() {
            return this.deploymentPlan;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " contents:" + this.objects;
        }
        
        public Set<String> getImportStatements() {
            return this.importStatements;
        }
        
        public void setImportStatements(final Set<String> importStatements) {
            this.importStatements = importStatements;
        }
        
        public StatusInfo.Status getFlowStatus() {
            return this.flowStatus;
        }
        
        public void setFlowStatus(final StatusInfo.Status flowStatus) {
            this.flowStatus = flowStatus;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe();
            if (this.recoveryType == 2) {
                str += "  RECOVERY ";
                if (this.recoveryPeriod % 3600L == 0L) {
                    str += String.valueOf(this.recoveryPeriod / 3600L);
                    str += " HOUR INTERVAL\n";
                }
                else if (this.recoveryPeriod % 60L == 0L) {
                    str += String.valueOf(this.recoveryPeriod / 60L);
                    str += " MINUTE INTERVAL\n";
                }
                else {
                    str += String.valueOf(this.recoveryPeriod);
                    str += " SECOND INTERVAL\n";
                }
            }
            str += "ELEMENTS {\n";
            for (final EntityType type : this.getObjectTypes()) {
                for (final UUID uuid : this.getObjects(type)) {
                    if (uuid == null) {
                        continue;
                    }
                    final MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (o == null || o.getMetaInfoStatus().isDropped()) {
                        continue;
                    }
                    str = str + "  " + o.startDescribe();
                }
            }
            str += "}\n";
            if (this.deploymentPlan != null) {
                str += "DEPLOYMENT PLAN {\n";
                for (final Detail d : this.deploymentPlan) {
                    final MetaObject flow = MetadataRepository.getINSTANCE().getMetaObjectByUUID(d.flow, HSecurityManager.TOKEN);
                    final MetaObject group = MetadataRepository.getINSTANCE().getMetaObjectByUUID(d.deploymentGroup, HSecurityManager.TOKEN);
                    str = str + flow.type + " " + flow.name + " " + d.strategy + " IN " + group.name + "\n";
                }
                str += "}\n";
            }
            if (this.type.ordinal() == EntityType.APPLICATION.ordinal()) {
                str = str + "ENCRYPTION: " + this.encrypted + ";\n";
            }
            if (this.ehandlers != null && !this.ehandlers.isEmpty()) {
                str += "EHANDLERS: {\n";
                for (final Map.Entry<String, Object> entry : this.ehandlers.entrySet()) {
                    str = str + "  " + entry.getKey() + ": " + entry.getValue() + "\n";
                }
                str += "}\n";
            }
            this.printDependencyAndStatus();
            return str;
        }
        
        public void setDeploymentPlan(final List<Detail> deploymentPlan) {
            this.deploymentPlan = deploymentPlan;
        }
        
        public Set<UUID> getObjects(final EntityType type) {
            final Set<UUID> l = this.objects.get(type);
            return (l == null) ? Collections.emptySet() : l;
        }
        
        @JsonIgnore
        public Collection<EntityType> getObjectTypes() {
            return this.objects.keySet();
        }
        
        public void addObject(final EntityType type, final UUID objid) {
            LinkedHashSet<UUID> list = this.objects.get(type);
            if (list == null) {
                list = new LinkedHashSet<UUID>();
                this.objects.put(type, list);
            }
            list.add(objid);
        }
        
        @Override
        public List<UUID> getDependencies() {
            final List<UUID> ret = new ArrayList<UUID>();
            for (final Set<UUID> l : this.objects.values()) {
                ret.addAll(l);
            }
            return ret;
        }
        
        public void setDeepDependencies(final Set<UUID> set) {
        }
        
        public Set<UUID> getDeepDependencies() throws MetaDataRepositoryException {
            final Set<UUID> ret = new HashSet<UUID>();
            final List<Flow> flows = this.getSubFlows();
            for (final Flow flow : flows) {
                ret.addAll(flow.getDependencies());
            }
            for (final Set<UUID> l : this.objects.values()) {
                for (final UUID uuid : l) {
                    final MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (obj != null && !obj.getMetaInfoStatus().isDropped()) {
                        ret.addAll(MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN).getDependencies());
                    }
                }
                ret.addAll(l);
            }
            return ret;
        }
        
        public void setAllObjects(final Set<UUID> set) {
        }
        
        public Set<UUID> getAllObjects() throws MetaDataRepositoryException {
            final Set<UUID> ret = new HashSet<UUID>();
            final List<Flow> flows = this.getSubFlows();
            for (final Flow flow : flows) {
                ret.addAll(flow.getAllObjects());
            }
            for (final Set<UUID> l : this.objects.values()) {
                for (final UUID uuid : l) {
                    final MetaObject metaObjectByUUID = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (metaObjectByUUID != null) {
                        ret.add(metaObjectByUUID.getUuid());
                    }
                }
                ret.addAll(l);
            }
            return ret;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (!nodes.getKey().isFlowComponent()) {
                    continue;
                }
                for (final UUID uuid : nodes.getValue()) {
                    try {
                        final MetaObject appComponent = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                        if (appComponent != null) {
                            appComponent.inEdges(graph);
                        }
                        else {
                            if (!MetaInfo.logger.isInfoEnabled()) {
                                continue;
                            }
                            MetaInfo.logger.info((Object)(nodes.getKey() + " with uuid " + uuid + ", not found on topological sort."));
                        }
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e);
                    }
                }
            }
            return graph;
        }
        
        private Flow findFlow(final UUID uuid) throws MetaDataRepositoryException {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> ii : this.objects.entrySet()) {
                if (ii.getKey() == EntityType.FLOW) {
                    continue;
                }
                if (ii.getValue().contains(uuid)) {
                    return this;
                }
            }
            final List<Flow> flows = this.getSubFlows();
            for (final Flow flow : flows) {
                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> ii2 : flow.objects.entrySet()) {
                    if (ii2.getKey() == EntityType.FLOW) {
                        continue;
                    }
                    if (ii2.getValue().contains(uuid)) {
                        return flow;
                    }
                }
            }
            return null;
        }
        
        public List<Pair<MetaObject, Flow>> exportTQL(final MetadataRepository metadataRepository, final AuthToken authToken) throws Exception {
            if (metadataRepository == null || authToken == null) {
                throw new RuntimeException((metadataRepository == null) ? "MetadataRepository" : "Authtokennot initialized, cannot export TQL without it");
            }
            final Set<UUID> allOfTheObjects = this.getAllObjects();
            final Graph<UUID, Set<UUID>> applicationGraph = new Graph<UUID, Set<UUID>>();
            for (final UUID metaObjectUUID : allOfTheObjects) {
                final MetaObject metaObject = MetaObject.obtainMetaObject(metaObjectUUID);
                if (metaObject.getMetaInfoStatus().isAnonymous()) {
                    if (!MetaInfo.logger.isInfoEnabled()) {
                        continue;
                    }
                    MetaInfo.logger.info((Object)("Skipping object " + metaObject.getFullName() + " as they are anonymous"));
                }
                else {
                    metaObject.exportOrder(applicationGraph);
                }
            }
            if (MetaInfo.logger.isDebugEnabled()) {
                final StringBuilder graphOrdering = new StringBuilder();
                for (final Map.Entry<UUID, Set<UUID>> graphEntry : applicationGraph.entrySet()) {
                    final MetaObject mo = metadataRepository.getMetaObjectByUUID(graphEntry.getKey(), authToken);
                    graphOrdering.append("FROM ").append(mo.getFullName()).append(" TO ");
                    for (final UUID outedgeObjectUUID : graphEntry.getValue()) {
                        final MetaObject outedgeObject = MetaObject.obtainMetaObject(outedgeObjectUUID);
                        graphOrdering.append(" ").append(outedgeObject.getFullName());
                    }
                    graphOrdering.append("\n");
                }
                MetaInfo.logger.debug((Object)("Graph topology is " + (Object)graphOrdering));
            }
            final List<UUID> orderOfObjects = GraphUtility.topologicalSort(applicationGraph);
            final Set<UUID> objectsBelongToCurrentApplication = this.getAllObjects();
            final List<Pair<MetaObject, Flow>> orderWithFlowInfo = new ArrayList<Pair<MetaObject, Flow>>();
            for (final UUID metaObjectUUID2 : orderOfObjects) {
                final Flow flow = this.findFlow(metaObjectUUID2);
                if (flow == null) {
                    if (!MetaInfo.logger.isInfoEnabled()) {
                        continue;
                    }
                    MetaInfo.logger.info((Object)"Skipping the component as it doesn't belong to this app/flow.");
                }
                else {
                    if (!objectsBelongToCurrentApplication.contains(metaObjectUUID2)) {
                        continue;
                    }
                    orderWithFlowInfo.add(Pair.make(MetaObject.obtainMetaObject(metaObjectUUID2), flow));
                }
            }
            return orderWithFlowInfo;
        }
        
        public List<Pair<MetaObject, Flow>> showTopology() throws Exception {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (nodes.getKey() != EntityType.FLOW) {
                    continue;
                }
                for (final UUID flowUUID : nodes.getValue()) {
                    Flow flow = null;
                    try {
                        flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, HSecurityManager.TOKEN);
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e.getMessage());
                    }
                    flow.inEdges(graphF);
                }
            }
            final List<UUID> metaObjectOrder = GraphUtility.topologicalSort(graphF);
            final List<Pair<MetaObject, Flow>> orderWithFlowInfo = new ArrayList<Pair<MetaObject, Flow>>();
            final Set<UUID> X = this.getAllObjects();
            for (final UUID uuid : metaObjectOrder) {
                final Flow flow2 = this.findFlow(uuid);
                if (X.contains(uuid)) {
                    final List<Pair<MetaObject, Flow>> list = orderWithFlowInfo;
                    new Pair();
                    list.add(Pair.make(MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN), flow2));
                }
            }
            return orderWithFlowInfo;
        }
        
        @JsonIgnore
        public Set<MetaObject> getStartingPoints() throws Exception {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (nodes.getKey() != EntityType.FLOW) {
                    continue;
                }
                for (final UUID flowUUID : nodes.getValue()) {
                    Flow flow = null;
                    try {
                        flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, HSecurityManager.TOKEN);
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e.getMessage());
                    }
                    flow.inEdges(graphF);
                }
            }
            final Set<UUID> startingPointUUIDs = GraphUtility.nodesWithNoIncomingEdges(graphF);
            final Set<MetaObject> result = new HashSet<MetaObject>();
            for (final UUID uuid : startingPointUUIDs) {
                try {
                    final MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (mo != null) {
                        result.add(mo);
                    }
                    else {
                        MetaInfo.logger.warn((Object)("Did not find MetaObject when determining starting points: " + uuid));
                    }
                }
                catch (MetaDataRepositoryException e2) {
                    MetaInfo.logger.warn((Object)e2.getMessage());
                }
            }
            return result;
        }
        
        @JsonIgnore
        public Set<MetaObject> getSemanticSources() throws Exception {
            final Set<MetaObject> startingPoints = this.getStartingPoints();
            final Set<MetaObject> result = new HashSet<MetaObject>();
            for (final MetaObject startingPoint : startingPoints) {
                if (startingPoint instanceof Source) {
                    result.add(startingPoint);
                }
                else {
                    if (startingPoint.getType() != EntityType.STREAM || ((Stream)startingPoint).pset == null) {
                        continue;
                    }
                    result.add(startingPoint);
                }
            }
            return result;
        }
        
        public Map<UUID, Integer> getInDegrees() {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            final Map<UUID, Integer> inDegrees = GraphUtility.calculateInDegree(graphF);
            return inDegrees;
        }
        
        public Set<UUID> getInputs(final UUID targetUuid) {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (nodes.getKey() != EntityType.FLOW) {
                    continue;
                }
                for (final UUID flowUUID : nodes.getValue()) {
                    Flow flow = null;
                    try {
                        flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, HSecurityManager.TOKEN);
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e.getMessage());
                    }
                    flow.inEdges(graphF);
                }
            }
            final Set<UUID> result = new HashSet<UUID>();
            for (final Map.Entry<UUID, Set<UUID>> entry : graphF.entrySet()) {
                if (entry.getValue().contains(targetUuid)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }
        
        public void findAllPaths(final Set<MetaObject> startingPoints, final Set<MetaObject> endPoints) {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (nodes.getKey() != EntityType.FLOW) {
                    continue;
                }
                for (final UUID flowUUID : nodes.getValue()) {
                    Flow flow = null;
                    try {
                        flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, HSecurityManager.TOKEN);
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e.getMessage());
                    }
                    flow.inEdges(graphF);
                }
            }
            for (final MetaObject start : startingPoints) {
                for (final MetaObject end : endPoints) {
                    System.out.println("============================================================");
                    System.out.println("From component " + start.getFQN() + " to " + end.getFQN() + "\n");
                    GraphUtility.findAllPaths(graphF, 0, start.getUuid(), end.getUuid());
                    System.out.println("End of all the paths from component " + start.getFQN() + " to " + end.getFQN());
                    System.out.println("============================================================");
                    System.out.println("\n\n");
                }
            }
        }
        
        @JsonIgnore
        public Set<MetaObject> getEndPoints() throws Exception {
            final Graph<UUID, Set<UUID>> graphF = new Graph<UUID, Set<UUID>>();
            this.inEdges(graphF);
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> nodes : this.objects.entrySet()) {
                if (nodes.getKey() != EntityType.FLOW) {
                    continue;
                }
                for (final UUID flowUUID : nodes.getValue()) {
                    Flow flow = null;
                    try {
                        flow = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, HSecurityManager.TOKEN);
                    }
                    catch (MetaDataRepositoryException e) {
                        MetaInfo.logger.warn((Object)e.getMessage());
                    }
                    flow.inEdges(graphF);
                }
            }
            final Set<UUID> startingPointUUIDs = GraphUtility.nodesWithNoOutgoingEdges(graphF);
            final Set<MetaObject> result = new HashSet<MetaObject>();
            for (final UUID uuid : startingPointUUIDs) {
                try {
                    final MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (mo != null) {
                        result.add(mo);
                    }
                    else {
                        MetaInfo.logger.warn((Object)("Did not find MetaObject when determining starting points: " + uuid));
                    }
                }
                catch (MetaDataRepositoryException e2) {
                    MetaInfo.logger.warn((Object)e2.getMessage());
                }
            }
            return result;
        }
        
        @JsonIgnore
        public List<Flow> getSubFlows() throws MetaDataRepositoryException {
            final List<Flow> subflows = new ArrayList<Flow>();
            for (final UUID flowid : this.getObjects(EntityType.FLOW)) {
                final Flow f = (Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowid, HSecurityManager.TOKEN);
                if (f != null) {
                    subflows.add(f);
                    subflows.addAll(f.getSubFlows());
                }
            }
            return subflows;
        }
        
        @JSON(include = false)
        @JsonIgnore
        public boolean isDeployed() {
            return this.deploymentPlan != null;
        }
        
        public void validate() throws MetaDataRepositoryException {
            final Map<UUID, Flow> map = new HashMap<UUID, Flow>();
            for (final UUID id : this.getDependencies()) {
                final Flow old = map.put(id, this);
                assert old == null;
            }
            final List<Flow> subflows = this.getSubFlows();
            for (final Flow f : subflows) {
                for (final UUID id2 : f.getDependencies()) {
                    final Flow old2 = map.put(id2, f);
                    if (old2 != null) {
                        final MetaObject o = MetadataRepository.getINSTANCE().getMetaObjectByUUID(id2, HSecurityManager.TOKEN);
                        if (o.type == EntityType.TYPE) {
                            continue;
                        }
                        if (o.type == EntityType.STREAM) {
                            continue;
                        }
                        throw new RuntimeException(o + " duplicated in flows :" + old2.name + " and " + f.name);
                    }
                }
            }
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            final JSONObject objectsJ = new JSONObject();
            final Map<EntityType, LinkedHashSet<UUID>> objects = this.getObjects();
            for (final EntityType key : objects.keySet()) {
                final LinkedHashSet<UUID> def = objects.get(key);
                final JSONArray uuidArray = new JSONArray();
                for (final UUID u : def) {
                    uuidArray.put((Object)u.getUUIDString());
                }
                objectsJ.put(key.toString(), (Object)uuidArray);
            }
            json.put("objects", (Object)objectsJ);
            return json;
        }
        
        public static Map buildActions() {
            return MetaObject.buildBaseActions();
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            String status = "UNKNOWN";
            StatusInfo statusInfo = null;
            try {
                statusInfo = FlowUtil.getCurrentStatus(this.uuid);
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.error((Object)("Failed to get status for app " + this.getName() + " with exception " + e.getMessage()));
            }
            if (statusInfo != null) {
                status = statusInfo.getStatus().toString();
            }
            parent.put("flowStatus", status);
            final ObjectNode obb = this.jsonMapper.createObjectNode();
            if (this.objects != null) {
                for (final Map.Entry<EntityType, LinkedHashSet<UUID>> entries : this.objects.entrySet()) {
                    final LinkedHashSet<UUID> values = entries.getValue();
                    final ArrayNode objectsInFlow = this.jsonMapper.createArrayNode();
                    for (final UUID uuid : values) {
                        try {
                            final MetaObject metaObject = MetaInfo.metadataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                            if (metaObject == null || metaObject.getMetaInfoStatus().isDropped()) {
                                continue;
                            }
                            objectsInFlow.add(metaObject.getFQN());
                        }
                        catch (MetaDataRepositoryException e2) {
                            MetaInfo.logger.error((Object)e2.getMessage(), (Throwable)e2);
                        }
                    }
                    obb.set(entries.getKey().name(), (JsonNode)objectsInFlow);
                }
                parent.set("objects", (JsonNode)obb);
            }
            else {
                parent.putNull("objects");
            }
            parent.put("recoveryPeriod", this.recoveryPeriod);
            parent.put("recoveryType", this.recoveryType);
            if (this.ehandlers != null && !this.ehandlers.isEmpty()) {
                final ObjectNode eHandlersNode = this.jsonMapper.createObjectNode();
                for (final ExceptionType exceptionType : ExceptionType.values()) {
                    final String exceptionName = exceptionType.name();
                    final String handlerValue = (this.ehandlers.get(exceptionName) != null) ? (String)this.ehandlers.get(exceptionName) : null;
                    eHandlersNode.put(exceptionName, handlerValue);
                }
                parent.set("eHandlers", (JsonNode)eHandlersNode);
            }
            if (this.importStatements != null) {
                final ArrayNode importStatementsList = this.jsonMapper.createArrayNode();
                for (final String importStatement : this.importStatements) {
                    importStatementsList.add(importStatement);
                }
                parent.set("importStatements", (JsonNode)importStatementsList);
            }
            parent.put("encrypted", this.encrypted);
            List<Flow> subflows;
            try {
                subflows = this.getSubFlows();
            }
            catch (MetaDataRepositoryException ex) {
                subflows = Collections.emptyList();
            }
            final ArrayNode deployment_detail_array = this.jsonMapper.createArrayNode();
            if (this.deploymentPlan != null) {
                for (final Detail detail : this.deploymentPlan) {
                    final ObjectNode node = this.jsonMapper.createObjectNode();
                    node.put("strategy", detail.strategy.name());
                    node.put("failoverRule", detail.failOverRule.name());
                    try {
                        final DeploymentGroup dg = (DeploymentGroup)MetaInfo.metadataRepository.getMetaObjectByUUID(detail.deploymentGroup, HSecurityManager.TOKEN);
                        node.put("dg", dg.getFQN());
                        final Flow flow_metaObject = (Flow)MetaInfo.metadataRepository.getMetaObjectByUUID(detail.flow, HSecurityManager.TOKEN);
                        node.put("flowName", flow_metaObject.getFQN());
                    }
                    catch (MetaDataRepositoryException e2) {
                        MetaInfo.logger.error((Object)("The deployment detail returned will have some missing info because of: " + e2.getMessage()));
                    }
                    deployment_detail_array.add((JsonNode)node);
                }
            }
            for (final Flow subflow : subflows) {
                boolean subflowMissing = true;
                if (this.deploymentPlan != null) {
                    final List<Detail> temp = new ArrayList<Detail>();
                    for (final Detail detail2 : this.deploymentPlan) {
                        if (subflow.getUuid().equals((Object)detail2.getFlow())) {
                            temp.add(detail2);
                        }
                    }
                    subflowMissing = (temp.size() == 0);
                }
                if (subflowMissing) {
                    final ObjectNode node2 = this.jsonMapper.createObjectNode();
                    node2.put("strategy", DeploymentStrategy.ON_ALL.name());
                    node2.put("failoverRule", Detail.FailOverRule.NONE.name());
                    node2.put("dg", "Global.DG.default");
                    node2.put("flowName", subflow.getFQN());
                    deployment_detail_array.add((JsonNode)node2);
                }
            }
            parent.set("deploymentInfo", (JsonNode)deployment_detail_array);
            boolean containsEventTables = false;
            final Set<UUID> caches = this.objects.get(EntityType.CACHE);
            if (caches != null) {
                final Iterator<UUID> cachesIt = caches.iterator();
                while (cachesIt.hasNext() && !containsEventTables) {
                    final UUID cacheUuid = cachesIt.next();
                    try {
                        final Cache c = (Cache)MetaInfo.metadataRepository.getMetaObjectByUUID(cacheUuid, HSecurityManager.TOKEN);
                        containsEventTables |= c.isEventTable();
                    }
                    catch (MetaDataRepositoryException e3) {
                        throw new RuntimeException(e3.getMessage());
                    }
                }
            }
            parent.put("isFlowDesignerCompatible", !containsEventTables);
            return parent;
        }
        
        public static class Detail implements Serializable
        {
            private static final long serialVersionUID = 2589595630815708124L;
            public DeploymentStrategy strategy;
            public UUID deploymentGroup;
            public UUID flow;
            public FailOverRule failOverRule;
            
            public Detail() {
                this.failOverRule = FailOverRule.NONE;
            }
            
            public void construct(final DeploymentStrategy strategy, final UUID dg, final UUID flow, final FailOverRule failOverRule) {
                this.strategy = strategy;
                this.deploymentGroup = dg;
                this.flow = flow;
                this.failOverRule = failOverRule;
            }
            
            public DeploymentStrategy getStrategy() {
                return this.strategy;
            }
            
            public void setStrategy(final DeploymentStrategy strategy) {
                this.strategy = strategy;
            }
            
            public void setFailOverRule(final FailOverRule failOverRule) {
                this.failOverRule = failOverRule;
            }
            
            public UUID getDeploymentGroup() {
                return this.deploymentGroup;
            }
            
            public void setDeploymentGroup(final UUID deploymentGroup) {
                this.deploymentGroup = deploymentGroup;
            }
            
            public UUID getFlow() {
                return this.flow;
            }
            
            public void setFlow(final UUID flow) {
                this.flow = flow;
            }
            
            public enum FailOverRule
            {
                AUTO, 
                MANUAL, 
                NONE;
            }
        }
    }
    
    public static class PropertyDef implements Serializable
    {
        private static final long serialVersionUID = -6231159418970255777L;
        public boolean required;
        public Class<?> type;
        public String defaultValue;
        public String label;
        public String description;
        
        public void construct(final boolean required, final Class<?> type, final String defaultValue, final String label, final String description) {
            this.required = required;
            this.type = type;
            this.defaultValue = defaultValue;
            this.label = label;
            this.description = description;
        }
        
        public boolean isRequired() {
            return this.required;
        }
        
        public void setRequired(final boolean required) {
            this.required = required;
        }
        
        public Class<?> getType() {
            return this.type;
        }
        
        public void setType(final Class<?> type) {
            this.type = type;
        }
        
        public String getDefaultValue() {
            return this.defaultValue;
        }
        
        public void setDefaultValue(final String defaultValue) {
            this.defaultValue = defaultValue;
        }
        
        @Override
        public String toString() {
            return this.type.getCanonicalName() + " default " + this.defaultValue + (this.required ? " required" : " optional");
        }
        
        public boolean equals(final PropertyDef that) {
            return that != null && this.required == that.required && this.type.toString().equals(that.type.toString()) && this.defaultValue.equals(that.defaultValue) && this.label.equals(that.label) && this.description.equals(that.description);
        }
        
        @JsonIgnore
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
            final ObjectNode parent = jsonMapper.createObjectNode();
            parent.put("required", this.required);
            parent.put("type", this.type.getCanonicalName());
            parent.put("defaultValue", this.defaultValue);
            if (this.type == Enum.class || this.type.isEnum()) {
                parent.put("type", "Enum");
                List<String> values = null;
                values = (List<String>)com.datasphere.runtime.utils.StringUtils.getEnumValuesAsList(this.type.getName());
                final ArrayNode array = (ArrayNode)jsonMapper.valueToTree((Object)values);
                parent.putArray("values").addAll(array);
            }
            return parent;
        }
        
        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(this.required).append((Object)this.type.toString()).append((Object)this.defaultValue).toHashCode();
        }
    }
    
    public static class PropertyTemplateInfo extends MetaObject
    {
        private static final long serialVersionUID = 699451660431760677L;
        public AdapterType adapterType;
        public String className;
        public String inputClassName;
        public String outputClassName;
        public Map<String, PropertyDef> propertyMap;
        private transient Class<?> inputClass;
        private transient Class<?> outputClass;
        public boolean requiresParser;
        public boolean requiresFormatter;
        public String adapterVersion;
        public boolean isParallelizable;
        
        public PropertyTemplateInfo() {
            this.inputClass = null;
            this.outputClass = null;
            this.adapterType = AdapterType.notset;
            this.propertyMap = null;
            this.className = null;
            this.inputClassName = null;
            this.outputClassName = null;
            this.requiresParser = false;
            this.requiresFormatter = false;
            this.adapterVersion = null;
            this.isParallelizable = false;
        }
        
        public void construct(final String name, final AdapterType adapterType, final Map<String, PropertyDef> propertyMap, final String inputClassName, final String outputClassName, final String className, final boolean requiresParser, final boolean requiresFormatter, final String version, final boolean isParallelizable) {
            super.construct(name, MetaInfo.GlobalNamespace, EntityType.PROPERTYTEMPLATE);
            this.adapterType = adapterType;
            this.propertyMap = propertyMap;
            this.className = className;
            this.inputClassName = inputClassName;
            this.outputClassName = outputClassName;
            this.requiresParser = requiresParser;
            this.requiresFormatter = requiresFormatter;
            this.adapterVersion = version;
            this.isParallelizable = isParallelizable;
        }
        
        public AdapterType getAdapterType() {
            return this.adapterType;
        }
        
        public void setAdapterType(final AdapterType adapterType) {
            this.adapterType = adapterType;
        }
        
        public String getClassName() {
            return this.className;
        }
        
        public void setClassName(final String className) {
            this.className = className;
        }
        
        public String getInputClassName() {
            return this.inputClassName;
        }
        
        public void setInputClassName(final String inputClassName) {
            this.inputClassName = inputClassName;
        }
        
        public String getOutputClassName() {
            return this.outputClassName;
        }
        
        public void setOutputClassName(final String outputClassName) {
            this.outputClassName = outputClassName;
        }
        
        public boolean isRequiresParser() {
            return this.requiresParser;
        }
        
        public boolean isRequiresFormatter() {
            return this.requiresFormatter;
        }
        
        public void setRequiresParser(final boolean requiresParser) {
            this.requiresParser = requiresParser;
        }
        
        public void setRequiresFormatter(final boolean requiresFormatter) {
            this.requiresFormatter = requiresFormatter;
        }
        
        public void setPropertyMap(final Map<String, PropertyDef> propertyMap) {
            this.propertyMap = propertyMap;
        }
        
        public Map<String, PropertyDef> getPropertyMap() {
            return this.propertyMap;
        }
        
        public String getAdapterVersion() {
            return this.adapterVersion.trim();
        }
        
        @JSON(include = false)
        @JsonIgnore
        public Class<?> getInputClass() {
            if (this.inputClass == null) {
                try {
                    this.inputClass = ClassLoader.getSystemClassLoader().loadClass(this.inputClassName);
                }
                catch (ClassNotFoundException e) {
                    MetaInfo.logger.error((Object)("Problem loading input class " + this.inputClassName + " for Property Template " + this.name));
                }
            }
            return this.inputClass;
        }
        
        @JSON(include = false)
        @JsonIgnore
        public Class<?> getOutputClass() {
            if (this.outputClass == null) {
                try {
                    this.outputClass = ClassLoader.getSystemClassLoader().loadClass(this.outputClassName);
                }
                catch (ClassNotFoundException e) {
                    MetaInfo.logger.error((Object)("Problem loading output class " + this.outputClassName + " for Property Template " + this.name));
                }
            }
            return this.outputClass;
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " type:" + this.adapterType + " version:" + this.adapterVersion + " class:" + this.className + " props:" + this.propertyMap + " inputType:" + this.inputClassName + " outputType:" + this.outputClassName;
        }
        
        @Override
        public String describe(final AuthToken token) {
            String str = this.startDescribe() + "FOR " + this.adapterType + ((this.adapterVersion != null) ? (" OF VERSION " + this.adapterVersion) : "") + " IMPLEMENTED BY " + this.className + "\n";
            if (!this.getInputClass().equals(NotSet.class)) {
                str = str + "  GENERATES EVENTS OF TYPE " + this.inputClassName + "\n";
            }
            if (!this.getOutputClass().equals(NotSet.class)) {
                str = str + "  CONSUMES EVENTS OF TYPE " + this.outputClassName + "\n";
            }
            str += "  SETTABLE PROPERTIES (\n";
            for (final Map.Entry<String, PropertyDef> entry : this.propertyMap.entrySet()) {
                final String key = entry.getKey();
                final PropertyDef def = entry.getValue();
                final Object value = def.defaultValue;
                str = str + "   " + key + " (" + def.type + "): default \"";
                if (!key.equals("directory")) {
                    str += StringEscapeUtils.escapeJava(value.toString());
                }
                else {
                    str += value;
                }
                str = str + "\" " + (def.required ? " REQUIRED" : " optional") + "\n";
            }
            str += "  )";
            return str;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            json.put("className", (Object)this.getClassName());
            json.put("adapterType", (Object)this.getAdapterType());
            json.put("requiresParser", this.requiresParser);
            final Map<String, PropertyDef> propertyMap = this.getPropertyMap();
            final JSONObject propertyMapJ = new JSONObject();
            for (final String key : propertyMap.keySet()) {
                final PropertyDef def = propertyMap.get(key);
                final String type = def.getType().getSimpleName();
                if (type != null) {
                    propertyMapJ.put("type", (Object)type);
                }
                else {
                    propertyMapJ.put("type", (Object)def.getType().getName());
                }
                propertyMapJ.put("required", def.required);
                propertyMapJ.put("defaultValue", (Object)def.getDefaultValue());
            }
            json.put("propertyMap", (Object)propertyMapJ);
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("adapterType", this.adapterType.toString());
            parent.put("requiresParser", this.requiresParser);
            parent.put("requiresFormatter", this.requiresFormatter);
            if (this.adapterVersion != null) {
                parent.put("adapterVersion", this.adapterVersion);
            }
            else {
                parent.putNull("adapterVersion");
            }
            final ArrayNode propertyList = this.jsonMapper.createArrayNode();
            if (this.propertyMap != null) {
                for (final Map.Entry<String, PropertyDef> entry : this.propertyMap.entrySet()) {
                    final ObjectNode temp = entry.getValue().getJsonForClient();
                    temp.put("name", (String)entry.getKey());
                    propertyList.add((JsonNode)temp);
                }
                parent.set("propertyMap", (JsonNode)propertyList);
            }
            else {
                parent.putNull("propertyMap");
            }
            return parent;
        }
    }
    
    public static class ShowStream implements Serializable
    {
        private static final long serialVersionUID = 4120058926908733541L;
        public boolean show;
        public int line_count;
        public UUID stream_name;
        public UUID session_id;
        public boolean isTungsten;
        
        public void construct(final boolean show, final int line_count, final UUID stream_name, final UUID session_id) {
            this.show = show;
            this.line_count = line_count;
            this.stream_name = stream_name;
            this.session_id = session_id;
            this.isTungsten = true;
        }
        
        public void construct(final boolean show, final int line_count, final UUID stream_name, final boolean isTungsten, final UUID session_id) {
            this.show = show;
            this.line_count = line_count;
            this.stream_name = stream_name;
            this.session_id = session_id;
            this.isTungsten = isTungsten;
        }
        
        public boolean isShow() {
            return this.show;
        }
        
        public void setShow(final boolean show) {
            this.show = show;
        }
        
        public int getLine_count() {
            return this.line_count;
        }
        
        public void setLine_count(final int line_count) {
            this.line_count = line_count;
        }
        
        public UUID getStream_name() {
            return this.stream_name;
        }
        
        public void setStream_name(final UUID stream_name) {
            this.stream_name = stream_name;
        }
        
        public UUID getSession_id() {
            return this.session_id;
        }
        
        public void setSession_id(final UUID session_id) {
            this.session_id = session_id;
        }
        
        @Override
        public String toString() {
            return " show:" + this.show + " line_count:" + this.line_count + " stream_name: " + this.stream_name;
        }
        
        public com.datasphere.runtime.components.Stream getLocalStreamRuntime() throws MetaDataRepositoryException {
            final Stream stream_mo = (Stream)MetaInfo.metadataRepository.getMetaObjectByUUID(this.stream_name, HSecurityManager.TOKEN);
            final Set<UUID> uuids = stream_mo.getReverseIndexObjectDependencies();
            final com.datasphere.runtime.Server server = com.datasphere.runtime.Server.getServer();
            FlowComponent obo = null;
        Label_0327:
            for (final UUID uuid : uuids) {
                final MetaObject mo = MetaInfo.metadataRepository.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                if (mo instanceof CQ) {
                    if (((CQ)mo).getStream().equals((Object)stream_mo.getUuid()) && server.openObjects.containsKey(mo.getUuid())) {
                        obo = server.openObjects.get(mo.getUuid());
                        break;
                    }
                    continue;
                }
                else if (mo.getType().equals(EntityType.SOURCE)) {
                    if (((Source)mo).getOutputStream().equals((Object)stream_mo.getUuid()) && server.openObjects.containsKey(mo.getUuid())) {
                        obo = server.openObjects.get(mo.getUuid());
                        break;
                    }
                    continue;
                }
                else {
                    if (!mo.getType().equals(EntityType.SORTER)) {
                        continue;
                    }
                    final List<Sorter.SorterRule> sorterRules = ((Sorter)mo).inOutRules;
                    for (final Sorter.SorterRule sr : sorterRules) {
                        if (sr.getOutStream().equals((Object)stream_mo.getUuid()) && server.openObjects.containsKey(mo.getUuid())) {
                            obo = server.openObjects.get(mo.getUuid());
                            break Label_0327;
                        }
                    }
                }
            }
            return (obo == null) ? null : obo.getFlow().getStream(stream_mo.getUuid());
        }
    }
    
    public static class StatusInfo implements Serializable
    {
        private static final long serialVersionUID = -8514361580056565663L;
        public static boolean ASYNC;
        public static boolean SYNC;
        public List<UUID> SIDs;
        public UUID OID;
        public Status status;
        public Status previousStatus;
        public EntityType type;
        public String name;
        public List<String> exceptionMessages;
        
        public StatusInfo() {
            this.SIDs = new ArrayList<UUID>(1);
            this.exceptionMessages = new LinkedList<String>();
            this.status = Status.UNKNOWN;
            this.previousStatus = Status.UNKNOWN;
        }
        
        public StatusInfo(final UUID OID, final Status status, final EntityType type, final String name) {
            this(OID, Status.UNKNOWN, status, type, name);
        }
        
        public StatusInfo(final UUID OID, final Status previousState, final Status status, final EntityType type, final String name) {
            this.SIDs = new ArrayList<UUID>(1);
            this.exceptionMessages = new LinkedList<String>();
            this.OID = OID;
            this.previousStatus = previousState;
            this.status = status;
            this.type = type;
            this.name = name;
        }
        
        public void construct(final UUID OID, final Status status, final EntityType type, final String name) {
            this.construct(OID, Status.UNKNOWN, status, type, name);
        }
        
        public void construct(final UUID OID, final Status previousState, final Status status, final EntityType type, final String name) {
            this.OID = OID;
            this.previousStatus = previousState;
            this.status = status;
            this.type = type;
            this.name = name;
        }
        
        public List<UUID> getSIDs() {
            return this.SIDs;
        }
        
        public void setSIDs(final List<UUID> sIDs) {
            this.SIDs = sIDs;
        }
        
        public UUID getOID() {
            return this.OID;
        }
        
        public void setOID(final UUID oID) {
            this.OID = oID;
        }
        
        public Status getStatus() {
            return this.status;
        }
        
        public void setStatus(final Status status) {
            this.status = status;
        }
        
        public EntityType getType() {
            return this.type;
        }
        
        public void setType(final EntityType type) {
            this.type = type;
        }
        
        public String getName() {
            return this.name;
        }
        
        public void setName(final String name) {
            this.name = name;
        }
        
        public List<String> getExceptionMessages() {
            return this.exceptionMessages;
        }
        
        public void setExceptionMessages(final List<String> exceptionMessages) {
            this.exceptionMessages.addAll(exceptionMessages);
        }
        
        public void addExceptionMessage(final String exceptionMessage) {
            this.exceptionMessages.add(exceptionMessage);
        }
        
        public void clearExceptionMessages() {
            this.exceptionMessages.clear();
        }
        
        public Status getPreviousStatus() {
            return this.previousStatus;
        }
        
        public void setPreviousStatus(final Status previousStatus) {
            this.previousStatus = previousStatus;
        }
        
        @Override
        public String toString() {
            return this.type + " " + this.name + " " + this.status + " " + this.previousStatus + " " + this.OID;
        }
        
        public String serializeToString() {
            return serializeToString(this.OID.toString(), this.name, this.type.name(), this.previousStatus.name(), this.status.name());
        }
        
        public static String serializeToString(final String OID, final String name, final String type, final String previousStatus, final String status) {
            return "{ \"OID\": \"" + OID + "\", \"name\": \"" + name + "\", \"type\": \"" + type + "\", \"previousStatus\": \"" + previousStatus + "\", \"currentStatus\": \"" + status + "\" }";
        }
        
        public String serializeToHumanReadableString(final long timestamp) {
            return serializeToHumanReadableString(this.OID.toString(), this.name, this.type.name(), this.previousStatus.name(), this.status.name(), timestamp);
        }
        
        public static String serializeToHumanReadableString(final String OID, final String name, final String type, final String previousStatus, final String status, final long timestamp) {
            final String timestampString = CluiMonitorView.DATE_FORMAT_SECS.format(new Date(timestamp));
            return "Object UUID    : " + OID + "\nName           : " + name + "\nType           : " + type + "\nPrevious Status: " + previousStatus + "\nCurrent Status : " + status + "\nTimestamp      : " + timestampString;
        }
        
        public static StatusInfo deserializeFromString(final String serializedString) {
            final String[] fiveParts = extractFiveStrings(serializedString);
            if (fiveParts == null) {
                return null;
            }
            try {
                final UUID OID = new UUID(fiveParts[0]);
                final EntityType type = EntityType.forObject(fiveParts[2]);
                final Status previousStatus = Status.valueOf(fiveParts[3]);
                final Status currentStatus = Status.valueOf(fiveParts[4]);
                return new StatusInfo(OID, previousStatus, currentStatus, type, fiveParts[1]);
            }
            catch (Exception e) {
                if (MetaInfo.logger.isInfoEnabled()) {
                    MetaInfo.logger.info((Object)("StatusInfo failed to deserialize " + serializedString), (Throwable)e);
                }
                return null;
            }
        }
        
        public static String[] extractFiveStrings(final String serializedString) {
            final String patternString = "\\{ \"OID\"\\: \"(.*)\", \"name\": \"(.*)\", \"type\"\\: \"(.*)\", \"previousStatus\"\\: \"(.*)\", \"currentStatus\"\\: \"(.*)\" \\}";
            final Pattern pattern = Pattern.compile(patternString);
            final Matcher matcher = pattern.matcher(serializedString);
            if (matcher.find() && matcher.groupCount() == 5) {
                final String[] result = { matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5) };
                return result;
            }
            if (MetaInfo.logger.isInfoEnabled()) {
                MetaInfo.logger.info((Object)("StatusInfo failed to match pattern for " + serializedString));
            }
            return null;
        }
        
        static {
            StatusInfo.ASYNC = true;
            StatusInfo.SYNC = false;
        }
        
        public enum Status implements Serializable
        {
            UNKNOWN(StatusInfo.SYNC), 
            CREATED(StatusInfo.SYNC), 
            DEPLOYING(StatusInfo.ASYNC), 
            DEPLOYED(StatusInfo.SYNC), 
            CRASH(StatusInfo.SYNC), 
            STOPPING(StatusInfo.SYNC), 
            QUIESCING(StatusInfo.ASYNC), 
            QUIESCED(StatusInfo.SYNC), 
            RUNNING(StatusInfo.SYNC), 
            NOT_ENOUGH_SERVERS(StatusInfo.SYNC), 
            FLUSHING(StatusInfo.SYNC);
            
            private boolean isAsync;
            
            private Status(final boolean isAsync) {
                this.isAsync = isAsync;
            }
            
            public boolean isAsync() {
                return this.isAsync;
            }
            
            public boolean isSync() {
                return !this.isAsync;
            }
        }
    }
    
    public static class Cache extends MetaObject implements MetaObjectPermissionChecker
    {
        private static final long serialVersionUID = 1901374789247928L;
        public String adapterClassName;
        public Map<String, Object> reader_properties;
        public Map<String, Object> parser_properties;
        public Map<String, Object> query_properties;
        public Class<?> retType;
        public UUID typename;
        
        public Cache() {
            this.adapterClassName = null;
            this.reader_properties = null;
            this.parser_properties = null;
            this.query_properties = null;
            this.typename = null;
            this.retType = null;
        }
        
        public Cache(final String name, final Namespace ns, final String adapterClassName, final Map<String, Object> reader_properties, final Map<String, Object> parser_properties, final Map<String, Object> query_properties, final UUID typename, final Class<?> retType) {
            super.construct(name, ns, EntityType.CACHE);
            this.adapterClassName = adapterClassName;
            this.reader_properties = reader_properties;
            this.parser_properties = parser_properties;
            this.query_properties = query_properties;
            this.typename = typename;
            this.retType = retType;
        }
        
        public void construct(final String name, final Namespace ns, final String adapterClassName, final Map<String, Object> reader_properties, final Map<String, Object> parser_properties, final Map<String, Object> query_properties, final UUID typename, final Class<?> retType) {
            super.construct(name, ns, EntityType.CACHE);
            this.adapterClassName = adapterClassName;
            this.reader_properties = reader_properties;
            this.parser_properties = parser_properties;
            this.query_properties = query_properties;
            this.typename = typename;
            this.retType = retType;
        }
        
        public String getAdapterClassName() {
            return this.adapterClassName;
        }
        
        public void setAdapterClassName(final String adapterClassName) {
            this.adapterClassName = adapterClassName;
        }
        
        public Map<String, Object> getReader_properties() {
            return this.reader_properties;
        }
        
        public Map<String, Object> getParser_properties() {
            return this.parser_properties;
        }
        
        public void setReader_properties(final Map<String, Object> reader_properties) {
            this.reader_properties = reader_properties;
        }
        
        public Map<String, Object> getQuery_properties() {
            return this.query_properties;
        }
        
        public void setQuery_properties(final Map<String, Object> query_properties) {
            this.query_properties = query_properties;
        }
        
        public Class<?> getRetType() {
            return this.retType;
        }
        
        public void setRetType(final Class<?> retType) {
            this.retType = retType;
        }
        
        public UUID getTypename() {
            return this.typename;
        }
        
        public void setTypename(final UUID typename) {
            this.typename = typename;
        }
        
        @Override
        public String describe(final AuthToken token) {
            PropertyTemplate anno = null;
            String str;
            if (this.adapterClassName == null) {
                str = "EVENTTABLE " + (this.type.isGlobal() ? this.name : (this.nsName + "." + this.name)) + " CREATED " + MetaInfo.sdf.format(new Date(this.ctime));
                str += "\n";
                if (this.description != null) {
                    str = str + this.description + "\n";
                }
                str += " USING STREAM WITH PROPERTIES";
                str += "(\n";
                str += this.prettyPrintMap(this.reader_properties);
                str += ") ";
                str += "QUERY PROPERTIES ( \n";
                str += this.prettyPrintMap(this.query_properties);
                str += ")";
            }
            else {
                try {
                    final Class<?> cls = Class.forName(this.adapterClassName, false, ClassLoader.getSystemClassLoader());
                    anno = cls.getAnnotation(PropertyTemplate.class);
                }
                catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }
                str = this.startDescribe() + " USING " + anno.name() + " WITH READER PROPERTIES";
                str += "(\n";
                str += this.prettyPrintMap(this.reader_properties);
                str += " ) \n";
                if (this.parser_properties != null) {
                    str += " WITH PARSER PROPERTIES";
                    str += "(\n";
                    str += this.prettyPrintMap(this.parser_properties);
                    str += " ) \n";
                }
                str += " QUERY PROPERTIES ( \n";
                str += this.prettyPrintMap(this.query_properties);
                str += " ) \n";
                str += " OF TYPE ";
                try {
                    if (MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.typename, HSecurityManager.TOKEN) != null) {
                        str += MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.typename, HSecurityManager.TOKEN).name;
                    }
                }
                catch (MetaDataRepositoryException e2) {
                    throw new RuntimeException(e2.getMessage());
                }
            }
            this.printDependencyAndStatus();
            return str;
        }
        
        public boolean backedByElasticSearch() {
            boolean backedByElasticSearch = false;
            final String persistPolicy = (String)this.getQuery_properties().get("persistPolicy");
            if (persistPolicy != null && persistPolicy.equalsIgnoreCase("TRUE")) {
                backedByElasticSearch = true;
            }
            return backedByElasticSearch;
        }
        
        public boolean isEventTable() {
            return this.adapterClassName == null;
        }
        
        public void setEventTable(final boolean EventTable) {
        }
        
        @Override
        public List<UUID> getDependencies() {
            final List<UUID> dependencyList = new ArrayList<UUID>();
            dependencyList.add(this.typename);
            if (this.adapterClassName == null) {
                Stream stream = null;
                final String streamName = (String)this.reader_properties.get("NAME");
                if (streamName != null) {
                    try {
                        if (!streamName.contains(".")) {
                            stream = (Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, this.nsName, streamName, null, HSecurityManager.TOKEN);
                        }
                        else {
                            stream = (Stream)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.STREAM, Utility.splitDomain(streamName), Utility.splitName(streamName), null, HSecurityManager.TOKEN);
                        }
                    }
                    catch (SecurityException | MetaDataRepositoryException ex2) {
                        MetaInfo.logger.warn((Object)ex2.getMessage());
                    }
                }
                if (stream != null) {
                    dependencyList.add(stream.getUuid());
                }
            }
            return dependencyList;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final TextProperties adapter_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            adapter_property.setIsRequired(true);
            final ObjectProperties parser_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            parser_property.setIsRequired(false);
            final ObjectProperties query_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            query_property.setIsRequired(true);
            final MetaObjectProperties type_property = MetaInfo.actionablePropertiesFactory.createMetaObjectProperties();
            type_property.setIsRequired(true);
            Cache.actions.put("adapter", adapter_property);
            Cache.actions.put("parser", parser_property);
            Cache.actions.put("queryProperties", query_property);
            Cache.actions.put("typename", type_property);
            return Cache.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            final Map propertiesCopy = new HashMap(this.reader_properties);
            final StringBuilder tempBuffer = new StringBuilder();
            final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
            if (this.adapterClassName != null) {
                final ObjectNode adapterProps = jsonMapper.createObjectNode();
                final String simpleHandlerName = this.adapterClassName.split("\\.")[3].split("_")[0];
                tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerName);
                adapterProps.put("handler", tempBuffer.toString());
                final JsonNode pNode = (JsonNode)jsonMapper.convertValue((Object)propertiesCopy, (Class)JsonNode.class);
                adapterProps.set("properties", pNode);
                parent.set("adapter", (JsonNode)adapterProps);
                tempBuffer.setLength(0);
            }
            final Map parserPropertiesCopy = new HashMap(this.parser_properties);
            String simpleHandlerNameInParser = (String)parserPropertiesCopy.get("handler");
            if (simpleHandlerNameInParser != null) {
                final ObjectNode parserProps = jsonMapper.createObjectNode();
                simpleHandlerNameInParser = simpleHandlerNameInParser.split("\\.")[3].split("_")[0];
                tempBuffer.append(MetaInfo.GlobalNamespace.getName()).append(".").append(EntityType.PROPERTYTEMPLATE).append(".").append(simpleHandlerNameInParser);
                parserProps.put("handler", tempBuffer.toString());
                final JsonNode ppNode = (JsonNode)jsonMapper.convertValue((Object)parserPropertiesCopy, (Class)JsonNode.class);
                parserProps.set("properties", ppNode);
                parent.set("parser", (JsonNode)parserProps);
                tempBuffer.setLength(0);
            }
            final Map queryPropertiesCopy = new HashMap(this.query_properties);
            final JsonNode qNode = (JsonNode)jsonMapper.convertValue((Object)queryPropertiesCopy, (Class)JsonNode.class);
            parent.set("queryProperties", qNode);
            if (this.typename != null) {
                try {
                    final MetaObject typeInfo = MetaInfo.metadataRepository.getMetaObjectByUUID(this.typename, HSecurityManager.TOKEN);
                    if (typeInfo != null) {
                        parent.put("typename", typeInfo.getFQN());
                    }
                    else {
                        parent.putNull("typename");
                    }
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("typename");
            }
            return parent;
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            graph.get(this.typename).add(this.getUuid());
            return graph;
        }
        
        @Override
        public boolean checkPermissionForMetaPropertyVariable(final AuthToken token) throws MetaDataRepositoryException {
            final String namespace = this.getNsName();
            final Map<String, Object> readerProperties = this.getReader_properties();
            final Map<String, Object> queryProperties = this.getQuery_properties();
            final Map<String, Object> parserProperties = this.getParser_properties();
            return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, readerProperties, queryProperties, parserProperties);
        }
    }
    
    public static class HDStore extends MetaObject implements MetaObjectPermissionChecker
    {
        private static final long serialVersionUID = -8622778110930671241L;
        public UUID contextType;
        public Interval frequency;
        public List<UUID> eventTypes;
        public List<String> eventKeys;
        public Map<String, Object> properties;
        public com.datasphere.hdstore.Type hdstoretype;
        public int classId;
        
        public HDStore() {
            this.classId = -1;
            this.contextType = null;
            this.frequency = null;
            this.eventTypes = null;
            this.eventKeys = null;
            this.properties = null;
            this.type = null;
        }
        
        public void construct(final String name, final Namespace ns, final UUID contextType, final Interval frequency, final List<UUID> eventTypes, final List<String> eventKeys, final Map<String, Object> properties) {
            super.construct(name, ns, EntityType.HDSTORE);
            this.contextType = contextType;
            this.frequency = frequency;
            this.eventTypes = eventTypes;
            this.eventKeys = eventKeys;
            this.properties = ((properties == null) ? Collections.emptyMap() : properties);
            this.hdstoretype = com.datasphere.hdstore.Type.getType(this.properties, com.datasphere.hdstore.Type.INTERVAL);
        }
        
        public UUID getContextType() {
            return this.contextType;
        }
        
        public void setContextType(final UUID contextType) {
            this.contextType = contextType;
        }
        
        public Interval getFrequency() {
            return this.frequency;
        }
        
        public void setFrequency(final Interval frequency) {
            this.frequency = frequency;
        }
        
        public List<UUID> getEventTypes() {
            return this.eventTypes;
        }
        
        public void setEventTypes(final List<UUID> eventTypes) {
            this.eventTypes = eventTypes;
        }
        
        public List<String> getEventKeys() {
            return this.eventKeys;
        }
        
        public void setEventKeys(final List<String> eventKeys) {
            this.eventKeys = eventKeys;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
            this.hdstoretype = com.datasphere.hdstore.Type.getType(this.properties, com.datasphere.hdstore.Type.INTERVAL);
        }
        
        @Override
        public String toString() {
            return this.metaToString() + " context:" + this.contextType + " frequency:" + this.frequency + " event types:" + this.eventTypes + " properties:" + this.properties;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String str = this.startDescribe();
            str = str + "CONTEXT = " + MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, HSecurityManager.TOKEN).name;
            str += "\nEVENT TYPE = [ \n";
            for (int i = 0; i < this.eventTypes.size(); ++i) {
                str += MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.eventTypes.get(i), HSecurityManager.TOKEN).name;
                str = str + "(" + this.eventKeys.get(i) + ")";
            }
            str += "\n]\n";
            boolean showProperties = false;
            switch (this.hdstoretype) {
                case IN_MEMORY: {
                    str += "USING MEMORY\n";
                    break;
                }
                case STANDARD: {
                    showProperties = true;
                    break;
                }
                case INTERVAL: {
                    showProperties = true;
                    str += "PERSIST ";
                    if (this.frequency == null) {
                        str += "NONE\n";
                        break;
                    }
                    if (this.frequency.value == 0L) {
                        str += "IMMEDIATE\n";
                        break;
                    }
                    str = str + "EVERY " + this.frequency.toHumanReadable() + '\n';
                    break;
                }
            }
            if (showProperties) {
                boolean first = true;
                for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                    final String propertyName = entry.getKey();
                    if (propertyName.toLowerCase().contains("password_encrypted")) {
                        continue;
                    }
                    if (entry.getValue() == null) {
                        continue;
                    }
                    final String propertyValue = propertyName.toLowerCase().contains("password") ? "********" : entry.getValue().toString();
                    if (!first) {
                        str += ",\n";
                    }
                    else {
                        str += "USING (\n";
                        first = false;
                    }
                    str = str + "  " + propertyName + ": '" + propertyValue + "'";
                }
                if (!first) {
                    str += "\n)";
                }
            }
            this.printDependencyAndStatus();
            return str;
        }
        
        @Override
        public List<UUID> getDependencies() {
            return Collections.singletonList(this.contextType);
        }
        
        public Class<?> getHDType() throws MetaDataRepositoryException {
            final WALoader loader = WALoader.get();
            final Type contextBeanDef = (Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, HSecurityManager.TOKEN);
            final String fullTableName = this.nsName + "_" + this.name;
            final String hdClassName = "wa.HD_" + fullTableName;
            Class<?> hdClass = null;
            try {
                hdClass = loader.loadClass(hdClassName);
            }
            catch (ClassNotFoundException e2) {
                final String wbundleUri = WALoader.get().getBundleUri(this.nsName, BundleDefinition.Type.hd, hdClassName);
                try {
                    loader.lockBundle(wbundleUri);
                    loader.addHDClass(hdClassName, contextBeanDef);
                    hdClass = loader.loadClass(hdClassName);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    loader.unlockBundle(wbundleUri);
                }
            }
            return hdClass;
        }
        
        List<Pair<String, String>> getColumnsDesc() {
            return null;
        }
        
        List<Pair<Integer, List<String>>> getIndexesDesc() {
            return null;
        }
        
        public boolean usesInMemoryHDStore() {
            return this.hdstoretype == com.datasphere.hdstore.Type.IN_MEMORY;
        }
        
        public boolean usesIntervalBasedHDStore() {
            return this.hdstoretype == com.datasphere.hdstore.Type.INTERVAL;
        }
        
        public boolean usesOldHDStore() {
            return this.hdstoretype == com.datasphere.hdstore.Type.INTERVAL || this.hdstoretype == com.datasphere.hdstore.Type.IN_MEMORY;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final ObjectProperties context_type_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            context_type_property.setIsRequired(true);
            final ObjectProperties frequency_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties event_type_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties event_keys_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties persistence_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            persistence_property.setIsRequired(true);
            HDStore.actions.put("contextType", context_type_property);
            HDStore.actions.put("frequency", frequency_property);
            HDStore.actions.put("eventTypes", event_type_property);
            HDStore.actions.put("keyField", event_keys_property);
            HDStore.actions.put("persistence", persistence_property);
            return HDStore.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            if (this.contextType != null) {
                try {
                    final MetaObject ctxType = MetaInfo.metadataRepository.getMetaObjectByUUID(this.contextType, HSecurityManager.TOKEN);
                    if (ctxType != null) {
                        parent.put("contextType", ctxType.getFQN());
                    }
                    else {
                        parent.putNull("contextType");
                    }
                }
                catch (MetaDataRepositoryException ex) {}
            }
            else {
                parent.putNull("contextType");
            }
            final ArrayNode fieldArray = this.jsonMapper.createArrayNode();
            if (this.eventTypes != null) {
                for (int iter = 0; iter < this.eventTypes.size(); ++iter) {
                    try {
                        final Type type = (Type)MetaInfo.metadataRepository.getMetaObjectByUUID(this.eventTypes.get(iter), HSecurityManager.TOKEN);
                        final ObjectNode eventType = this.jsonMapper.createObjectNode();
                        eventType.put("id", type.getUuid().toString());
                        eventType.put("typename", type.getFQN());
                        eventType.put("keyField", (String)this.eventKeys.get(iter));
                        fieldArray.add((JsonNode)eventType);
                    }
                    catch (MetaDataRepositoryException ex2) {}
                }
            }
            parent.set("eventTypes", (JsonNode)fieldArray);
            final ObjectNode persistenceInfo = this.jsonMapper.createObjectNode();
            if (this.frequency != null) {
                persistenceInfo.put("frequency", this.frequency.toHumanReadable());
            }
            else {
                persistenceInfo.putNull("frequency");
            }
            final Map<String, Object> processedProperties = Maps.newHashMap();
            if (this.properties != null) {
                for (final Map.Entry<String, Object> entry : this.properties.entrySet()) {
                    processedProperties.put(entry.getKey(), entry.getValue());
                }
            }
            if (processedProperties.get("storageProvider") == null) {
                if (this.hdstoretype == com.datasphere.hdstore.Type.STANDARD) {
                    processedProperties.put("storageProvider", "elasticsearch");
                }
                else if (this.frequency == null && this.hdstoretype == com.datasphere.hdstore.Type.INTERVAL) {
                    processedProperties.put("storageProvider", "inmemory");
                }
            }
            final JsonNode pNode = (JsonNode)this.jsonMapper.convertValue((Object)processedProperties, (Class)JsonNode.class);
            persistenceInfo.set("properties", pNode);
            parent.set("persistence", (JsonNode)persistenceInfo);
            return parent;
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            super.exportOrder(graph);
            graph.get(this.contextType).add(this.getUuid());
            if (this.eventTypes != null) {
                for (final UUID eventType : this.eventTypes) {
                    graph.get(eventType).add(this.getUuid());
                }
            }
            return graph;
        }
        
        public void generateClasses() throws Exception {
            final WALoader waLoader = WALoader.get();
            final Type contextBeanDef = (Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, HSecurityManager.TOKEN);
            final String hdContextClassName = contextBeanDef.className + "_hdContext";
            final String fullTableName = this.nsName + "_" + this.name;
            final String hdClassName = "wa.HD_" + fullTableName;
            this.classId = waLoader.getClassId(hdContextClassName);
            final String cbundleUri = WALoader.get().getBundleUri(this.nsName, BundleDefinition.Type.context, hdContextClassName);
            WALoader.get().lockBundle(cbundleUri);
            try {
                if (this.eventTypes == null || this.eventTypes.isEmpty()) {
                    WALoader.get().addHDContextClassNoEventType(contextBeanDef);
                }
                else {
                    WALoader.get().addHDContextClass(contextBeanDef);
                }
            }
            catch (IllegalArgumentException e) {
                if (MetaInfo.logger.isDebugEnabled()) {
                    MetaInfo.logger.debug((Object)("Context Class is already defined elsewhere for " + this.getFullName()), (Throwable)e);
                }
            }
            catch (Exception e2) {
                MetaInfo.logger.error((Object)("Problem creating hd context class for " + this.getFullName()), (Throwable)e2);
            }
            finally {
                WALoader.get().unlockBundle(cbundleUri);
            }
            final String wbundleUri = WALoader.get().getBundleUri(this.nsName, BundleDefinition.Type.hd, hdClassName);
            WALoader.get().lockBundle(wbundleUri);
            try {
                WALoader.get().addHDClass(hdClassName, contextBeanDef);
                if (MetaInfo.logger.isDebugEnabled()) {
                    MetaInfo.logger.debug((Object)("Generated HD class for HD Store " + this.name + " of type " + contextBeanDef.getFullName()));
                }
            }
            catch (IllegalArgumentException e3) {
                if (MetaInfo.logger.isDebugEnabled()) {
                    MetaInfo.logger.debug((Object)("HD Class is already defined elsewhere for " + this.getFullName()), (Throwable)e3);
                }
            }
            catch (Exception e4) {
                MetaInfo.logger.error((Object)("Problem creating hd context class for " + this.getFullName()), (Throwable)e4);
            }
            finally {
                WALoader.get().unlockBundle(wbundleUri);
            }
            final String bundleUri = WALoader.get().createIfNotExistsBundleDefinition(this.nsName, BundleDefinition.Type.fieldFactory, this.name);
            for (final String fname : contextBeanDef.fields.keySet()) {
                try {
                    this.genFieldFactory(bundleUri, contextBeanDef.className, fname, this.name + "_" + contextBeanDef.name, contextBeanDef.nsName);
                    MetaInfo.logger.info((Object)("Generated Field Factory class for context field " + fname + " in HD Store: " + this.name + " of type: " + contextBeanDef.getFullName()));
                }
                catch (Exception e5) {
                    if (!MetaInfo.logger.isDebugEnabled()) {
                        continue;
                    }
                    MetaInfo.logger.debug((Object)("Field factory class is already generated for " + fname + " in " + contextBeanDef.getFullName()), (Throwable)e5);
                }
            }
        }
        
        private void genFieldFactory(final String bundleUri, final String eventTypeClassName, final String fieldName, final String typeName, final String typeNamespaceName) throws IllegalArgumentException, NotFoundException, CannotCompileException, IOException {
            final WALoader wal = WALoader.get();
            final ClassPool pool = wal.getBundlePool(bundleUri);
            final String className = "FieldFactory_" + fieldName + "_" + typeNamespaceName + "_" + typeName;
            final CtClass cc = pool.makeClass(className);
            final CtClass sup = pool.get(HStore.FieldFactory.class.getName());
            cc.setSuperclass(sup);
            final String code = "public Object getField(Object obj)\n{\n\t" + eventTypeClassName + " tmp = (" + eventTypeClassName + ")obj;\n\treturn " + FieldToObject.genConvert("tmp." + fieldName) + ";\n}\n";
            final CtMethod m = CtNewMethod.make(code, cc);
            cc.addMethod(m);
            final String fieldString = "public String fieldName = \"" + fieldName + '\"' + ";";
            final CtField ctField = CtField.make(fieldString, cc);
            ctField.setModifiers(1);
            cc.addField(ctField);
            final String getFieldNameMethod = "public String getFieldName()\n{\n\t return fieldName;\n}\n";
            final CtMethod cm = CtNewMethod.make(getFieldNameMethod, cc);
            cm.setModifiers(1);
            cc.addMethod(cm);
            cc.setModifiers(cc.getModifiers() & 0xFFFFFBFF);
            cc.setModifiers(1);
            wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        }
        
        public void removeGeneratedClasses() throws MetaDataRepositoryException {
            final Type contextBeanDef = (Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.contextType, HSecurityManager.TOKEN);
            final String hdContextClassName = contextBeanDef.className + "_hdContext";
            final String fullTableName = this.nsName + "_" + this.name;
            final String hdClassName = "wa.HD_" + fullTableName;
            WALoader.get().removeBundle(this.nsName, BundleDefinition.Type.fieldFactory, this.name);
            WALoader.get().removeBundle(this.nsName, BundleDefinition.Type.context, hdContextClassName);
            WALoader.get().removeBundle(this.nsName, BundleDefinition.Type.hd, hdClassName);
        }
        
        @Override
        public boolean checkPermissionForMetaPropertyVariable(final AuthToken token) throws MetaDataRepositoryException {
            final String namespace = this.getNsName();
            final Map<String, Object> readerProperties = this.getProperties();
            return PropertyVariablePermissionChecker.propertyVariableAccessChecker(token, namespace, readerProperties);
        }
    }
    
    public static class AlertSubscriber extends MetaObject
    {
        private static final long serialVersionUID = -693870604910801142L;
        public String adapterClassName;
        public Map<String, Object> properties;
        
        public void construct(final String name, final Namespace ns, final String adapterClassName, final Map<String, Object> props) {
            super.construct(name, ns, EntityType.ALERTSUBSCRIBER);
            this.adapterClassName = adapterClassName;
            this.properties = props;
        }
        
        public String getAdapterClassName() {
            return this.adapterClassName;
        }
        
        public void setAdapterClassName(final String adapterClassName) {
            this.adapterClassName = adapterClassName;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        @Override
        public String toString() {
            return this.type + " " + this.name + " " + this.adapterClassName + " " + this.properties;
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final TextProperties adapter_class_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            adapter_class_property.setIsRequired(true);
            final ObjectProperties properties_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            properties_property.setIsRequired(true);
            return AlertSubscriber.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("adapter", this.adapterClassName);
            final ObjectNode objectNode = this.jsonMapper.createObjectNode();
            final JsonNode pNode = (JsonNode)this.jsonMapper.convertValue((Object)this.properties, (Class)JsonNode.class);
            objectNode.set("properties", pNode);
            return parent;
        }
    }
    
    public static class ContactMechanism implements Serializable
    {
        private static final long serialVersionUID = 7025105184172001669L;
        private boolean isDefault;
        private ContactType type;
        private String data;
        private int index;
        
        public ContactMechanism() {
            this.construct(false, null, null);
        }
        
        public void construct(final ContactType type, final String data) {
            this.construct(false, type, data);
        }
        
        public void construct(final boolean isDefault, final ContactType type, final String data) {
            this.isDefault = isDefault;
            this.type = type;
            this.data = data;
        }
        
        public boolean isDefault() {
            return this.isDefault;
        }
        
        public void setDefault(final boolean isDefault) {
            this.isDefault = isDefault;
        }
        
        public ContactType getType() {
            return this.type;
        }
        
        public void setType(final ContactType type) {
            this.type = type;
        }
        
        public String getData() {
            return this.data;
        }
        
        public void setData(final String data) {
            this.data = data;
        }
        
        public int getIndex() {
            return this.index;
        }
        
        public void setIndex(final int index) {
            this.index = index;
        }
        
        @Override
        public boolean equals(final Object other) {
            return other instanceof ContactMechanism && ((ContactMechanism)other).type == this.type && ((ContactMechanism)other).data.equals(this.data);
        }
        
        @Override
        public int hashCode() {
            return new HashCodeBuilder().append((Object)this.type.toString()).append((Object)this.data).toHashCode();
        }
        
        @Override
        public String toString() {
            return "type : " + this.type + " value : " + this.data;
        }
        
        public ObjectNode getJsonForClient() {
            final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
            final ObjectNode parent = jsonMapper.createObjectNode();
            parent.put("contactType", this.type.toString());
            parent.put("isDefault", this.isDefault);
            parent.put("data", this.data);
            parent.put("index", this.index);
            return parent;
        }
        
        public enum ContactType
        {
            email, 
            phone, 
            sms, 
            web;
        }
    }
    
    public static class User extends MetaObject implements Permissable, Roleable
    {
        private static final long serialVersionUID = -8964675659436023782L;
        private String userId;
        private String firstName;
        private String lastName;
        private String mainEmail;
        private String encryptedPassword;
        private String defaultNamespace;
        private String userTimeZone;
        private String alias;
        private List<ContactMechanism> contactMechanisms;
        private List<ObjectPermission> permissions;
        private List<UUID> roleUUIDs;
        private transient List<ObjectPermission> allPermissions;
        private String ldap;
        private AUTHORIZATION_TYPE originType;
        
        public User() {
            this.contactMechanisms = new ArrayList<ContactMechanism>();
            this.permissions = new ArrayList<ObjectPermission>();
            this.roleUUIDs = new ArrayList<UUID>();
            this.ldap = null;
            this.originType = AUTHORIZATION_TYPE.INTERNAL;
            super.type = EntityType.USER;
            super.namespaceId = MetaInfo.GlobalNamespace.uuid;
            super.nsName = MetaInfo.GlobalNamespace.name;
            this.userTimeZone = "";
        }
        
        public void construct(final String userId) {
            this.construct(userId, null, null, null, null, null, null, null);
        }
        
        public void construct(final String userId, final String encryptedPassword) {
            this.construct(userId, encryptedPassword, null, null, null, null, null, null);
        }
        
        public void construct(final String userId, final String encryptedPassword, final List<Role> roles) {
            this.construct(userId, encryptedPassword, null, null, null, null, null, roles);
        }
        
        public void construct(final String userId, final String encryptedPassword, final String mainEmail) {
            this.construct(userId, encryptedPassword, mainEmail, null, null, null, null, null);
        }
        
        public void construct(final String userId, final String encryptedPassword, final String mainEmail, final String firstName, final String lastName) {
            this.construct(userId, encryptedPassword, mainEmail, firstName, lastName, null, null, null);
        }
        
        public void construct(final String userId, final String encryptedPassword, final String mainEmail, final String firstName, final String lastName, final String alias, final List<ContactMechanism> contactMechanisms, final List<Role> roles) {
            super.construct(userId, MetaInfo.GlobalNamespace, EntityType.USER);
            this.userId = userId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.mainEmail = mainEmail;
            this.userTimeZone = "";
            this.alias = alias;
            try {
                this.encryptedPassword = HSecurityManager.encrypt(encryptedPassword, super.uuid.toEightBytes());
            }
            catch (UnsupportedEncodingException e) {
                MetaInfo.logger.error((Object)e);
            }
            catch (GeneralSecurityException e2) {
                MetaInfo.logger.error((Object)e2);
            }
            if (contactMechanisms != null) {
                this.contactMechanisms.addAll(contactMechanisms);
            }
            if (mainEmail != null) {
                final ContactMechanism memail = new ContactMechanism();
                memail.construct(ContactMechanism.ContactType.email, mainEmail);
                this.contactMechanisms.add(memail);
            }
            if (roles != null) {
                for (final Role r : roles) {
                    this.roleUUIDs.add(r.getUuid());
                }
            }
            this.defaultNamespace = null;
        }
        
        public String getAlias() {
            return this.alias;
        }
        
        public void setAlias(final String alias) {
            this.alias = alias;
        }
        
        public String getLdap() {
            return this.ldap;
        }
        
        public void setLdap(final String ldap) {
            this.ldap = ldap;
        }
        
        public AUTHORIZATION_TYPE getOriginType() {
            return this.originType;
        }
        
        public void setOriginType(final AUTHORIZATION_TYPE originType) {
            this.originType = originType;
        }
        
        @Override
        public int hashCode() {
            return this.userId.hashCode();
        }
        
        public List<UUID> getRoleUUIDs() {
            return this.roleUUIDs;
        }
        
        public void setRoleUUIDs(final List<UUID> roleUUIDs) {
            this.roleUUIDs = roleUUIDs;
        }
        
        public void setUserId(final String userId) {
            this.userId = userId;
        }
        
        public void setPermissions(final List<ObjectPermission> permissions) {
            if (this.permissions == null) {
                this.permissions = permissions;
            }
            else {
                this.permissions.clear();
                this.permissions.addAll(permissions);
            }
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof User) {
                final User other = (User)obj;
                return other.userId.equals(this.userId);
            }
            return false;
        }
        
        @Override
        public String toString() {
            return this.userId;
        }
        
        public String getDefaultNamespace() {
            return this.defaultNamespace;
        }
        
        public void setDefaultNamespace(final String defaultNamespace) {
            this.defaultNamespace = defaultNamespace;
        }
        
        public String getUserId() {
            return this.userId;
        }
        
        public String getFirstName() {
            return this.firstName;
        }
        
        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }
        
        public String getUserTimeZone() {
            return this.userTimeZone;
        }
        
        public void setUserTimeZone(final String tz) {
            this.userTimeZone = tz;
        }
        
        public String getLastName() {
            return this.lastName;
        }
        
        public void setLastName(final String lastName) {
            this.lastName = lastName;
        }
        
        public String getMainEmail() {
            return this.mainEmail;
        }
        
        public void setMainEmail(final String mainEmail) {
            this.mainEmail = mainEmail;
            final ContactMechanism memail = new ContactMechanism();
            memail.construct(ContactMechanism.ContactType.email, mainEmail);
            if (!this.contactMechanisms.contains(memail)) {
                this.contactMechanisms.add(memail);
            }
        }
        
        public String getEncryptedPassword() {
            return this.encryptedPassword;
        }
        
        public void setEncryptedPassword(final String encryptedPassword) {
            this.encryptedPassword = encryptedPassword;
        }
        
        private void updateContactIndices() {
            for (final ContactMechanism contact : this.contactMechanisms) {
                contact.setIndex(this.contactMechanisms.indexOf(contact));
            }
        }
        
        public List<ContactMechanism> getContactMechanisms() {
            return this.contactMechanisms;
        }
        
        public void addContactMechanism(final ContactMechanism.ContactType type, final String data) {
            final ContactMechanism contact = new ContactMechanism();
            contact.construct(type, data);
            if (!this.contactMechanisms.contains(contact)) {
                this.contactMechanisms.add(contact);
                this.updateContactIndices();
            }
        }
        
        public void removeContactMechanism(final int index) {
            if (index < this.contactMechanisms.size()) {
                this.contactMechanisms.remove(index);
                this.updateContactIndices();
            }
        }
        
        public void updateContactMechanism(final int index, final ContactMechanism.ContactType type, final String data) {
            if (index < this.contactMechanisms.size()) {
                final ContactMechanism contact = this.contactMechanisms.get(index);
                contact.setType(type);
                contact.setData(data);
            }
        }
        
        public void setDefaultContactMechanism(final int index) {
            if (index < this.contactMechanisms.size()) {
                for (final ContactMechanism contact : this.contactMechanisms) {
                    contact.setDefault(contact.getIndex() == index);
                }
            }
        }
        
        @Override
        public void grantPermission(final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
            if (this.permissions.contains(permission)) {
                return;
            }
            final ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
            if (this.permissions.contains(reversePermission)) {
                this.permissions.remove(reversePermission);
            }
            this.refreshPermissions();
            for (final ObjectPermission perm : this.getAllPermissions()) {
                if (perm.equals(permission)) {
                    return;
                }
            }
            this.permissions.add(permission);
        }
        
        @Override
        public void grantPermissions(final List<ObjectPermission> permissionsList) throws SecurityException, MetaDataRepositoryException {
            for (final ObjectPermission perm : permissionsList) {
                this.grantPermission(perm);
            }
        }
        
        public void resetPermissions(List<ObjectPermission> permissionsList) throws SecurityException {
            if (permissionsList == null) {
                permissionsList = new ArrayList<ObjectPermission>();
            }
            this.permissions.clear();
            this.permissions.addAll(permissionsList);
        }
        
        public void revokePermission1(final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
            if (permission.getType().equals(ObjectPermission.PermissionType.disallow) && this.permissions.contains(permission)) {
                this.permissions.remove(permission);
                return;
            }
            final ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
            if (this.permissions.contains(reversePermission)) {
                return;
            }
            if (this.permissions.contains(permission)) {
                this.permissions.remove(permission);
            }
            this.refreshPermissions();
            for (final ObjectPermission perm : this.getAllPermissions()) {
                if (perm.implies(permission)) {
                    this.permissions.add(reversePermission);
                    return;
                }
            }
            this.permissions.add(reversePermission);
        }
        
        @Override
        public void revokePermission(final ObjectPermission permToBeRevoked) throws SecurityException, MetaDataRepositoryException {
            final UserAndRoleUtility util = new UserAndRoleUtility();
            if (util.isDisallowedPermission(permToBeRevoked)) {
                throw new SecurityException("Revoking a disallowed permission is NOT allowed. Grant to remove it");
            }
            final boolean userHasPermission = util.doesListHasImpliedPerm(this.permissions, permToBeRevoked);
            final boolean userHasPermissionThruRole = util.doesListHasImpliedPerm(this.getPermissionsThruRoles(), permToBeRevoked);
            if (!userHasPermission) {
                String exceptionString = "User has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet.";
                MetaInfo.logger.warn((Object)("User has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet."));
                if (userHasPermissionThruRole) {
                    exceptionString += "But user has that permission through a role. Revoke permission from Role.";
                }
                throw new SecurityException(exceptionString);
            }
            final ObjectPermission revPermToBeRevoked = new ObjectPermission(permToBeRevoked, ObjectPermission.PermissionType.disallow);
            if (this.permissions.contains(revPermToBeRevoked)) {
                MetaInfo.logger.warn((Object)(revPermToBeRevoked + " permission already revoked."));
                return;
            }
            final boolean exactPermissionExists = util.doesListHasExactPerm(this.permissions, permToBeRevoked);
            if (exactPermissionExists) {
                this.permissions.remove(permToBeRevoked);
                final List<ObjectPermission> list = util.getDependentPerms(permToBeRevoked, this.permissions);
                for (final ObjectPermission perm : list) {
                    this.permissions.remove(perm);
                }
            }
            if (util.doesListHasImpliedPerm(this.permissions, permToBeRevoked)) {
                this.permissions.add(revPermToBeRevoked);
            }
            this.refreshPermissions();
        }
        
        @Override
        public void revokePermissions(final List<ObjectPermission> permissionsList) throws SecurityException, MetaDataRepositoryException {
            for (final ObjectPermission perm : permissionsList) {
                this.revokePermission(perm);
            }
        }
        
        @Override
        public void grantRole(final Role role) {
            if (this.roleUUIDs.contains(role.getUuid())) {
                return;
            }
            this.roleUUIDs.add(role.getUuid());
        }
        
        @Override
        public void grantRoles(final List<Role> roles) {
            if (roles == null || roles.size() == 0) {
                return;
            }
            for (final Role role : roles) {
                final UUID id = role.getUuid();
                if (!this.roleUUIDs.contains(id)) {
                    this.roleUUIDs.add(id);
                }
            }
        }
        
        @Override
        public void revokeRole(final Role role) {
            if (!this.roleUUIDs.contains(role.getUuid())) {
                return;
            }
            this.roleUUIDs.remove(role.getUuid());
        }
        
        @Override
        public void revokeRoles(final List<Role> roles) {
            if (roles == null || roles.size() == 0) {
                return;
            }
            for (final Role role : roles) {
                final UUID id = role.getUuid();
                if (this.roleUUIDs.contains(id)) {
                    this.roleUUIDs.remove(id);
                }
            }
        }
        
        @Override
        public List<Role> getRoles() throws MetaDataRepositoryException {
            List<Role> rs = null;
            if (this.roleUUIDs != null) {
                rs = new ArrayList<Role>();
                for (final UUID id : this.roleUUIDs) {
                    final Role r = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, HSecurityManager.TOKEN);
                    if (r != null && !rs.contains(r)) {
                        rs.add(r);
                    }
                }
            }
            return rs;
        }
        
        @Override
        public void setRoles(final List<Role> roles) {
            if (roles != null) {
                for (final Role r : roles) {
                    if (!this.roleUUIDs.contains(r.getUuid())) {
                        this.roleUUIDs.add(r.getUuid());
                    }
                }
            }
        }
        
        public void resetRoles(final List<Role> roles) {
            if (roles == null) {
                this.roleUUIDs.clear();
            }
            else {
                this.roleUUIDs.clear();
                this.setRoles(roles);
            }
        }
        
        @Override
        public List<String> convertToNewSyntax(final List<ObjectPermission> permissions) {
            final List<String> resultSet = new ArrayList<String>();
            if (permissions == null) {
                return resultSet;
            }
            for (final ObjectPermission permission : permissions) {
                final String permissionString = permission.toString();
                final String[] splitArray = permissionString.split(":");
                if (splitArray.length < 4) {
                    continue;
                }
                final String[] subTypes = splitArray[2].split(",");
                final StringBuilder stringBuffer = new StringBuilder();
                for (int i = 0; i < subTypes.length; ++i) {
                    stringBuffer.append((splitArray.length == 4) ? "GRANT " : "REVOKE ");
                    if (splitArray[1].equalsIgnoreCase("*")) {
                        stringBuffer.append("ALL ");
                    }
                    else {
                        stringBuffer.append(splitArray[1].toUpperCase()).append(" ");
                    }
                    stringBuffer.append("ON ");
                    stringBuffer.append(subTypes[i]).append(" ");
                    stringBuffer.append(splitArray[0]).append(".").append(splitArray[3]);
                    if (i != subTypes.length - 1) {
                        stringBuffer.append(", ");
                    }
                }
                resultSet.add(stringBuffer.toString());
            }
            return resultSet;
        }
        
        @Override
        public List<ObjectPermission> getPermissions() {
            return this.permissions;
        }
        
        @JsonIgnore
        public List<ObjectPermission> getPermissionsThruRoles() throws MetaDataRepositoryException {
            final List<ObjectPermission> perms = new ArrayList<ObjectPermission>();
            if (this.roleUUIDs != null && this.roleUUIDs.size() > 0) {
                for (final UUID id : this.roleUUIDs) {
                    final Role temprole = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, HSecurityManager.TOKEN);
                    if (temprole != null) {
                        perms.addAll(temprole.getAllPermissions());
                    }
                }
            }
            return perms;
        }
        
        @JsonIgnore
        @Override
        public List<ObjectPermission> getAllPermissions() {
            return this.allPermissions;
        }
        
        public void refreshPermissions() throws MetaDataRepositoryException {
            (this.allPermissions = new ArrayList<ObjectPermission>()).addAll(this.getPermissionsThruRoles());
            this.allPermissions.addAll(this.permissions);
        }
        
        public void setContactMechanisms(final List<ContactMechanism> contactMechanisms) {
            if (contactMechanisms != null) {
                this.contactMechanisms = contactMechanisms;
            }
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            final StringBuilder desc = new StringBuilder(this.startDescribe());
            desc.append("USERID ").append(this.userId).append("\n");
            if (this.firstName != null && !this.firstName.isEmpty()) {
                desc.append("FIRSTNAME ").append(this.firstName).append("\n");
            }
            if (this.lastName != null && !this.lastName.isEmpty()) {
                desc.append("LASTNAME ").append(this.lastName).append("\n");
            }
            if (this.userTimeZone != null && !this.userTimeZone.isEmpty()) {
                desc.append("TIMEZONE ").append(this.userTimeZone).append("\n");
            }
            if (this.alias != null && !this.alias.isEmpty()) {
                desc.append("ALIAS FOR ").append(this.alias).append("\n");
            }
            if (this.ldap != null && !this.ldap.isEmpty()) {
                desc.append("LDAP ").append(this.ldap).append("\n");
            }
            desc.append("CONTACT THROUGH ").append(this.contactMechanisms).append("\n");
            desc.append("ROLES {");
            if (this.roleUUIDs != null) {
                boolean notFirst = false;
                for (final UUID roleUUID : this.roleUUIDs) {
                    final Role r = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(roleUUID, HSecurityManager.TOKEN);
                    if (r != null) {
                        if (notFirst) {
                            desc.append(", ");
                        }
                        else {
                            notFirst = true;
                        }
                        desc.append(r.toString());
                    }
                }
            }
            desc.append("}\n");
            desc.append("PERMISSIONS ").append(this.convertToNewSyntax(this.getPermissions())).append("\n");
            desc.append(this.originType.toString()).append(" user.");
            return desc.toString();
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        public static User deserialize(final JsonNode jsonNode) throws JsonParseException, JsonMappingException, IOException {
            if (jsonNode == null) {
                return null;
            }
            return MetaInfoJsonSerializer.UserJsonSerializer.deserialize(jsonNode.toString());
        }
        
        public boolean hasGlobalAdminRole() {
            try {
                for (final Role role : this.getRoles()) {
                    if (role.isGlobalAdminRole()) {
                        return true;
                    }
                }
                return false;
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.error((Object)"Error looking up user roles", (Throwable)e);
                return false;
            }
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final TextProperties userid_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final TextProperties fname_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final TextProperties lname_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final TextProperties mainemail_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final ObjectProperties contact_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties permission_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties role_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            User.actions.put("firstName", fname_property);
            User.actions.put("userId", userid_property);
            User.actions.put("lastName", lname_property);
            User.actions.put("mainEmail", mainemail_property);
            User.actions.put("contactMechanisms", contact_property);
            User.actions.put("permissions", permission_property);
            User.actions.put("roleUUIDs", role_property);
            return User.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("userId", this.userId);
            parent.put("firstName", this.firstName);
            parent.put("lastName", this.lastName);
            parent.put("mainEmail", this.mainEmail);
            parent.put("defaultNamespace", this.defaultNamespace);
            final ArrayNode cArray = this.jsonMapper.createArrayNode();
            parent.set("contactMechanisms", (JsonNode)cArray);
            final ArrayNode pArray = this.jsonMapper.createArrayNode();
            parent.set("permissions", (JsonNode)pArray);
            final ArrayNode rArray = this.jsonMapper.createArrayNode();
            parent.set("roleUUIDs", (JsonNode)rArray);
            return parent;
        }
        
        public enum AUTHORIZATION_TYPE
        {
            INTERNAL, 
            LDAP;
        }
    }
    
    public static class Role extends MetaObject implements Permissable, Roleable
    {
        private static final long serialVersionUID = 1450294322887913505L;
        public String domain;
        public String roleName;
        public List<ObjectPermission> permissions;
        public List<UUID> roleUUIDs;
        
        public Role() {
            this.permissions = new ArrayList<ObjectPermission>();
            this.roleUUIDs = new ArrayList<UUID>();
            this.uuid = new UUID(System.currentTimeMillis());
            super.type = EntityType.ROLE;
        }
        
        public void construct(final Namespace ns, final String roleName) {
            this.construct(ns, roleName, null, null);
        }
        
        public void construct(final Namespace ns, final String roleName, final List<Role> roles, final List<ObjectPermission> permissions) {
            super.construct(roleName, ns, EntityType.ROLE);
            this.domain = ns.name;
            this.roleName = roleName;
            if (roles != null) {
                for (final Role r : roles) {
                    this.roleUUIDs.add(r.getUuid());
                }
            }
            if (permissions != null) {
                this.permissions = permissions;
            }
        }
        
        @Override
        public UUID getUuid() {
            return this.uuid;
        }
        
        public String getDomain() {
            return this.domain;
        }
        
        public String getRoleName() {
            return this.roleName;
        }
        
        @Override
        public List<ObjectPermission> getPermissions() {
            return this.permissions;
        }
        
        public List<UUID> getRoleUUIDs() {
            return this.roleUUIDs;
        }
        
        @JSON(include = false)
        @JsonIgnore
        public String getRole() {
            return this.domain + ":" + this.roleName;
        }
        
        @Override
        public String toString() {
            return this.domain + "." + this.roleName;
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof Role) {
                final Role other = (Role)obj;
                if (this.domain.equals(other.domain) && this.roleName.equals(other.roleName)) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public void grantPermission(final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
            if (this.permissions.contains(permission)) {
                return;
            }
            final ObjectPermission reversePermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
            if (this.permissions.contains(reversePermission)) {
                this.permissions.remove(reversePermission);
            }
            for (final ObjectPermission perm : this.getAllPermissions()) {
                if (perm.equals(permission)) {
                    return;
                }
            }
            this.permissions.add(permission);
        }
        
        @Override
        public void grantPermissions(final List<ObjectPermission> permissionsList) throws SecurityException, MetaDataRepositoryException {
            for (final ObjectPermission aPermissionsList : permissionsList) {
                this.grantPermission(aPermissionsList);
            }
        }
        
        @Override
        public void revokePermission(final ObjectPermission permToBeRevoked) throws SecurityException, MetaDataRepositoryException {
            final UserAndRoleUtility util = new UserAndRoleUtility();
            if (util.isDisallowedPermission(permToBeRevoked)) {
                throw new SecurityException("Revoking a disallowed permission is NOT allowed. Grant to remove it");
            }
            final boolean roleHasPermission = util.doesListHasImpliedPerm(this.permissions, permToBeRevoked);
            final boolean roleHasPermissionThruRole = util.doesListHasImpliedPerm(this.getPermissionsThruRoles(), permToBeRevoked);
            if (!roleHasPermission) {
                String exceptionString = "Role has no permission that implies : \"" + permToBeRevoked + "\". Can't revoke a permission that is NOT granted yet.";
                if (roleHasPermissionThruRole) {
                    exceptionString += "But role has that permission through another role. Revoke permission from that Role.";
                }
                throw new SecurityException(exceptionString);
            }
            final ObjectPermission revPermToBeRevoked = new ObjectPermission(permToBeRevoked, ObjectPermission.PermissionType.disallow);
            if (this.permissions.contains(revPermToBeRevoked)) {
                MetaInfo.logger.warn((Object)(revPermToBeRevoked + " permission already revoked."));
                return;
            }
            final boolean exactPermissionExists = util.doesListHasExactPerm(this.permissions, permToBeRevoked);
            if (exactPermissionExists) {
                this.permissions.remove(permToBeRevoked);
                final List<ObjectPermission> list = util.getDependentPerms(permToBeRevoked, this.permissions);
                for (final ObjectPermission perm : list) {
                    this.permissions.remove(perm);
                }
            }
            if (util.doesListHasImpliedPerm(this.permissions, permToBeRevoked)) {
                this.permissions.add(revPermToBeRevoked);
            }
        }
        
        @Override
        public void revokePermissions(final List<ObjectPermission> permissionsList) throws SecurityException, MetaDataRepositoryException {
            for (final ObjectPermission aPermissionsList : permissionsList) {
                this.revokePermission(aPermissionsList);
            }
        }
        
        @Override
        public void grantRole(final Role role) {
            if (this.uuid.equals((Object)role.getUuid())) {
                return;
            }
            if (this.roleUUIDs.contains(role.getUuid())) {
                return;
            }
            this.roleUUIDs.add(role.getUuid());
        }
        
        @Override
        public void grantRoles(final List<Role> roles) {
            if (roles == null || roles.size() == 0) {
                return;
            }
            for (final Role role : roles) {
                final UUID id = role.getUuid();
                if (!this.roleUUIDs.contains(id)) {
                    this.roleUUIDs.add(id);
                }
            }
        }
        
        @Override
        public void revokeRole(final Role role) {
            if (!this.roleUUIDs.contains(role.getUuid())) {
                return;
            }
            this.roleUUIDs.remove(role.getUuid());
        }
        
        @Override
        public void revokeRoles(final List<Role> roles) {
            if (roles == null || roles.size() == 0) {
                return;
            }
            for (final Role role : roles) {
                final UUID id = role.getUuid();
                if (this.roleUUIDs.contains(id)) {
                    this.roleUUIDs.remove(id);
                }
            }
        }
        
        @Override
        public List<Role> getRoles() throws MetaDataRepositoryException {
            List<Role> r1 = null;
            if (this.roleUUIDs != null) {
                r1 = new ArrayList<Role>();
                for (final UUID roleUUID : this.roleUUIDs) {
                    final Role r2 = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(roleUUID, HSecurityManager.TOKEN);
                    if (r2 != null) {
                        r1.add(r2);
                    }
                }
            }
            return r1;
        }
        
        @Override
        public void setRoles(final List<Role> roles) {
            if (roles != null) {
                for (final Role r : roles) {
                    this.roleUUIDs.add(r.getUuid());
                }
            }
        }
        
        @JsonIgnore
        public List<ObjectPermission> getPermissionsThruRoles() throws MetaDataRepositoryException {
            final List<ObjectPermission> perms = new ArrayList<ObjectPermission>();
            if (this.roleUUIDs != null && this.roleUUIDs.size() > 0) {
                for (final UUID id : this.roleUUIDs) {
                    final Role temprole = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(id, HSecurityManager.TOKEN);
                    if (temprole != null) {
                        perms.addAll(temprole.getAllPermissions());
                    }
                }
            }
            return perms;
        }
        
        @JsonIgnore
        @Override
        public List<ObjectPermission> getAllPermissions() throws MetaDataRepositoryException {
            final List<ObjectPermission> allPermissions = new ArrayList<ObjectPermission>();
            allPermissions.addAll(this.getPermissionsThruRoles());
            if (this.permissions != null) {
                allPermissions.addAll(this.permissions);
            }
            return allPermissions;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            String desc = this.startDescribe();
            desc += "ROLES {";
            if (this.roleUUIDs != null) {
                boolean notFirst = false;
                for (final UUID roleUUID : this.roleUUIDs) {
                    if (notFirst) {
                        desc += ", ";
                    }
                    else {
                        notFirst = true;
                    }
                    final Role r = (Role)MetadataRepository.getINSTANCE().getMetaObjectByUUID(roleUUID, HSecurityManager.TOKEN);
                    desc += r.toString();
                }
            }
            desc += "}\n";
            desc = desc + "PERMISSIONS " + this.convertToNewSyntax(this.getAllPermissions()) + "\n";
            return desc;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            json.put("roleName", (Object)this.getName());
            return json;
        }
        
        @JsonIgnore
        public boolean isGlobalAdminRole() {
            return this.nsName.equalsIgnoreCase("Global") && this.name.equals("admin");
        }
        
        public static Role deserialize(final JsonNode jsonNode) throws JsonParseException, JsonMappingException, IOException {
            if (jsonNode == null) {
                return null;
            }
            return MetaInfoJsonSerializer.RoleJsonSerializer.deserialize(jsonNode.toString());
        }
        
        public static Map buildActions() {
            MetaObject.buildBaseActions();
            final TextProperties domain_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final TextProperties role_name_property = MetaInfo.actionablePropertiesFactory.createTextProperties();
            final ObjectProperties permissions_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            final ObjectProperties roleids_property = MetaInfo.actionablePropertiesFactory.createObjectProperties();
            Role.actions.put("domain", domain_property);
            Role.actions.put("roleName", role_name_property);
            Role.actions.put("permissions", permissions_property);
            Role.actions.put("roleUUIDs", role_name_property);
            return Role.actions;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("domain", this.domain);
            parent.put("roleName", this.roleName);
            final ArrayNode permissions_array = this.jsonMapper.createArrayNode();
            final ArrayNode role_uuids_array = this.jsonMapper.createArrayNode();
            parent.set("permissions", (JsonNode)permissions_array);
            parent.set("roleUUIDs", (JsonNode)role_uuids_array);
            return parent;
        }
    }
    
    public static class Initializer extends MetaObject
    {
        private static final long serialVersionUID = 134312423423234L;
        public String WAClusterName;
        public String MetaDataRepositoryLocation;
        public String MetaDataRepositoryDBname;
        public String MetaDataRepositoryUname;
        public String MetaDataRepositoryPass;
        public String WAClusterPassword;
        public String ProductKey;
        public String CompanyName;
        public String LicenseKey;
        private static final String initName = "Initializer";
        
        public Initializer() {
            this.WAClusterName = null;
            this.MetaDataRepositoryLocation = null;
            this.MetaDataRepositoryDBname = null;
            this.MetaDataRepositoryUname = null;
            this.MetaDataRepositoryPass = null;
            this.WAClusterPassword = null;
            this.ProductKey = null;
            this.CompanyName = null;
            this.LicenseKey = null;
        }
        
        public void construct(final String WAClusterName, final String WAClusterPassword, final String MetaDataRepositoryLocation, final String MetaDataRepositoryDBname, final String MetaDataRepositoryUname, final String MetaDataRepositoryPass, final String ProductKey, final String CompanyName, final String LicenseKey) {
            super.construct("Initializer", MetaInfo.GlobalNamespace, EntityType.INITIALIZER);
            this.WAClusterName = WAClusterName;
            this.WAClusterPassword = WAClusterPassword;
            this.MetaDataRepositoryLocation = MetaDataRepositoryLocation;
            this.MetaDataRepositoryDBname = MetaDataRepositoryDBname;
            this.MetaDataRepositoryUname = MetaDataRepositoryUname;
            this.MetaDataRepositoryPass = MetaDataRepositoryPass;
            this.ProductKey = ProductKey;
            this.CompanyName = CompanyName;
            this.LicenseKey = LicenseKey;
        }
        
        public String getWAClusterName() {
            return this.WAClusterName;
        }
        
        public void setWAClusterName(final String wAClusterName) {
            this.WAClusterName = wAClusterName;
        }
        
        public String getMetaDataRepositoryLocation() {
            return this.MetaDataRepositoryLocation;
        }
        
        public void setMetaDataRepositoryLocation(final String metaDataRepositoryLocation) {
            this.MetaDataRepositoryLocation = metaDataRepositoryLocation;
        }
        
        public String getMetaDataRepositoryDBname() {
            return this.MetaDataRepositoryDBname;
        }
        
        public void setMetaDataRepositoryDBname(final String metaDataRepositoryDBname) {
            this.MetaDataRepositoryDBname = metaDataRepositoryDBname;
        }
        
        public String getMetaDataRepositoryUname() {
            return this.MetaDataRepositoryUname;
        }
        
        public void setMetaDataRepositoryUname(final String metaDataRepositoryUname) {
            this.MetaDataRepositoryUname = metaDataRepositoryUname;
        }
        
        public String getMetaDataRepositoryPass() {
            return this.MetaDataRepositoryPass;
        }
        
        public void setMetaDataRepositoryPass(final String metaDataRepositoryPass) {
            this.MetaDataRepositoryPass = metaDataRepositoryPass;
        }
        
        public String getWAClusterPassword() {
            return this.WAClusterPassword;
        }
        
        public void setWAClusterPassword(final String wAClusterPassword) {
            this.WAClusterPassword = wAClusterPassword;
        }
        
        public String getProductKey() {
            return this.ProductKey;
        }
        
        public void setProductKey(final String productKey) {
            this.ProductKey = productKey;
        }
        
        public String getCompanyName() {
            return this.CompanyName;
        }
        
        public void setCompanyName(final String companyName) {
            this.CompanyName = companyName;
        }
        
        public String getLicenseKey() {
            return this.LicenseKey;
        }
        
        public void setLicenseKey(final String licenseKey) {
            this.LicenseKey = licenseKey;
        }
        
        public boolean authenticate(final String password) {
            return this.WAClusterPassword.equals(password);
        }
        
        public void currentData() {
            if (MetaInfo.logger.isInfoEnabled()) {
                MetaInfo.logger.info((Object)("Current initializer Object: Cluster name: " + this.WAClusterName + " , Metadata repository location: " + this.MetaDataRepositoryLocation + " , Metadata repository name: " + this.MetaDataRepositoryDBname + " , Metadata repository user name: " + this.MetaDataRepositoryUname + " , "));
            }
        }
        
        public boolean authenticate(final String WAClusterName, final String WAClusterPassword) {
            boolean b = false;
            if (this.WAClusterName.equals(WAClusterName) && this.WAClusterPassword.equals(WAClusterPassword)) {
                b = true;
            }
            return b;
        }
        
        @Override
        public String describe(final AuthToken token) {
            String desc = "CLUSTER INFORMATION\n";
            desc = desc + "\tCLUSTER NAME " + this.WAClusterName + "\n";
            if (this.MetaDataRepositoryLocation != null) {
                desc += "\tDATABASE USED FOR METADATA\n";
                desc = desc + "\tDATABASE LOCATION " + this.MetaDataRepositoryLocation + "\n";
                desc = desc + "\tDATABASE NAME " + this.MetaDataRepositoryDBname + "\n";
            }
            desc += "\nLICENSE INFORMATION\n";
            desc = desc + "\tCOMPANY NAME " + this.CompanyName + "\n";
            final LicenseManager licenseManager = LicenseManager.get();
            try {
                licenseManager.setProductKey(this.CompanyName, this.ProductKey);
            }
            catch (RuntimeException e) {
                licenseManager.setProductKey(this.WAClusterName, this.ProductKey);
            }
            licenseManager.setLicenseKey(this.LicenseKey);
            desc = desc + "\tLICENSE ALLOWS " + licenseManager.getClusterSize() + " CORES IN CLUSTER\n";
            desc = desc + "\tLICENSE ALLOWS " + licenseManager.getNumberOfNodes() + " SERVER INSTANCES\n";
            if (LicenseManager.get().allowsAllOptions()) {
                desc = desc + "\tLICENSED FOR " + LicenseManager.Option.CompanyLicense.name() + " \n";
            }
            else {
                String licenseOptions = licenseManager.getLicenseOptions().toString();
                licenseOptions = licenseOptions.replace("[", "").replace("]", "").trim();
                desc += licenseManager.getExpiry().toUpperCase();
                desc = desc + "\tLICENSED FOR " + licenseOptions + "\n";
                desc += "\tALLOWED ADAPTERS ";
                if (LicenseManager.get().allowsOption(LicenseManager.Option.AzureSQLServer)) {
                    desc += Arrays.toString(com.datasphere.runtime.Server.azureSQLServerTemplates).replace("[", "").replace("]", "").trim();
                }
                if (LicenseManager.get().allowsOption(LicenseManager.Option.AzureHDInsight)) {
                    desc += Arrays.toString(com.datasphere.runtime.Server.azureHDInsightTemplates).replace("[", "").replace("]", "").trim();
                }
                if (LicenseManager.get().allowsOption(LicenseManager.Option.AzureStorage)) {
                    desc += Arrays.toString(com.datasphere.runtime.Server.azureSQLServerTemplates).replace("[", "").replace("]", "").trim();
                }
                if (LicenseManager.get().allowsOption(LicenseManager.Option.AzureEventHub)) {
                    desc += Arrays.toString(com.datasphere.runtime.Server.azureSQLServerTemplates).replace("[", "").replace("]", "").trim();
                }
            }
            desc = desc + "\tPRODUCT KEY " + this.ProductKey + "\n";
            desc = desc + "\tLICENSE KEY " + this.LicenseKey + "\n";
            return desc;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("clusterName", this.WAClusterName);
            parent.put("companyName", this.CompanyName);
            parent.put("productKey", this.ProductKey);
            parent.put("licenseKey", this.LicenseKey);
            final LicenseManager lm = LicenseManager.get();
            try {
                lm.setProductKey(this.CompanyName, this.ProductKey);
            }
            catch (RuntimeException e) {
                lm.setProductKey(this.WAClusterName, this.ProductKey);
            }
            lm.setLicenseKey(this.LicenseKey);
            final String expiry = lm.getExpiry();
            parent.put("expiry", expiry);
            return parent;
        }
    }
    
    public static class Visualization extends MetaObject
    {
        private static final long serialVersionUID = -6868010700688028465L;
        private String visName;
        private String json;
        public String fname;
        
        public Visualization() {
            super.uuid = new UUID(System.currentTimeMillis());
            super.type = EntityType.VISUALIZATION;
        }
        
        public void construct(final String visName, final Namespace ns, final String json, final String fname) {
            super.construct(visName, ns, EntityType.VISUALIZATION);
            this.visName = visName;
            this.json = json;
            this.fname = fname;
        }
        
        public String getVisName() {
            return this.visName;
        }
        
        public void setVisName(final String visName) {
            this.visName = visName;
        }
        
        public String getJson() {
            return this.json;
        }
        
        public void setJson(final String json) {
            this.json = json;
        }
        
        @Override
        public String toString() {
            return this.json;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class DeploymentGroup extends MetaObject
    {
        private static final long serialVersionUID = 8369271979074176474L;
        public final List<String> configuredMembers;
        public long minimumRequiredServers;
        public long maxApps;
        @JsonIgnore
        public final Map<UUID, Long> groupMembers;
        
        public long getMaxApps() {
            return this.maxApps;
        }
        
        public void setMaxApps(final Long maxApps) {
            if (maxApps != null) {
                this.maxApps = maxApps;
            }
        }
        
        public DeploymentGroup() {
            this.configuredMembers = new ArrayList<String>();
            this.minimumRequiredServers = 0L;
            this.maxApps = 2147483647L;
            this.groupMembers = Factory.makeLinkedMap();
        }
        
        public void construct(final String name) {
            super.construct(name, MetaInfo.GlobalNamespace, EntityType.DG);
        }
        
        public void addConfiguredMembers(final List<String> servers) {
            this.configuredMembers.removeAll(servers);
            this.configuredMembers.addAll(servers);
            MetaInfo.logger.info((Object)("Adding Configured members " + servers + " to DG " + this.name + ". " + this.toString()));
        }
        
        public void removeConfiguredMember(final List<String> servers) {
            this.configuredMembers.removeAll(servers);
            MetaInfo.logger.info((Object)("Removing Configured members " + servers + " from DG " + this.name + ". " + this.toString()));
        }
        
        public void addMembers(final List<UUID> uuids, final long serverId) {
            for (final UUID uuid : uuids) {
                this.groupMembers.put(uuid, serverId);
            }
            MetaInfo.logger.info((Object)("adding member(s) " + uuids + " to DG " + this.name + ". " + this.toString()));
        }
        
        public void removeMember(final UUID memberUUID) {
            this.groupMembers.remove(memberUUID);
            MetaInfo.logger.info((Object)("Removing member " + memberUUID + " from DG " + this.name + ". " + this.toString()));
        }
        
        public long getMinimumRequiredServers() {
            return this.minimumRequiredServers;
        }
        
        public void setMinimumRequiredServers(final Long minimumRequiredServers) {
            if (minimumRequiredServers != null) {
                this.minimumRequiredServers = minimumRequiredServers;
            }
        }
        
        public boolean isAutomaticDG() {
            return this.configuredMembers.isEmpty();
        }
        
        @Override
        public String toString() {
            return this.name + " :Actual: " + this.groupMembers + " :Configured: " + this.configuredMembers + " Minimum Servers: " + this.minimumRequiredServers + " limit applications: " + this.maxApps;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            final List<String> servers = new ArrayList<String>();
            for (final UUID sId : this.groupMembers.keySet()) {
                final Server s = (Server)MetadataRepository.getINSTANCE().getMetaObjectByUUID(sId, HSecurityManager.TOKEN);
                servers.add(s.name);
            }
            return this.startDescribe() + "Servers: " + this.configuredMembers + " MINIMUM SERVERS: " + this.minimumRequiredServers + " LIMIT APPLICATIONS: " + this.maxApps;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class Server extends MetaObject
    {
        private static final long serialVersionUID = -4815356066614655011L;
        private static Random rng;
        private static final String characters = "qwertyuiopasdfghjklzxcvbnm";
        public Map<String, Set<UUID>> currentUUIDs;
        public int numCpus;
        public String macAdd;
        public String version;
        public Initializer initializer;
        public Set<UUID> deploymentGroupsIDs;
        public long id;
        public boolean isAgent;
        public String webBaseUri;
        
        public Server() {
            this.currentUUIDs = new ConcurrentHashMap<String, Set<UUID>>();
            this.deploymentGroupsIDs = new HashSet<UUID>();
            this.numCpus = 0;
            this.version = null;
            this.initializer = null;
            this.id = 0L;
            this.isAgent = false;
            this.webBaseUri = null;
        }
        
        public void construct(final UUID uuid, final String name, final int numCpus, final String version, final Initializer initializer, final long id, final boolean isAgent) {
            super.construct(name, uuid, MetaInfo.GlobalNamespace, EntityType.SERVER);
            this.numCpus = numCpus;
            this.version = version;
            this.initializer = initializer;
            this.id = id;
            this.isAgent = isAgent;
            this.webBaseUri = null;
        }
        
        public String getWebBaseUri() {
            return this.webBaseUri;
        }
        
        public void setWebBaseUri(final String webBaseUri) {
            this.webBaseUri = webBaseUri;
        }
        
        public Map<String, Set<UUID>> getCurrentUUIDs() {
            return this.currentUUIDs;
        }
        
        public void setCurrentUUIDs(final Map<String, Set<UUID>> currentUUIDs) {
            this.currentUUIDs = currentUUIDs;
        }
        
        public int getNumCpus() {
            return this.numCpus;
        }
        
        public String getMacAdd() {
            return this.macAdd;
        }
        
        public void setMacAdd(final String macAdd) {
            this.macAdd = macAdd;
        }
        
        public void setNumCpus(final int numCpus) {
            this.numCpus = numCpus;
        }
        
        public String getVersion() {
            return this.version;
        }
        
        public void setVersion(final String version) {
            this.version = version;
        }
        
        public Initializer getInitializer() {
            return this.initializer;
        }
        
        public void setInitializer(final Initializer initializer) {
            this.initializer = initializer;
        }
        
        public Set<UUID> getDeploymentGroupsIDs() {
            return this.deploymentGroupsIDs;
        }
        
        public void setDeploymentGroupsIDs(final Set<UUID> deploymentGroupsIDs) {
            this.deploymentGroupsIDs = deploymentGroupsIDs;
        }
        
        public long getId() {
            return this.id;
        }
        
        public void setId(final long id) {
            this.id = id;
        }
        
        private Set<UUID> getCurrentObjectsInServer(final String applicationName) throws MetaDataRepositoryException {
            Set<UUID> uuids = this.currentUUIDs.get(applicationName);
            if (uuids == null) {
                uuids = Collections.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());
                this.currentUUIDs.put(applicationName, uuids);
            }
            final Iterator<UUID> it = uuids.iterator();
            while (it.hasNext()) {
                final UUID uuid = it.next();
                final MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                if (obj == null) {
                    it.remove();
                }
            }
            return uuids;
        }
        
        public void addCurrentObjectsInServer(final String applicationName, final List<MetaObjectInfo> added) throws MetaDataRepositoryException {
            final Set<UUID> uuids = this.getCurrentObjectsInServer(applicationName);
            for (final MetaObjectInfo obj : added) {
                uuids.add(obj.uuid);
            }
        }
        
        public void removeCurrentObjectsInServer(final String applicationName, final List<MetaObjectInfo> removed) throws MetaDataRepositoryException {
            final Set<UUID> uuids = this.getCurrentObjectsInServer(applicationName);
            for (final MetaObjectInfo obj : removed) {
                uuids.remove(obj.uuid);
            }
        }
        
        public Map<String, List<MetaObject>> getCurrentObjects() throws MetaDataRepositoryException {
            final Map<String, List<MetaObject>> ret = new HashMap<String, List<MetaObject>>();
            for (final String application : this.currentUUIDs.keySet()) {
                final List<MetaObject> retM = new ArrayList<MetaObject>();
                ret.put(application, retM);
                final Set<UUID> uuids = this.currentUUIDs.get(application);
                for (final UUID uuid : uuids) {
                    final MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                    if (obj != null) {
                        retM.add(obj);
                        if (!MetaInfo.logger.isDebugEnabled()) {
                            continue;
                        }
                        MetaInfo.logger.debug((Object)(" Server has meta object " + obj.uri + " as part of " + application));
                    }
                }
            }
            return ret;
        }
        
        @Override
        public String toString() {
            Map<String, List<MetaObject>> currObjs = null;
            try {
                currObjs = this.getCurrentObjects();
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.error((Object)e.getMessage());
            }
            return this.uuid + " : " + currObjs + ": total objects : " + currObjs.size();
        }
        
        public static String generateString(final int length) {
            final char[] text = new char[length];
            for (int i = 0; i < length; ++i) {
                text[i] = "qwertyuiopasdfghjklzxcvbnm".charAt(Server.rng.nextInt("qwertyuiopasdfghjklzxcvbnm".length()));
            }
            return new String(text);
        }
        
        @Override
        public String describe(final AuthToken token) {
            String desc = (this.isAgent ? "AGENT" : "SERVER ") + this.name + " [" + this.uuid + "]\n";
            desc = desc + "IN GROUPS " + this.deploymentGroupsIDs + "\n";
            desc += "DEPLOYED OBJECTS [\n";
            Map<String, List<MetaObject>> objects = null;
            try {
                objects = this.getCurrentObjects();
            }
            catch (MetaDataRepositoryException e) {
                MetaInfo.logger.error((Object)e.getMessage());
            }
            for (final Map.Entry<String, List<MetaObject>> entry : objects.entrySet()) {
                final List<MetaObject> objs = entry.getValue();
                if (objs != null) {
                    for (final MetaObject obj : objs) {
                        desc = desc + "  (" + entry.getKey() + ") " + obj.startDescribe();
                    }
                }
            }
            desc += "]";
            return desc;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("version", this.version);
            parent.put("isAgent", this.isAgent);
            final ArrayNode dNode = this.jsonMapper.createArrayNode();
            for (final UUID uuid : this.deploymentGroupsIDs) {
                dNode.add(uuid.toString());
            }
            parent.set("deploymentGroupsIDs", (JsonNode)dNode);
            if (!this.isAgent) {
                parent.set("initializer", (JsonNode)this.initializer.getJsonForClient());
            }
            return parent;
        }
        
        static {
            Server.rng = new Random();
        }
    }
    
    @Deprecated
    public static class ExceptionHandler extends MetaObject
    {
        private static final long serialVersionUID = 5142558263978570020L;
        public List<String> exceptions;
        public List<String> components;
        public String cmd;
        public Map<String, Object> properties;
        
        public void construct(final String name, final Namespace ns, final List<String> exceptions, final List<String> components, final ActionType cmd, final List<Property> props) {
            super.construct(name, ns, EntityType.EXCEPTIONHANDLER);
            this.exceptions = exceptions;
            this.components = components;
            this.cmd = cmd.toString();
            this.properties = new HashMap<String, Object>();
            if (props != null && props.size() > 0) {
                for (final Property prop : props) {
                    this.properties.put(prop.name, prop.value);
                }
            }
        }
        
        public List<String> getExceptions() {
            return this.exceptions;
        }
        
        public void setExceptions(final List<String> exceptions) {
            this.exceptions = exceptions;
        }
        
        public List<String> getComponents() {
            return this.components;
        }
        
        public void setComponents(final List<String> components) {
            this.components = components;
        }
        
        public String getCmd() {
            return this.cmd;
        }
        
        public void setCmd(final String cmd) {
            this.cmd = cmd;
        }
        
        public Map<String, Object> getProperties() {
            return this.properties;
        }
        
        public void setProperties(final Map<String, Object> properties) {
            this.properties = properties;
        }
        
        @Override
        public String toString() {
            return "ExceptionHandler [exceptions=" + this.exceptions + ", components=" + this.components + ", cmd=" + this.cmd + ", properties=" + this.properties + "]";
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
    }
    
    public static class Sorter extends MetaObject
    {
        private static final long serialVersionUID = -8887902378355417094L;
        public Interval sortTimeInterval;
        public List<SorterRule> inOutRules;
        public UUID errorStream;
        
        public Sorter() {
            this.sortTimeInterval = null;
            this.inOutRules = null;
            this.errorStream = null;
        }
        
        public void construct(final String name, final Namespace ns, final Interval sortTimeInterval, final List<SorterRule> inOutRules, final UUID errorStream) {
            super.construct(name, ns, EntityType.SORTER);
            this.sortTimeInterval = sortTimeInterval;
            this.inOutRules = inOutRules;
            this.errorStream = errorStream;
        }
        
        public static Map buildActions() {
            return MetaObject.buildBaseActions();
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            if (this.inOutRules != null) {
                for (final SorterRule rule : this.inOutRules) {
                    graph.get(rule.inStream).add(this.getUuid());
                    graph.get(rule.outStream).add(this.getUuid());
                }
            }
            return super.inEdges(graph);
        }
        
        @Override
        public Graph<UUID, Set<UUID>> exportOrder(@NotNull final Graph<UUID, Set<UUID>> graph) {
            if (this.inOutRules != null) {
                for (final SorterRule rule : this.inOutRules) {
                    graph.get(rule.inStream).add(this.getUuid());
                    graph.get(rule.outStream).add(this.getUuid());
                }
            }
            return super.exportOrder(graph);
        }
        
        public static class SorterRule implements Serializable
        {
            private static final long serialVersionUID = 3988496773069385192L;
            public UUID inStream;
            public UUID outStream;
            public String inStreamField;
            public UUID inOutType;
            
            public SorterRule(final UUID inStream, final UUID outStream, final String inStreamField, final UUID inOutType) {
                this.inStream = inStream;
                this.outStream = outStream;
                this.inStreamField = inStreamField;
                this.inOutType = inOutType;
            }
            
            public void construct(final UUID inStream, final UUID outStream, final String inStreamField, final UUID inOutType) {
                this.inStream = inStream;
                this.outStream = outStream;
                this.inStreamField = inStreamField;
                this.inOutType = inOutType;
            }
            
            public UUID getInStream() {
                return this.inStream;
            }
            
            public void setInStream(final UUID inStream) {
                this.inStream = inStream;
            }
            
            public UUID getOutStream() {
                return this.outStream;
            }
            
            public void setOutStream(final UUID outStream) {
                this.outStream = outStream;
            }
            
            public String getInStreamField() {
                return this.inStreamField;
            }
            
            public void setInStreamField(final String inStreamField) {
                this.inStreamField = inStreamField;
            }
            
            public UUID getInOutType() {
                return this.inOutType;
            }
            
            public void setInOutType(final UUID inOutType) {
                this.inOutType = inOutType;
            }
        }
    }
    
    public static class WAStoreView extends MetaObject
    {
        private static final long serialVersionUID = 1358685338132027542L;
        public UUID wastoreID;
        public IntervalPolicy viewSize;
        public Boolean isJumping;
        public boolean subscribeToUpdates;
        public byte[] query;
        
        public void construct(final String name, final Namespace ns, final UUID wastoreID, final IntervalPolicy viewSize, final Boolean isJumping, final boolean subscribeToUpdates) {
            super.construct(name, ns, EntityType.WASTOREVIEW);
            this.wastoreID = wastoreID;
            this.viewSize = viewSize;
            this.isJumping = isJumping;
            this.query = null;
            this.subscribeToUpdates = subscribeToUpdates;
            this.getMetaInfoStatus().setAnonymous(true);
        }
        
        public void setQuery(final byte[] query) {
            this.query = (byte[])((query != null) ? ((byte[])query.clone()) : null);
        }
        
        public boolean hasQuery() {
            return this.query != null;
        }
        
        public UUID getWastoreID() {
            return this.wastoreID;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            return parent;
        }
        
        @Override
        public Map<UUID, Set<UUID>> inEdges(final Graph<UUID, Set<UUID>> graph) {
            graph.get(this.wastoreID).add(this.getUuid());
            return super.inEdges(graph);
        }
    }
    
    public static class Dashboard extends MetaObject
    {
        String title;
        String defaultLandingPage;
        List<String> pages;
        
        public String getTitle() {
            return this.title;
        }
        
        public void setTitle(final String title) {
            this.title = title;
        }
        
        public List<String> getPages() {
            return this.pages;
        }
        
        public void setPages(final List<String> pages) {
            this.pages = pages;
        }
        
        public String getDefaultLandingPage() {
            return this.defaultLandingPage;
        }
        
        public void setDefaultLandingPage(final String defaultLandingPage) {
            this.defaultLandingPage = defaultLandingPage;
        }
        
        public void addPage(final String page) {
            if (this.pages == null) {
                this.pages = new ArrayList<String>();
            }
            this.pages.add(page);
        }
        
        public void removePage(final String page) {
            if (this.pages != null) {
                this.pages.remove(page);
            }
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            final List<String> pages = this.getPages();
            final JSONArray pagesJ = new JSONArray();
            if (pages != null && pages.size() > 0) {
                for (final String u : pages) {
                    pagesJ.put((Object)u);
                }
            }
            json.put("title", (Object)this.getTitle());
            json.put("pages", (Object)pagesJ);
            json.put("defaultLandingPage", (Object)this.getDefaultLandingPage());
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("title", this.title);
            parent.put("defaultLandingPage", this.defaultLandingPage);
            final ArrayNode pagesArrayNode = this.jsonMapper.createArrayNode();
            if (this.pages != null && this.pages.size() > 0) {
                for (final String cup : this.pages) {
                    pagesArrayNode.add(cup);
                }
            }
            parent.set("pages", (JsonNode)pagesArrayNode);
            return parent;
        }
        
        @Override
        public String describe(final AuthToken token) throws MetaDataRepositoryException {
            final StringBuilder sb = new StringBuilder(super.describe(token));
            final List<String> pages = this.getPages();
            sb.append(this.getFullName()).append(": Pages in this Dashboard [").append("\n");
            for (String pageName : pages) {
                sb.append(pageName).append("\n").append("QueryVisualizations: [").append("\n");
                String pageNamespace = this.getNsName();
                if (Utility.checkIfFullName(pageName)) {
                    pageName = Utility.splitName(pageName);
                    pageNamespace = Utility.splitDomain(pageName);
                }
                final Page pageMetaObject = (Page)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PAGE, pageNamespace, pageName, null, HSecurityManager.TOKEN);
                if (pageMetaObject == null) {
                    if (!MetaInfo.logger.isDebugEnabled()) {
                        continue;
                    }
                    MetaInfo.logger.debug((Object)("Page not found with name: " + pageName + " in namespace: " + pageNamespace));
                }
                else {
                    final List<String> queryVisualizations = pageMetaObject.getQueryVisualizations();
                    for (final String queryVisualizationName : queryVisualizations) {
                        final QueryVisualization queryVisualizationMeta = (QueryVisualization)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.QUERYVISUALIZATION, this.getNsName(), queryVisualizationName, null, HSecurityManager.TOKEN);
                        final String query = queryVisualizationMeta.getQuery();
                        sb.append(queryVisualizationName).append("\n").append("Query: ").append(query).append("\n");
                    }
                    sb.append("]").append("\n");
                }
            }
            sb.append("]").append("\n");
            return sb.toString();
        }
    }
    
    public static class Page extends MetaObject
    {
        String title;
        String gridJSON;
        List<String> queryVisualizations;
        
        public String getTitle() {
            return this.title;
        }
        
        public void setTitle(final String title) {
            this.title = title;
        }
        
        public List<String> getQueryVisualizations() {
            return this.queryVisualizations;
        }
        
        public void setQueryVisualizations(final List<String> queryVisualizations) {
            this.queryVisualizations = queryVisualizations;
        }
        
        public void addVisualization(final String visualization) {
            if (this.queryVisualizations == null) {
                this.queryVisualizations = new ArrayList<String>(0);
            }
            if (visualization != null) {
                this.queryVisualizations.add(visualization);
            }
        }
        
        public void removeVisualization(final String visualization) {
            if (visualization != null && this.queryVisualizations != null) {
                this.queryVisualizations.remove(visualization);
            }
        }
        
        public String getGridJSON() {
            return this.gridJSON;
        }
        
        public void setGridJSON(final String grid) {
            this.gridJSON = grid;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            json.put("title", (Object)this.getTitle());
            final List<String> visualizations = this.getQueryVisualizations();
            final JSONArray componentsJ = new JSONArray();
            if (visualizations != null && visualizations.size() > 0) {
                for (final String u : visualizations) {
                    componentsJ.put((Object)u);
                }
            }
            json.put("queryVisualizations", (Object)componentsJ);
            json.put("gridJSON", (Object)this.getGridJSON());
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("title", this.title);
            parent.put("gridJSON", this.gridJSON);
            final ArrayNode qvNode = this.jsonMapper.createArrayNode();
            final List<String> visualizations = this.getQueryVisualizations();
            if (visualizations != null && visualizations.size() > 0) {
                for (final String cuv : visualizations) {
                    qvNode.add(cuv);
                }
            }
            parent.set("queryVisualizations", (JsonNode)qvNode);
            return parent;
        }
    }
    
    public static class QueryVisualization extends MetaObject
    {
        String title;
        String query;
        String visualizationType;
        String config;
        
        public QueryVisualization() {
        }
        
        public QueryVisualization(final String name, final UUID uuid, final String nsName, final UUID namespaceId) {
            super.construct(name, uuid, nsName, namespaceId, EntityType.QUERYVISUALIZATION);
        }
        
        public String getTitle() {
            return this.title;
        }
        
        public void setTitle(final String title) {
            this.title = title;
        }
        
        public String getQuery() {
            return this.query;
        }
        
        public void setQuery(final String query) {
            this.query = query;
        }
        
        public String getVisualizationType() {
            return this.visualizationType;
        }
        
        public void setVisualizationType(final String visualizationType) {
            this.visualizationType = visualizationType;
        }
        
        public String getConfig() {
            return this.config;
        }
        
        public void setConfig(final String config) {
            this.config = config;
        }
        
        @Override
        public JSONObject JSONify() throws JSONException {
            final JSONObject json = super.JSONify();
            json.put("title", (Object)this.title);
            json.put("query", (Object)this.query);
            json.put("visualizationType", (Object)this.visualizationType);
            json.put("config", (Object)this.config);
            return json;
        }
        
        @JsonIgnore
        @Override
        public ObjectNode getJsonForClient() throws Exception {
            final ObjectNode parent = super.getJsonForClient();
            parent.put("title", this.title);
            if (this.query.startsWith("{")) {
                try {
                    final JsonNode jsonNode = this.jsonMapper.readTree(this.query);
                    parent.set("query", jsonNode);
                }
                catch (IOException e) {
                    MetaInfo.logger.error((Object)e.getMessage(), (Throwable)e);
                }
            }
            else {
                parent.put("query", this.query);
            }
            parent.put("visualizationType", this.visualizationType);
            parent.put("config", this.config);
            return parent;
        }
    }
}
