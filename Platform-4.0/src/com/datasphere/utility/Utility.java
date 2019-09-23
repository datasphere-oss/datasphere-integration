package com.datasphere.utility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.LocalTime;
import org.json.JSONException;
import org.json.JSONObject;

import com.datasphere.anno.PropertyTemplate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.Interval;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.HStorePersistencePolicy;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.compiler.stmts.MappedStream;
import com.datasphere.runtime.compiler.stmts.OutputClause;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.Type;

public class Utility
{
    private static Logger logger;
    
    public static String convertToFullQualifiedName(final MetaInfo.Namespace namespace, final String name) {
        if (name.contains(".")) {
            return name;
        }
        if (namespace == null) {
            throw new RuntimeException("Name is not fully qualified name and also namespace is not supplied");
        }
        return namespace.getName() + "." + name;
    }
    
    public static List<String> convertStringToRoleFormat(final List<String> roles) {
        final List<String> convertedResult = new ArrayList<String>();
        for (final String string : roles) {
            convertedResult.add(convertStringToRoleFormat(string));
        }
        return convertedResult;
    }
    
    public static String convertStringToRoleFormat(final String role) {
        final String[] splitRole;
        if ((splitRole = role.split("\\.")).length == 2) {
            return splitRole[0] + ":" + splitRole[1];
        }
        if (role.split(":").length == 2) {
            return role;
        }
        throw new RuntimeException("Wrong role format, either chose 'namespacename:rolename' or namespacename.rolename");
    }
    
    public static ObjectPermission.ObjectType getPermissionObjectType(final EntityType et) {
        switch (et) {
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
            case STREAM_GENERATOR: {
                return ObjectPermission.ObjectType.stream_generator;
            }
            default: {
                return null;
            }
        }
    }
    
    public static String splitName(final String name) {
        if (!checkIfFullName(name)) {
            return name;
        }
        final String[] splitName = name.split("\\.");
        if (splitName.length == 2) {
            return splitName[1];
        }
        if (splitName.length == 3) {
            return splitName[2];
        }
        return name;
    }
    
    public static String splitDomain(final String name) {
        if (!checkIfFullName(name)) {
            return null;
        }
        final String[] splitName = name.split("\\.");
        if (splitName.length >= 2) {
            return splitName[0];
        }
        return name;
    }
    
    public static boolean checkIfFullName(final String name) {
        return name != null && name.contains(".");
    }
    
    public static void removeDroppedFlow(final List<MetaInfo.Flow> result) {
        final Iterator<MetaInfo.Flow> it = result.iterator();
        while (it.hasNext()) {
            final MetaInfo.MetaObject metaObject = it.next();
            if (metaObject.getMetaInfoStatus().isDropped()) {
                it.remove();
            }
        }
    }
    
    public static void removeAdhocNamedQuery(final List<MetaInfo.Flow> applicationsList) {
        final Iterator<MetaInfo.Flow> it = applicationsList.iterator();
        while (it.hasNext()) {
            final MetaInfo.MetaObject metaObject = it.next();
            if (metaObject.getMetaInfoStatus().isAnonymous()) {
                it.remove();
            }
        }
    }
    
    public static Set<? extends MetaInfo.MetaObject> removeInternalApplicationsWithTypes(final Set<MetaInfo.MetaObject> obj, final boolean allowAnonTypes) {
        final Iterator<MetaInfo.MetaObject> it = obj.iterator();
        while (it.hasNext()) {
            final MetaInfo.MetaObject metaObject = it.next();
            if (metaObject.getMetaInfoStatus().isAdhoc() || metaObject.getName().startsWith(Compiler.NAMED_QUERY_PREFIX)) {
                it.remove();
            }
            else {
                if (!metaObject.getMetaInfoStatus().isAnonymous() || metaObject.getMetaInfoStatus().isGenerated()) {
                    continue;
                }
                if (metaObject.getType() != EntityType.TYPE) {
                    it.remove();
                }
                else {
                    if (allowAnonTypes) {
                        continue;
                    }
                    it.remove();
                }
            }
        }
        return obj;
    }
    
    public static Set<? extends MetaInfo.MetaObject> removeInternalApplications(final Set<MetaInfo.MetaObject> obj) {
        return removeInternalApplicationsWithTypes(obj, false);
    }
    
    public static void removeDroppedObjects(final List<MetaInfo.MetaObject> result) {
        final Iterator<MetaInfo.MetaObject> it = result.iterator();
        while (it.hasNext()) {
            final MetaInfo.MetaObject metaObject = it.next();
            if (metaObject.getMetaInfoStatus().isDropped()) {
                it.remove();
            }
        }
    }
    
    public static boolean isValidJson(final String json) {
        try {
            final JsonParser jp = new JsonParser();
            jp.parse(json);
        }
        catch (JsonSyntaxException ex) {
            return false;
        }
        return true;
    }
    
    public static String addJsonStrings(final String firstJson, final String secondJson) {
        String output = "";
        try {
            final JSONObject first = new JSONObject(firstJson);
            final JSONObject second = new JSONObject(secondJson);
            final Iterator keys = second.keys();
            while (keys.hasNext()) {
                final String key = (String)keys.next();
                first.put(key, second.get(key));
            }
            output = first.toString();
        }
        catch (JSONException e) {
            Utility.logger.warn((Object)e);
        }
        return output;
    }
    
    public static String getPrettyJsonString(final String uglyJSONString) {
        final Gson gson = new GsonBuilder().setPrettyPrinting().create();
        final JsonParser jp = new JsonParser();
        final JsonElement je = jp.parse(uglyJSONString);
        return gson.toJson(je);
    }
    
    public static void insertOrAppendValueInJSON(final JSONObject target, final String key, final String value) {
        try {
            final String oldValue = target.getString(key);
            target.put(key, (Object)addJsonStrings(oldValue, value));
        }
        catch (JSONException e) {
            try {
                target.put(key, (Object)value);
            }
            catch (JSONException e2) {
                Utility.logger.error((Object)("Couldn't insert json: " + value));
            }
        }
    }
    
    public static String prettyPrintMap(final List<Property> propertyList) {
        String str = "";
        if (propertyList == null || propertyList.isEmpty()) {
            return str;
        }
        final Iterator<Property> i = propertyList.iterator();
        boolean firstPass = true;
        while (i.hasNext()) {
            final Property e = i.next();
            final String key = e.name;
            final Object value = e.value;
            if (value == null) {
                continue;
            }
            if (!firstPass) {
                str += ",\n";
            }
            firstPass = false;
            if (value instanceof Boolean) {
                str = str + "  " + key + ": " + value;
            }
            else if (value instanceof Integer) {
                if ((int)value < 0) {
                    str = str + "  " + key + ": '" + value + "'";
                }
                else {
                    str = str + "  " + key + ": " + value;
                }
            }
            else if (key.equalsIgnoreCase("Password") && value instanceof Map) {
                if (!((Map)value).containsKey("encrypted")) {
                    continue;
                }
                str = str + "  " + key + ": '" + ((Map)value).get("encrypted") + "'";
                str = str + ",\n  " + key + "_encrypted: 'true'";
            }
            else if (!key.equalsIgnoreCase("directory")) {
                str = str + "  " + key + ": '" + StringEscapeUtils.escapeJava(value.toString()) + "'";
            }
            else {
                str = str + "  " + key + ": '" + value + "'";
            }
        }
        str += "\n";
        return str;
    }
    
    public static String createCQStatementText(final String cq_name, final Boolean doReplace, final String dest_stream_name, final String[] field_name_list, final String selectText) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " CQ " + cq_name + " ";
        str += "\nINSERT INTO ";
        str = str + dest_stream_name + "\n";
        if (field_name_list != null && field_name_list.length > 0) {
            str = str + "( " + join(field_name_list) + " ) \n";
        }
        str = str + selectText + "\n";
        return str;
    }
    
    private static String join(final String[] arr) {
        final StringBuilder sb = new StringBuilder();
        String delim = "";
        for (final String i : arr) {
            sb.append(delim).append(i);
            delim = ",";
        }
        return sb.toString();
    }
    
    public static String join(final List<String> list) {
        final String[] arr = list.toArray(new String[list.size()]);
        return join(arr);
    }
    
    public static String createSourceStatementText(final String sourceNameWithoutDomain, final Boolean doReplace, final String adapType, final String adapVersion, final List<Property> adapProps, final String parserType, final String parserVersion, final List<Property> parserProps, final String instream, final MappedStream generated_streams) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " SOURCE " + splitName(sourceNameWithoutDomain) + " ";
        str = str + "USING " + splitName(adapType) + " ";
        if (adapVersion != null && !adapVersion.trim().isEmpty()) {
            str = str + "VERSION '" + adapVersion + "'";
        }
        str += " ( \n";
        str += prettyPrintMap(adapProps);
        str += " ) \n";
        if (parserType != null) {
            str = str + " PARSE USING " + splitName(parserType) + " ";
            if (parserVersion != null && !parserVersion.trim().isEmpty()) {
                str = str + "VERSION '" + parserVersion + "'";
            }
            str += " ( \n";
            if (parserProps != null) {
                str += prettyPrintMap(parserProps);
            }
            str += " ) \n";
        }
        str = str + "OUTPUT TO " + splitName(instream);
        if (generated_streams != null) {
            str = str + " ,\n" + generated_streams.streamName + " MAP ( \n" + prettyPrintMap(makePropList(generated_streams.mappingProperties)) + ")";
        }
        return str;
    }
    
    public static String createTargetStatementText(final String sourceNameWithoutDomain, final Boolean doReplace, final String adapType, final String adapVersion, final List<Property> adapProps, final String formatterType, final String formatterVersion, final List<Property> formatterProps, final String instream) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " TARGET " + sourceNameWithoutDomain + " ";
        str = str + "USING " + splitName(adapType) + " ";
        if (adapVersion != null && !adapVersion.trim().isEmpty()) {
            str = str + "VERSION '" + adapVersion + "'";
        }
        str += " ( \n";
        str += prettyPrintMap(adapProps);
        str += " ) \n";
        if (formatterType != null) {
            str = str + "FORMAT USING " + splitName(formatterType) + " ";
            if (formatterVersion != null && !formatterVersion.trim().isEmpty()) {
                str = str + "VERSION '" + formatterVersion + "'";
            }
            str += " ( ";
            str += prettyPrintMap(formatterProps);
            str += " ) \n";
        }
        str = str + "INPUT FROM " + splitName(instream);
        return str;
    }
    
    public static String createSubscriptionStatementText(final String sourceNameWithoutDomain, final Boolean doReplace, final String adapType, final List<Property> adapProps, final String formatterType, final List<Property> formatterProps, final String instream) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " SUBSCRIPTION " + sourceNameWithoutDomain + " ";
        str = str + "USING " + adapType + " ( \n";
        str += prettyPrintMap(adapProps);
        str += " ) \n";
        if (formatterType != null) {
            str = str + "FORMAT USING " + splitName(formatterType) + " ( ";
            str += prettyPrintMap(formatterProps);
            str += " ) \n";
        }
        str = str + "INPUT FROM " + splitName(instream);
        return str;
    }
    
    public static String convertAdapterClassToName(final String adapterClassName) {
        PropertyTemplate adapterAnno = null;
        try {
            final Class adapterCls = Class.forName(adapterClassName, false, ClassLoader.getSystemClassLoader());
            adapterAnno = (PropertyTemplate)adapterCls.getAnnotation(PropertyTemplate.class);
        }
        catch (ClassNotFoundException e) {
            if (Utility.logger.isInfoEnabled()) {
                Utility.logger.info((Object)e.getMessage());
            }
        }
        return (adapterAnno == null) ? null : adapterAnno.name();
    }
    
    public static String prettyPrintEventType(final Map<String, List<String>> ets) {
        String str = "EVENT TYPES ( ";
        boolean firstTime = true;
        for (final Map.Entry<String, List<String>> et : ets.entrySet()) {
            if (!firstTime) {
                str += ", ";
            }
            final String eventType = et.getKey();
            final List<String> keyFields = et.getValue();
            str = str + splitName(eventType) + " KEY ( " + join(keyFields.toArray(new String[keyFields.size()])) + " ) ";
            firstTime = false;
        }
        str += " ) ";
        return str;
    }
    
    public static String createWASStatementText(final String wasNameWithoutDomain, final Boolean doReplace, final String typeName, final Map<String, List<String>> ets, final String howLong, final HStorePersistencePolicy hStorePersistencePolicy) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE ";
        }
        str = str + "HDSTORE " + wasNameWithoutDomain + " ";
        str = str + " CONTEXT OF " + splitName(typeName) + "\n";
        if (!ets.isEmpty()) {
            str += prettyPrintEventType(ets);
        }
        String persistence = "";
        switch (hStorePersistencePolicy.type) {
            case IN_MEMORY: {
                persistence = " USING MEMORY";
                break;
            }
            case STANDARD: {
                if (hStorePersistencePolicy.properties.size() == 1 && hStorePersistencePolicy.properties.get(0).name.equalsIgnoreCase(Type.STANDARD.typeName())) {
                    persistence = " ";
                    break;
                }
                persistence = " USING ( " + prettyPrintMap(hStorePersistencePolicy.properties) + " ) ";
                break;
            }
            case INTERVAL: {
                String interval;
                if (hStorePersistencePolicy.howOften == null) {
                    interval = "NONE";
                }
                else {
                    interval = ((hStorePersistencePolicy.howOften.value != 0L) ? ("EVERY " + howLong) : "IMMEDIATE");
                }
                persistence = " PERSIST " + interval + " USING ( " + prettyPrintMap(hStorePersistencePolicy.properties) + " ) ";
                break;
            }
        }
        str += persistence;
        return str;
    }
    
    public static String createStreamStatementText(final String streamNameWithoutDomain, final Boolean doReplace, final String typeNameWithoutDomain, final String[] partition_fields, final String persistPropertySetName) {
        String str = createStreamStatementText(streamNameWithoutDomain, doReplace, typeNameWithoutDomain, partition_fields);
        if (persistPropertySetName != null) {
            str = str + " PERSIST USING " + persistPropertySetName;
        }
        return str;
    }
    
    public static String createStreamStatementText(final String streamNameWithoutDomain, final Boolean doReplace, final String typeNameWithoutDomain, final String[] partition_fields) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE ";
        }
        str = str + "STREAM " + streamNameWithoutDomain + " ";
        str = str + "OF " + typeNameWithoutDomain;
        if (partition_fields != null && partition_fields.length > 0) {
            str = str + " PARTITION BY " + join(partition_fields);
        }
        return str;
    }
    
    public static String createTypeStatementText(final String typeNameWithoutDomain, final Boolean doReplace, final List<TypeField> makeFieldList) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " TYPE " + typeNameWithoutDomain + " ";
        str += " ( ";
        for (int i = 0; i < makeFieldList.size(); ++i) {
            if (i + 1 == makeFieldList.size()) {
                if (makeFieldList.get(i).isPartOfKey) {
                    str = str + makeFieldList.get(i).fieldName + " " + makeFieldList.get(i).fieldType.name + " KEY  \n";
                }
                else {
                    str = str + makeFieldList.get(i).fieldName + " " + makeFieldList.get(i).fieldType.name + "  \n";
                }
            }
            else if (makeFieldList.get(i).isPartOfKey) {
                str = str + makeFieldList.get(i).fieldName + " " + makeFieldList.get(i).fieldType.name + " KEY , \n";
            }
            else {
                str = str + makeFieldList.get(i).fieldName + " " + makeFieldList.get(i).fieldType.name + " , \n";
            }
        }
        str += " ) ";
        return str;
    }
    
    public static String createWindowStatementText(final String windowNameWithoutDomain, final Boolean doReplace, final String stream_name, final Map<String, Object> window_len, final boolean isJumping, final String[] partition_fields, final boolean isPersistent) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        if (isJumping) {
            str += " JUMPING";
        }
        str = str + " WINDOW " + windowNameWithoutDomain + " ";
        str = str + "OVER " + splitName(stream_name) + " KEEP ";
        Object val = window_len.get("range");
        if (val != null) {
            str = str + "RANGE " + val + " ";
        }
        val = window_len.get("count");
        if (val != null) {
            Integer i;
            if (val instanceof String) {
                i = Integer.valueOf((String)val);
            }
            else {
                if (!(val instanceof Integer)) {
                    throw new RuntimeException("invalid count interval");
                }
                i = (Integer)val;
            }
            str = str + i + " ROWS ";
        }
        val = window_len.get("time");
        if (val != null) {
            final Interval interval = parseInterval((String)val);
            str = str + "WITHIN " + interval.toHumanReadable() + " ";
        }
        final Object attr = window_len.get("on");
        if (attr != null) {
            if (!(attr instanceof String)) {
                throw new RuntimeException("invalid ON attribute");
            }
            str = str + "ON " + attr;
        }
        if (partition_fields.length != 0) {
            str = str + " PARTITION BY " + join(partition_fields);
        }
        return str;
    }
    
    public static String createCacheStatementText(final String cacheNameWithoutDomain, final Boolean doReplace, final String adapType, final List<Property> readerPropList, final String parserType, final List<Property> parserPropList, final List<Property> queryPropList, final String typename) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        if (adapType.equalsIgnoreCase("STREAM")) {
            str = str + " EVENTTABLE " + splitName(cacheNameWithoutDomain) + " ";
        }
        else {
            str = str + " CACHE " + splitName(cacheNameWithoutDomain) + " ";
        }
        str = str + "USING " + splitName(adapType) + " ( \n";
        str += prettyPrintMap(readerPropList);
        str += " ) \n";
        if (parserType != null && !adapType.equalsIgnoreCase("STREAM")) {
            str = str + "PARSE USING " + parserType + " ( \n";
            str += prettyPrintMap(parserPropList);
            str += " ) \n";
        }
        str += "QUERY ( \n";
        str += prettyPrintMap(queryPropList);
        str += " ) \n";
        str = str + " OF  " + splitName(typename);
        return str;
    }
    
    public static Interval parseInterval(final String intervalString) {
        if (intervalString.equalsIgnoreCase("NONE")) {
            return null;
        }
        final Pattern p = Pattern.compile("\\s*([\\d\\s.:-]+)\\s+(day|hour|minute|second)(\\s+to\\s+(day|hour|minute|second))?\\s*", 2);
        final Matcher m = p.matcher(intervalString);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid interval string: " + intervalString);
        }
        final String literal = m.group(1);
        final String measurement1 = m.group(2);
        final String measurement2 = m.group(4);
        int flags = string2flag(measurement1);
        if (measurement2 != null) {
            final int flag2 = string2flag(measurement2);
            if (flags <= flag2) {
                throw new IllegalArgumentException("Invalid interval string: " + intervalString);
            }
            flags = ((flags << 1) - 1 & ~(flag2 - 1));
        }
        final Interval i = Interval.parseDSInterval(literal, flags);
        if (i == null) {
            throw new IllegalArgumentException("Invalid interval string: " + intervalString);
        }
        return i;
    }
    
    public static LocalTime parseLocalTime(final String localTime) {
        final String[] split = localTime.split(":");
        final String expectedFormat = "hh:mm:ss";
        final String errorMsg = "Invalid input for Localtime. Given input: " + localTime + " Expected format: " + expectedFormat;
        if (split.length != 3) {
            throw new IllegalArgumentException(errorMsg);
        }
        final int hrs = Integer.parseInt(split[0]);
        final int minutes = Integer.parseInt(split[1]);
        final int seconds = Integer.parseInt(split[2]);
        LocalTime time;
        try {
            time = new LocalTime(hrs, minutes, seconds);
        }
        catch (IllegalFieldValueException ex) {
            throw new IllegalArgumentException(errorMsg, (Throwable)ex);
        }
        return time;
    }
    
    public static int string2flag(final String val) {
        final String upperCase = val.toUpperCase();
        switch (upperCase) {
            case "DAY": {
                return 8;
            }
            case "HOUR": {
                return 4;
            }
            case "MINUTE": {
                return 2;
            }
            case "SECOND": {
                return 1;
            }
            default: {
                assert false;
                return 0;
            }
        }
    }
    
    public static UUID getDeploymentGroupFromMetaObject(final UUID metaObjectUUID, final AuthToken token) throws MetaDataRepositoryException {
        final MetadataRepository rep = MetadataRepository.getINSTANCE();
        MetaInfo.MetaObject metaObject = rep.getMetaObjectByUUID(metaObjectUUID, token);
        if (metaObject == null) {
            return null;
        }
        Set<UUID> deps = metaObject.getReverseIndexObjectDependencies();
        if (metaObject instanceof MetaInfo.WAStoreView) {
            final UUID hdStoreUUID = ((MetaInfo.WAStoreView)metaObject).wastoreID;
            metaObject = rep.getMetaObjectByUUID(hdStoreUUID, token);
            deps = metaObject.getReverseIndexObjectDependencies();
        }
        if (deps.isEmpty()) {
            if (!(metaObject instanceof MetaInfo.Window)) {
                return null;
            }
            final UUID streamUUID = ((MetaInfo.Window)metaObject).stream;
            metaObject = rep.getMetaObjectByUUID(streamUUID, token);
            deps = metaObject.getReverseIndexObjectDependencies();
        }
        final List<MetaInfo.Flow> possibleFlow = new ArrayList<MetaInfo.Flow>();
        final List<MetaInfo.Flow> possibleApp = new ArrayList<MetaInfo.Flow>();
        for (final UUID reverseIndex : deps) {
            final MetaInfo.MetaObject reverseIndexObject = rep.getMetaObjectByUUID(reverseIndex, token);
            if (reverseIndexObject != null) {
                if (reverseIndexObject.type == EntityType.FLOW) {
                    possibleFlow.add((MetaInfo.Flow)reverseIndexObject);
                }
                if (reverseIndexObject.type != EntityType.APPLICATION) {
                    continue;
                }
                possibleApp.add((MetaInfo.Flow)reverseIndexObject);
            }
        }
        UUID deploymentGroup = null;
        if (!possibleFlow.isEmpty()) {
            for (final MetaInfo.Flow flow : possibleFlow) {
                for (final MetaInfo.Flow application : possibleApp) {
                    if (application.deploymentPlan != null) {
                        for (final MetaInfo.Flow.Detail d : application.deploymentPlan) {
                            if (d != null && d.flow.equals((Object)flow.getUuid())) {
                                deploymentGroup = d.deploymentGroup;
                                if (deploymentGroup != null) {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
            }
        }
        if (deploymentGroup == null) {
            for (final MetaInfo.Flow flow : possibleApp) {
                if (flow.deploymentPlan != null) {
                    deploymentGroup = flow.deploymentPlan.get(0).deploymentGroup;
                    break;
                }
            }
        }
        return deploymentGroup;
    }
    
    public static boolean isValueEncryptionFlagSetToTrue(String key, final Map<String, Object> props) {
        final Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
        temp.putAll(props);
        key += "_encrypted";
        if (temp.get(key) != null) {
            final Object obj = temp.get(key);
            if (obj instanceof String) {
                if (((String)obj).equalsIgnoreCase("true")) {
                    return true;
                }
            }
            else if (obj instanceof Boolean) {
                return (boolean)obj;
            }
        }
        return false;
    }
    
    public static boolean isValueEncryptionFlagExists(String key, final Map<String, Object> props) {
        final Map<String, Object> temp = Factory.makeCaseInsensitiveMap();
        temp.putAll(props);
        key += "_encrypted";
        return temp.containsKey(key);
    }
    
    public static Map<String, Object> makePropertyMap(final List<Property> list) {
        if (list == null) {
            return null;
        }
        if (list.isEmpty()) {
            return Factory.makeCaseInsensitiveMap();
        }
        final Map<String, Object> map = Factory.makeCaseInsensitiveMap();
        for (final Property property : list) {
            map.put(property.name, property.value);
        }
        return map;
    }
    
    public static List<Property> makePropList(final Map<String, Object> properties) {
        if (properties == null) {
            return null;
        }
        final List<Property> plist = new ArrayList<Property>();
        for (final Map.Entry<String, Object> prop : properties.entrySet()) {
            final Property p = new Property(prop.getKey(), prop.getValue());
            plist.add(p);
        }
        return plist;
    }
    
    public static void changeLogLevel(final Object paramvalue, final boolean printMessage) {
        if (paramvalue instanceof String) {
            final String loglevel = paramvalue.toString();
            LogManager.getRootLogger().setLevel(Level.toLevel(loglevel));
        }
        else if (paramvalue instanceof List) {
        		Iterator iter = ((List)paramvalue).iterator();
            while (iter.hasNext()) {
            		final Property prop = (Property)iter.next();
                if (LogManager.exists(prop.name) != null) {
                    LogManager.getLogger(prop.name).setLevel(Level.toLevel(prop.value.toString()));
                }
                else {
                    if (!printMessage) {
                        continue;
                    }
                    System.out.println("Logger for class " + prop.name + " does not exist.");
                }
            }
        }
    }
    
    public static synchronized void prettyPrint(final PathManager position) {
        prettyPrint((position == null) ? null : position.toPosition());
    }
    
    public static synchronized void prettyPrint(final Position position) {
        final StringBuilder output = new StringBuilder();
        if (position == null) {
            output.append("\n- Position: {null}");
        }
        else if (position.isEmpty()) {
            output.append("\n- Position: {empty}");
        }
        else {
            output.append("\n- Position:");
            int max = 20;
            for (final Path path : position.values()) {
                if (max-- < 0) {
                    output.append("\n- * Too many to print: ").append(path.toString());
                }
                else {
                    output.append("\n- * Path <").append(path.getPathHash()).append(">");
                    for (int i = 0; i < path.getPathItems().size(); ++i) {
                        final Path.Item item = path.getPathComponent(i);
                        if (item == null) {
                            output.append("\n-      -- (unexpected null)\n");
                        }
                        else {
                            try {
                                final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), HSecurityManager.TOKEN);
                                if (mo != null) {
                                    output.append(String.format("%n-   ~ %-12s %-50s [%s]", mo.type, mo.name + "/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), mo.uuid));
                                }
                                else {
                                    output.append(String.format("%n-   ~ %s", "Could not find metadata for " + item.getComponentUUID()));
                                }
                            }
                            catch (MetaDataRepositoryException e) {
                                Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e);
                                output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound", "ComponentNotFound/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                            }
                            catch (Exception e2) {
                                Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e2);
                                output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound/Null", "Component/Null" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                            }
                        }
                    }
                    output.append("\n-    ").append(path.getAtOrAfter()).append(" ").append(path.getLowSourcePosition());
                }
            }
        }
        output.append("\n- End Of Position");
        final StackTraceElement[] traceElements = Thread.getAllStackTraces().get(Thread.currentThread());
        if (traceElements.length > 3) {
            output.append(" [Called from ").append(traceElements[3].getClassName()).append(".").append(traceElements[3].getMethodName()).append("(").append(traceElements[3].getFileName()).append(":").append(traceElements[3].getLineNumber()).append(")]");
        }
        Logger.getLogger("Recovery").log((Priority)Logger.getLogger("Recovery").getEffectiveLevel(), (Object)output.toString());
    }
    
    public static synchronized void prettyPrint(final OffsetPosition position) {
        final StringBuilder output = new StringBuilder();
        if (position == null) {
            output.append("\n- OffsetPosition: {null} ");
        }
        else if (position.isEmpty()) {
            output.append("\n- OffsetPosition: {empty} Offset=").append(position.getOffset());
        }
        else {
            output.append("\n- OffsetPosition: Offset=").append(position.getOffset());
            for (final Path path : position.values()) {
                output.append("\n- * Path <").append(path.getPathHash()).append(">");
                for (int i = 0; i < path.getPathItems().size(); ++i) {
                    final Path.Item item = path.getPathComponent(i);
                    if (item == null) {
                        output.append("\n-      -- (unexpected null)\n");
                    }
                    else {
                        try {
                            final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), HSecurityManager.TOKEN);
                            if (mo != null) {
                                output.append(String.format("%n-   ~ %-12s %-50s [%s]", mo.type, mo.name + "/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), mo.uuid));
                            }
                            else {
                                output.append(String.format("%n-   ~ %s", "Could not find metadata for " + item.getComponentUUID()));
                            }
                        }
                        catch (MetaDataRepositoryException e) {
                            Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e);
                            output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound", "ComponentNotFound/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                        }
                        catch (Exception e2) {
                            Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e2);
                            output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound/Null", "Component/Null" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                        }
                    }
                }
                output.append("\n-    ").append(path.getAtOrAfter()).append(" ").append(path.getLowSourcePosition());
            }
        }
        output.append("\n- End Of Position");
        final StackTraceElement[] traceElements = Thread.getAllStackTraces().get(Thread.currentThread());
        if (traceElements.length > 3) {
            output.append(" [Called from ").append(traceElements[3].getClassName()).append(".").append(traceElements[3].getMethodName()).append("(").append(traceElements[3].getFileName()).append(":").append(traceElements[3].getLineNumber()).append(")]");
        }
        Logger.getLogger("Recovery").log((Priority)Logger.getLogger("Recovery").getEffectiveLevel(), (Object)output.toString());
    }
    
    public static synchronized void prettyPrint(final Path path) {
        final StringBuilder output = new StringBuilder();
        if (path == null) {
            output.append("\n- * Path <null>");
        }
        else {
            output.append("\n- * Path <").append(path.getPathHash()).append("> ").append(path.getAtOrAfter()).append(path.getLowSourcePosition());
            for (int i = 0; i < path.getPathItems().size(); ++i) {
                final Path.Item item = path.getPathComponent(i);
                if (item == null) {
                    output.append("\n-      -- (unexpected null)\n");
                }
                else {
                    try {
                        final MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(item.getComponentUUID(), HSecurityManager.TOKEN);
                        output.append(String.format("%n-   ~ %-12s %-50s [%s]", mo.type, mo.name + "/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), mo.uuid));
                    }
                    catch (MetaDataRepositoryException e) {
                        Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e);
                        output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound", "ComponentNotFound/" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                    }
                    catch (Exception e2) {
                        Utility.logger.error((Object)("Error when trying to pretty print " + item.getComponentUUID()), (Throwable)e2);
                        output.append(String.format("%n-   ~ %-12s %-50s [%s]", "TypeNotFound/Null", "Component/Null" + ((item.getDistributionID() == null) ? "~" : item.getDistributionID()), "UUID Unknown"));
                    }
                }
            }
        }
        Logger.getLogger("Recovery").log((Priority)Logger.getLogger("Recovery").getEffectiveLevel(), (Object)output.toString());
    }
    
    public static Boolean getPropertyBoolean(final Map<String, Object> properties, final String propertyName) {
        Boolean result = null;
        final Object property = (properties != null) ? properties.get(propertyName) : null;
        if (property != null) {
            result = getBooleanValue(property);
        }
        return result;
    }
    
    private static Boolean getBooleanValue(final Object property) {
        return Boolean.valueOf(property.toString());
    }
    
    public static String createSourceStatementText(final String sourceNameWithoutDomain, final Boolean doReplace, final String adapterType, final String adapVersion, final List<Property> adapterProperty, final String parserType, final String parserVersion, final List<Property> parserProperty, final List<OutputClause> outputClauses) {
        String str = "CREATE ";
        if (doReplace) {
            str += "OR REPLACE";
        }
        str = str + " SOURCE " + splitName(sourceNameWithoutDomain) + " ";
        str = str + "USING " + splitName(adapterType) + " ";
        if (adapVersion != null && !adapVersion.trim().isEmpty()) {
            str = str + "VERSION '" + adapVersion + "'";
        }
        str += " ( \n";
        str += prettyPrintMap(adapterProperty);
        str += " ) \n";
        if (parserType != null) {
            str = str + " PARSE USING " + splitName(parserType) + " ";
            if (parserVersion != null && !parserVersion.trim().isEmpty()) {
                str = str + "VERSION '" + parserVersion + "'";
            }
            str += " ( \n";
            if (parserProperty != null) {
                str += prettyPrintMap(parserProperty);
            }
            str += " ) \n";
        }
        for (int i = 0; i < outputClauses.size(); ++i) {
            final OutputClause innerOutputClause = outputClauses.get(i);
            if (innerOutputClause.getStreamName() != null) {
                str = str + "OUTPUT TO " + splitName(innerOutputClause.getStreamName());
            }
            if (innerOutputClause.getGeneratedStream() != null) {
                str = str + "OUTPUT TO " + splitName(innerOutputClause.getGeneratedStream().streamName);
            }
            str = buildFilterClause(str, innerOutputClause.getGeneratedStream(), innerOutputClause.getTypeDefinition(), innerOutputClause.getFilterText());
            if (i + 1 < outputClauses.size()) {
                str += ", \n";
            }
        }
        return str;
    }
    
    private static String buildFilterClause(String base, final MappedStream mappedStreams, final List<TypeField> typeFields, final String selectText) {
        if (typeFields != null && !typeFields.isEmpty()) {
            final Iterator<TypeField> iterator = typeFields.iterator();
            base += "(";
            while (iterator.hasNext()) {
                final TypeField entry = iterator.next();
                base = base + " " + entry.fieldName + " " + entry.fieldType.name + " ";
                if (iterator.hasNext()) {
                    base += ", ";
                }
            }
            base += ")";
        }
        if (mappedStreams != null) {
            base += " MAP ( ";
            for (final Map.Entry<String, Object> entry2 : mappedStreams.mappingProperties.entrySet()) {
                base = base + entry2.getKey() + ":'" + entry2.getValue() + "'";
            }
            base += ")";
        }
        base += " ";
        if (selectText != null) {
            base = base + cleanUpSelectStatement(selectText) + " ";
        }
        return base;
    }
    
    public static String cleanUpSelectStatement(final String selectTQL) {
        final StringBuilder sb = new StringBuilder();
        if (selectTQL != null) {
            String slctxt = selectTQL.trim();
            slctxt = StringUtils.strip(slctxt);
            if (slctxt.endsWith(",")) {
                sb.append(slctxt.substring(0, slctxt.length() - 1));
            }
            else {
                sb.append(slctxt);
            }
        }
        return sb.toString();
    }
    
    public static String getWindowType(final IntervalPolicy policy) {
        Integer count = null;
        Long time = null;
        Long timeout = null;
        String onField = null;
        if (policy.getCountPolicy() != null) {
            count = policy.getCountPolicy().getCountInterval();
        }
        if (policy.getTimePolicy() != null) {
            time = policy.getTimePolicy().getTimeInterval();
        }
        if (policy.getAttrPolicy() != null) {
            timeout = policy.getAttrPolicy().getAttrValueRange();
            onField = policy.getAttrPolicy().getAttrName();
        }
        if (timeout != null && org.apache.commons.lang3.StringUtils.isNotBlank((CharSequence)onField) && (count != null || time != null)) {
            return "hybrid";
        }
        if (count != null && time != null) {
            return "hybrid";
        }
        if (count != null) {
            return "count";
        }
        if (time != null || timeout != null) {
            return "time";
        }
        return null;
    }
    
    public static String appendOutputClause(String str, final List<OutputClause> outputClauses) {
        for (int i = 0; i < outputClauses.size(); ++i) {
            final OutputClause innerOutputClause = outputClauses.get(i);
            if (innerOutputClause.getStreamName() != null) {
                str = str + "OUTPUT TO " + splitName(innerOutputClause.getStreamName());
            }
            if (innerOutputClause.getGeneratedStream() != null) {
                str = str + "OUTPUT TO " + splitName(innerOutputClause.getGeneratedStream().streamName);
            }
            str = buildFilterClause(str, innerOutputClause.getGeneratedStream(), innerOutputClause.getTypeDefinition(), innerOutputClause.getFilterText());
            if (i + 1 < outputClauses.size()) {
                str += " \n";
            }
        }
        return str;
    }
    
    public static PathManager updateUuids(final PathManager original) {
        if (original == null) {
            return null;
        }
        final Map<UUID, UUID> needUpdate = getMapNeedsUpdate(original.values());
        if (needUpdate.isEmpty()) {
            return original;
        }
        final PathManager updated = getPathManagerWithUpdatedUuids(original.values(), needUpdate);
        return updated;
    }
    
    public static Position updateUuids(final Position original) {
        if (original == null) {
            return null;
        }
        final Map<UUID, UUID> needUpdate = getMapNeedsUpdate(original.values());
        if (needUpdate.isEmpty()) {
            return original;
        }
        final PathManager updated = getPathManagerWithUpdatedUuids(original.values(), needUpdate);
        return updated.toPosition();
    }
    
    public static OffsetPosition updateUuids(final OffsetPosition original) {
        if (original == null) {
            return null;
        }
        final Map<UUID, UUID> needUpdate = getMapNeedsUpdate(original.values());
        if (needUpdate.isEmpty()) {
            return original;
        }
        final PathManager updated = getPathManagerWithUpdatedUuids(original.values(), needUpdate);
        final OffsetPosition result = new OffsetPosition(updated.toPosition(), original.getOffset(), original.getTimestamp());
        return result;
    }
    
    private static PathManager getPathManagerWithUpdatedUuids(final Collection<Path> original, final Map<UUID, UUID> needUpdate) {
        final PathManager updated = new PathManager();
        for (final Path originalPath : original) {
            final Path.ItemList originalPathItemList = originalPath.getPathItems();
            boolean pathNeedsUpdate = false;
            for (int i = 0; i < originalPathItemList.size(); ++i) {
                final Path.Item originalPathItem = originalPathItemList.get(i);
                if (needUpdate.containsKey(originalPathItem.getComponentUUID())) {
                    pathNeedsUpdate = true;
                    break;
                }
            }
            if (pathNeedsUpdate) {
                final Path.Item[] updatedPathItemArray = new Path.Item[originalPathItemList.size()];
                for (int j = 0; j < updatedPathItemArray.length; ++j) {
                    final Path.Item originalPathItem2 = originalPathItemList.get(j);
                    if (needUpdate.containsKey(originalPathItem2.getComponentUUID())) {
                        updatedPathItemArray[j] = Path.Item.get((UUID)needUpdate.get(originalPathItem2.getComponentUUID()), originalPathItem2.getDistributionID());
                    }
                    else {
                        updatedPathItemArray[j] = originalPathItem2;
                    }
                }
                final Path.ItemList updatedPathItemList = Path.ItemList.get(updatedPathItemArray);
                final Path updatedPath = new Path(updatedPathItemList, originalPath.getLowSourcePosition(), originalPath.getHighSourcePosition(), originalPath.getAtOrAfterBoolean());
                updated.mergeHigherPath(updatedPath);
            }
            else {
                updated.mergeHigherPath(originalPath);
            }
        }
        return updated;
    }
    
    private static Map<UUID, UUID> getMapNeedsUpdate(final Collection<Path> original) {
        final Set<UUID> dontNeedUpdate = new HashSet<UUID>();
        final Map<UUID, UUID> needUpdate = new HashMap<UUID, UUID>();
        for (final Path originalPath : original) {
            final Path.ItemList originalPathItemList = originalPath.getPathItems();
            for (int i = 0; i < originalPathItemList.size(); ++i) {
                final Path.Item originalPathItem = originalPathItemList.get(i);
                final UUID originalPathItemUuid = originalPathItem.getComponentUUID();
                if (!dontNeedUpdate.contains(originalPathItemUuid)) {
                    if (!needUpdate.containsKey(originalPathItemUuid)) {
                        final UUID updatedUuid = updateUuid(originalPathItemUuid);
                        if (updatedUuid.equals((Object)originalPathItemUuid)) {
                            dontNeedUpdate.add(originalPathItemUuid);
                        }
                        else {
                            needUpdate.put(originalPathItemUuid, updatedUuid);
                        }
                    }
                }
            }
        }
        return needUpdate;
    }
    
    private static UUID updateUuid(final UUID original) {
        try {
            final MetaInfo.MetaObject originalMO = MetadataRepository.getINSTANCE().getMetaObjectByUUID(original, HSecurityManager.TOKEN);
            final MetaInfo.MetaObject translatedMO = MetadataRepository.getINSTANCE().getMetaObjectByName(originalMO.getType(), originalMO.getNsName(), originalMO.getName(), null, HSecurityManager.TOKEN);
            final MetaInfo.Flow originalApp = originalMO.getCurrentApp();
            final MetaInfo.Flow translatedApp = translatedMO.getCurrentApp();
            if (originalApp != null && translatedApp != null && originalApp.getUuid().equals((Object)translatedApp.getUuid())) {
                return translatedMO.getUuid();
            }
            return original;
        }
        catch (Exception e) {
            if (Utility.logger.isDebugEnabled()) {
                Utility.logger.debug((Object)("Could not translate UUID: " + e.getMessage()));
            }
            return original;
        }
    }
    
    static {
        Utility.logger = Logger.getLogger((Class)Utility.class);
    }
}
