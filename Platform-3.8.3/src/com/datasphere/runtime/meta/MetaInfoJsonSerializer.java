package com.datasphere.runtime.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.intf.QueryManager;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.security.ObjectPermission;
import com.datasphere.uuid.UUID;

public class MetaInfoJsonSerializer
{
    public static ObjectMapper jsonMapper;
    
    public static UUID uuidDeserializer(final Object obj) {
        if (obj == null) {
            return null;
        }
        UUID uuid = null;
        if (obj instanceof String) {
            final String uuidstring = (String)obj;
            uuid = new UUID();
            uuid.setUUIDString(uuidstring);
        }
        else if (obj instanceof Map) {
            uuid = new UUID();
            uuid.setUUIDString((String)((Map)obj).get("uuidstring"));
        }
        return uuid;
    }
    
    static {
        MetaInfoJsonSerializer.jsonMapper = null;
        (MetaInfoJsonSerializer.jsonMapper = ObjectMapperFactory.newInstance()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MetaInfoJsonSerializer.jsonMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        MetaInfoJsonSerializer.jsonMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        MetaInfoJsonSerializer.jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        MetaInfoJsonSerializer.jsonMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }
    
    public static class MetaObjectJsonSerializer
    {
        public static MetaInfo.MetaObject deserialize(final String json) throws JsonParseException, JsonMappingException, IOException {
            if (json == null || json.isEmpty()) {
                return null;
            }
            final HashMap<String, Object> results = (HashMap<String, Object>)MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)HashMap.class);
            final MetaInfo.MetaObject metaObject = new MetaInfo.MetaObject();
            metaObject.name = (String)results.get("name");
            metaObject.uri = (String)results.get("uri");
            metaObject.uuid = MetaInfoJsonSerializer.uuidDeserializer(results.get("uuid"));
            metaObject.nsName = (String)results.get("nsName");
            metaObject.namespaceId = MetaInfoJsonSerializer.uuidDeserializer(results.get("namespaceId"));
            metaObject.type = EntityType.valueOf((String)results.get("type"));
            metaObject.description = (String)results.get("description");
            final Map metaInfoStatusmap = (Map)results.get("metaInfoStatus");
            final MetaInfoStatus mis = MetaInfoStatus.jsonDeserializer(metaInfoStatusmap);
            metaObject.setMetaInfoStatus(mis);
            return metaObject;
        }
        
        public static MetaInfo.MetaObject deserialize(final MetaInfo.MetaObject metaObject, final String json) throws JsonParseException, JsonMappingException, IOException {
            final MetaInfo.MetaObject temp = deserialize(json);
            metaObject.setName(temp.getName());
            metaObject.setUri(temp.getUri());
            metaObject.setUuid(temp.getUuid());
            metaObject.setNsName(temp.getNsName());
            metaObject.setNamespaceId(temp.getNamespaceId());
            metaObject.setType(temp.getType());
            metaObject.setDescription(temp.getDescription());
            return metaObject;
        }
    }
    
    public static class QueryJsonSerializer
    {
        public static MetaInfo.Query deserialize(final String json) throws JsonParseException, JsonMappingException, IOException {
            if (json == null || json.isEmpty()) {
                return null;
            }
            final HashMap<String, Object> results = (HashMap<String, Object>)MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)HashMap.class);
            MetaInfo.Query metaObject = new MetaInfo.Query();
            metaObject = (MetaInfo.Query)MetaObjectJsonSerializer.deserialize(metaObject, json);
            metaObject.appUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("appUUID"));
            metaObject.streamUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("streamUUID"));
            metaObject.cqUUID = MetaInfoJsonSerializer.uuidDeserializer(results.get("cqUUID"));
            metaObject.queryDefinition = (String)results.get("queryDefinition");
            final Map typeInfoMap = (Map)results.get("typeInfo");
            if (typeInfoMap != null && typeInfoMap.size() > 0) {
                metaObject.typeInfo = new HashMap<String, Long>();
                final Set keyset = typeInfoMap.keySet();
                Iterator iter = keyset.iterator();
                while (iter.hasNext()) {
                		final String ss = (String)iter.next();
                    metaObject.typeInfo.put(ss, ((Long)typeInfoMap.get(ss)).longValue());
                }
            }
            final List<String> queryParameters = (List<String>)results.get("queryParameters");
            if (queryParameters != null) {
                (metaObject.queryParameters = new ArrayList<String>()).addAll(queryParameters);
            }
            final List<Property> bindParameters = (List<Property>)results.get("bindParameters");
            if (bindParameters != null) {
                (metaObject.bindParameters = new ArrayList<Property>()).addAll(bindParameters);
            }
            final List qp = (List)results.get("projectionFields");
            metaObject.projectionFields.addAll(QueryManager.QueryProjection.jsonDeserializer(qp));
            return metaObject;
        }
    }
    
    public static class RoleJsonSerializer
    {
        public static MetaInfo.Role deserialize(final String json) throws JsonParseException, JsonMappingException, IOException {
            if (json == null || json.isEmpty()) {
                return null;
            }
            try {
                final Object ob = MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)MetaInfo.Role.class);
                if (ob != null) {
                    return (MetaInfo.Role)ob;
                }
            }
            catch (Exception ex) {}
            final HashMap<String, Object> results = (HashMap<String, Object>)MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)HashMap.class);
            MetaInfo.Role metaObject = new MetaInfo.Role();
            metaObject = (MetaInfo.Role)MetaObjectJsonSerializer.deserialize(metaObject, json);
            metaObject.domain = (String)results.get("domain");
            metaObject.roleName = (String)results.get("roleName");
            final List<ObjectPermission> permissions = (List<ObjectPermission>)results.get("permissions");
            if (permissions != null) {
                metaObject.permissions.addAll(permissions);
            }
            final List<UUID> roleUUIDs = (List<UUID>)results.get("roleUUIDs");
            if (permissions != null) {
                metaObject.roleUUIDs.addAll(roleUUIDs);
            }
            return metaObject;
        }
    }
    
    public static class UserJsonSerializer
    {
        public static MetaInfo.User deserialize(final String json) throws JsonParseException, JsonMappingException, IOException {
            if (json == null || json.isEmpty()) {
                return null;
            }
            try {
                final Object ob = MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)MetaInfo.User.class);
                if (ob != null) {
                    return (MetaInfo.User)ob;
                }
            }
            catch (Exception ex) {}
            final HashMap<String, Object> results = (HashMap<String, Object>)MetaInfoJsonSerializer.jsonMapper.readValue(json, (Class)HashMap.class);
            MetaInfo.User metaObject = new MetaInfo.User();
            metaObject = (MetaInfo.User)MetaObjectJsonSerializer.deserialize(metaObject, json);
            metaObject.setUserId((String)results.get("userId"));
            metaObject.setFirstName((String)results.get("firstName"));
            metaObject.setLastName((String)results.get("lastName"));
            metaObject.setMainEmail((String)results.get("mainEmail"));
            metaObject.setEncryptedPassword((String)results.get("encryptedPassword"));
            metaObject.setDefaultNamespace((String)results.get("defaultNamespace"));
            metaObject.setUserTimeZone((String)results.get("userTimeZone"));
            metaObject.setLdap((String)results.get("ldap"));
            String alias = null;
            if (results.get("alias") != null) {
                alias = (String)results.get("alias");
            }
            metaObject.setAlias(alias);
            final List<MetaInfo.ContactMechanism> contactMechanisms = (List<MetaInfo.ContactMechanism>)results.get("contactMechanisms");
            if (contactMechanisms != null) {
                metaObject.setContactMechanisms(contactMechanisms);
            }
            final List<ObjectPermission> permissions = (List<ObjectPermission>)results.get("permissions");
            if (permissions != null) {
                metaObject.setPermissions(permissions);
            }
            final List<UUID> roleUUIDs = (List<UUID>)results.get("roleUUIDs");
            if (permissions != null) {
                metaObject.setRoleUUIDs(roleUUIDs);
            }
            return metaObject;
        }
    }
}
