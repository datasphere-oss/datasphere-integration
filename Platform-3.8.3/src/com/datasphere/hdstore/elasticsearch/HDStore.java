package com.datasphere.hdstore.elasticsearch;

import com.datasphere.hdstore.base.*;

import org.apache.log4j.*;

import com.datasphere.anno.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.constants.*;
import com.datasphere.hdstore.*;
import com.datasphere.hdstore.exceptions.*;
import org.elasticsearch.cluster.metadata.*;
import com.fasterxml.jackson.databind.node.*;
import java.util.*;
import org.elasticsearch.common.collect.*;
import com.datasphere.persistence.*;
import org.apache.commons.lang.*;
import org.elasticsearch.client.*;
import org.elasticsearch.action.admin.indices.mapping.put.*;
import org.elasticsearch.action.admin.indices.refresh.*;
import org.elasticsearch.action.admin.indices.flush.*;
import org.elasticsearch.index.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.admin.indices.mapping.get.*;

@PropertyTemplate(name = "elasticsearch", type = AdapterType.hdstore, properties = { @PropertyTemplateProperty(name = "storageProvider", type = String.class, required = true, defaultValue = "elasticsearch"), @PropertyTemplateProperty(name = "elasticsearch.cluster_name", type = String.class, required = false, defaultValue = ""), @PropertyTemplateProperty(name = "elasticsearch.data_path", type = String.class, required = false, defaultValue = "./data"), @PropertyTemplateProperty(name = "elasticsearch.replicas", type = Integer.class, required = false, defaultValue = "1"), @PropertyTemplateProperty(name = "elasticsearch.shards", type = Integer.class, required = false, defaultValue = "5"), @PropertyTemplateProperty(name = "elasticsearch.storage_type", type = String.class, required = false, defaultValue = "any"), @PropertyTemplateProperty(name = "elasticsearch.time_to_live", type = String.class, required = false, defaultValue = "") })
public class HDStore extends HDStoreBase<HDStoreManager>
{
    private static final Class<HDStore> thisClass;
    private static final Logger logger;
    private static final String USE_HDSTORE_PROPERTY = "PROPERTY:";
    private String dataTypeName;
    private JsonNode dataTypeSchema;
    private static final String[][] ES_METADATA_FIELDS;
    private static final String[][] ES_METADATA_FIELDS_DISABLED;
    private final String actualName;
    private String indexName;
    
    HDStore(final HDStoreManager manager, final String hdStoreName, final Map<String, Object> properties, final String indexName) {
        super(manager, hdStoreName, properties);
        this.actualName = manager.translateName(NameType.HDSTORE, hdStoreName);
        this.indexName = indexName;
    }
    
    public void setIndexName(final String indexName) {
        this.indexName = indexName;
    }
    
    private static Map<String, Object> getSourceMapFromSchema(final String hdStoreName, final JsonNode hdStoreSchema, final Map<String, Object> properties) {
        final Map<String, Object> mapping = new HashMap<String, Object>(1);
        final JsonNode context = hdStoreSchema.get("context");
        final Map<String, Object> contextMapping = getContextMapping(context);
        final JsonNode eventTypes = hdStoreSchema.get("events");
        if (eventTypes != null) {
            addEventTypeMapping(contextMapping, eventTypes);
        }
        final JsonNode metadata = hdStoreSchema.get("metadata");
        addCheckpointMapping(metadata, contextMapping);
        mapping.put("properties", contextMapping);
        if (metadata != null) {
            addMetadataMapping(mapping, metadata, properties);
        }
        addDisabledMetadataMapping(mapping, metadata);
        final Map<String, Object> sourceMap = new HashMap<String, Object>(1);
        sourceMap.put(hdStoreName, mapping);
        return sourceMap;
    }
    
    private static void addCheckpointMapping(final JsonNode metadata, final Map<String, Object> properties) {
        if (metadata != null && metadata.isArray()) {
            for (final JsonNode item : metadata) {
                if ("checkpoint".equals(item.asText())) {
                    final Map<String, Object> columnProperties = new HashMap<String, Object>(1);
                    final String columnType = "long";
                    addColumnProperties(columnType, columnProperties, null);
                    final String columnName = "$checkpoint";
                    properties.put(columnName, columnProperties);
                }
            }
        }
    }
    
    private static void addEventTypeMapping(final Map<String, Object> properties, final JsonNode eventTypes) {
        for (final JsonNode eventType : eventTypes) {
            final String eventName = eventType.get("name").asText();
            final JsonNode eventAttributes = eventType.get("type");
            final Map<String, Object> eventProperties = getContextMapping(eventAttributes);
            final Map<String, Object> eventMapping = new HashMap<String, Object>(1);
            eventMapping.put("type", "nested");
            eventMapping.put("properties", eventProperties);
            properties.put(eventName, eventMapping);
        }
    }
    
    private static Map<String, Object> getContextMapping(final JsonNode context) {
        final Map<String, Object> properties = new HashMap<String, Object>(context.size());
        for (final JsonNode contextItem : context) {
            final String columnName = contextItem.get("name").asText();
            final String columnType = contextItem.get("type").asText();
            final JsonNode indexNode = contextItem.get("index");
            final Map<String, Object> columnProperties = new HashMap<String, Object>(1);
            addColumnProperties(columnType, columnProperties, indexNode);
            properties.put(columnName, columnProperties);
        }
        return properties;
    }
    
    private static void addMetadataMapping(final Map<String, Object> mapping, final JsonNode metadataItems, final Map<String, Object> properties) {
        for (final JsonNode metadataItem : metadataItems) {
            final String metadataItemName = metadataItem.asText();
            final String[] metadataFields = getMetadataFields(metadataItemName);
            if (!metadataFields[1].equalsIgnoreCase("_id") && metadataFields.length > 2) {
                final Map<String, Object> metadataProperties = new HashMap<String, Object>((metadataFields.length - 2) / 2);
                for (int index = 2; index < metadataFields.length; index += 2) {
                    final String metadataName = metadataFields[index];
                    final String metadataValue = metadataFields[index + 1];
                    addMetadataValue(metadataProperties, metadataName, metadataValue, properties);
                }
                mapping.put(metadataFields[1], metadataProperties);
            }
        }
    }
    
    private static Map<String, Object> getMetadataMappingForId(final JsonNode hdStoreSchema, final Map<String, Object> properties) {
        final JsonNode metadataItems = hdStoreSchema.get("metadata");
        if (metadataItems != null) {
            for (final JsonNode metadataItem : metadataItems) {
                final String metadataItemName = metadataItem.asText();
                final String[] metadataFields = getMetadataFields(metadataItemName);
                if (metadataFields[1].equalsIgnoreCase("_id") && metadataFields.length > 2) {
                    final Map<String, Object> mapping = new HashMap<String, Object>();
                    final Map<String, Object> metadataProperties = new HashMap<String, Object>((metadataFields.length - 2) / 2);
                    for (int index = 2; index < metadataFields.length; index += 2) {
                        final String metadataName = metadataFields[index];
                        final String metadataValue = metadataFields[index + 1];
                        addMetadataValue(metadataProperties, metadataName, metadataValue, properties);
                    }
                    mapping.put(metadataFields[1], metadataProperties);
                    return mapping;
                }
            }
        }
        return null;
    }
    
    private static void addMetadataValue(final Map<String, Object> metadataProperties, final String metadataName, final String metadataValue, final Map<String, Object> properties) {
        if (metadataValue.startsWith("PROPERTY:")) {
            final String propertyName = metadataValue.substring("PROPERTY:".length());
            final String propertyValue = Utility.getPropertyString(properties, propertyName);
            if (propertyValue != null) {
                metadataProperties.put(metadataName, propertyValue);
            }
        }
        else {
            metadataProperties.put(metadataName, metadataValue);
        }
    }
    
    private static void addDisabledMetadataMapping(final Map<String, Object> mapping, final JsonNode metadataItems) {
        final Collection<String> metadataList = new ArrayList<String>();
        if (metadataItems != null) {
            for (final JsonNode metadataItem : metadataItems) {
                final String metadataItemName = metadataItem.asText();
                metadataList.add(metadataItemName);
            }
        }
        for (final String[] metadataFields : HDStore.ES_METADATA_FIELDS_DISABLED) {
            if (!metadataList.contains(metadataFields[0])) {
                final Map<String, Object> metadataProperties = new HashMap<String, Object>((metadataFields.length - 2) / 2);
                for (int index = 2; index < metadataFields.length; index += 2) {
                    metadataProperties.put(metadataFields[index], metadataFields[index + 1]);
                }
                mapping.put(metadataFields[1], metadataProperties);
            }
        }
    }
    
    private static String[] getMetadataFields(final String metadataItemName) {
        for (final String[] metadataFields : HDStore.ES_METADATA_FIELDS) {
            if (metadataFields[0].equals(metadataItemName)) {
                return metadataFields;
            }
        }
        throw new HDStoreException(String.format("Invalid metadata item name, '%s'", metadataItemName));
    }
    
    private static void addColumnProperties(final String columnType, final Map<String, Object> columnProperties, final JsonNode indexNode) {
        switch (columnType) {
            case "string": {
                columnProperties.put("type", "keyword");
                columnProperties.put("index", "true");
                if (indexNode != null && indexNode.asText().equals("analyzed")) {
                    columnProperties.put("type", "text");
                    break;
                }
                break;
            }
            case "text": {
                columnProperties.put("type", "text");
                break;
            }
            case "binary64": {
                columnProperties.put("type", "binary");
                break;
            }
            case "boolean": {
                columnProperties.put("type", "boolean");
                break;
            }
            case "integer": {
                columnProperties.put("type", "integer");
                break;
            }
            case "long": {
                columnProperties.put("type", "long");
                break;
            }
            case "double": {
                columnProperties.put("type", "double");
                break;
            }
            case "date": {
                columnProperties.put("type", "date");
                columnProperties.put("format", "basic_date");
                break;
            }
            case "time": {
                columnProperties.put("type", "date");
                columnProperties.put("format", "basic_time");
                break;
            }
            case "datetime": {
                columnProperties.put("type", "date");
                columnProperties.put("format", "basic_date_time || epoch_millis");
                break;
            }
            default: {
                throw new HDStoreException(String.format("Unable to map HD data type '%s'", columnType));
            }
        }
        if (indexNode != null && !columnType.equals("string")) {
            columnProperties.put("index", indexNode.asText());
        }
    }
    
    private static JsonNode getSchemaFromMapping(final MappingMetaData mapping) {
        ObjectNode result = null;
        final Map<String, Object> sourceMap = getSourceMapFromMapping(mapping);
        final Object properties = (sourceMap != null) ? sourceMap.get("properties") : null;
        if (properties instanceof Map) {
            final Map<String, Object> propertyMap = (Map<String, Object>)properties;
            final ArrayNode context = Utility.nodeFactory.arrayNode();
            final ArrayNode metadata = Utility.nodeFactory.arrayNode();
            for (final Map.Entry<String, Object> property : propertyMap.entrySet()) {
                final String name = property.getKey();
                final Object value = property.getValue();
                final String type = getTypeName(value);
                final String metadataName = getMetadataName(name);
                if (metadataName == null) {
                    final ObjectNode contextItem = Utility.nodeFactory.objectNode();
                    contextItem.put("name", name);
                    contextItem.put("type", type);
                    context.add((JsonNode)contextItem);
                }
                else {
                    metadata.add(metadataName);
                }
            }
            for (final String[] metadataFields : HDStore.ES_METADATA_FIELDS) {
                if (metadataFields.length > 2 && sourceMap.get(metadataFields[1]) != null) {
                    metadata.add(metadataFields[0]);
                }
            }
            if (hasMetadataAll(sourceMap)) {
                metadata.add("any");
            }
            result = Utility.nodeFactory.objectNode();
            result.set("context", (JsonNode)context);
            if (metadata.size() > 0) {
                result.set("metadata", (JsonNode)metadata);
            }
        }
        return (JsonNode)result;
    }
    
    private static String getMetadataName(final String propertyName) {
        String result = null;
        for (final String[] metadataFields : HDStore.ES_METADATA_FIELDS) {
            if (metadataFields[1].equals(propertyName)) {
                result = metadataFields[0];
                break;
            }
        }
        return result;
    }
    
    private static boolean hasMetadataAll(final Map<String, Object> sourceMap) {
        final Object all = sourceMap.get("_all");
        if (all instanceof Map) {
            final Object allEnabled = ((Map)all).get("enabled");
            if (allEnabled instanceof Boolean) {
                return (boolean)allEnabled;
            }
        }
        return true;
    }
    
    private static String getTypeName(final Object value) {
        if (!(value instanceof Map)) {
            throw new HDStoreException(String.format("Unexpected object type: '%s'", value.getClass().getSimpleName()));
        }
        final Map<String, Object> typeMap = (Map<String, Object>)value;
        final String s;
        final String nativeType = s = (String)typeMap.get("type");
        String result = null;
        switch (s) {
            case "text": {
                result = "text";
                break;
            }
            case "keyword": {
                result = "string";
                break;
            }
            case "binary": {
                result = "binary64";
                break;
            }
            case "boolean": {
                result = "boolean";
                break;
            }
            case "integer": {
                result = "integer";
                break;
            }
            case "long": {
                result = "long";
                break;
            }
            case "double": {
                result = "double";
                break;
            }
            case "date": {
                result = getDateTypeName(typeMap);
                break;
            }
            default: {
                throw new HDStoreException(String.format("Cannot map data type '%s'", nativeType));
            }
        }
        return result;
    }
    
    private static String getDateTypeName(final Map<String, Object> typeMap) {
        final String s;
        final String format = s = (String)typeMap.get("format");
        String result = null;
        switch (s) {
            case "basic_date": {
                result = "date";
                break;
            }
            case "basic_time": {
                result = "time";
                break;
            }
            case "basic_date_time || epoch_millis": {
                result = "datetime";
                break;
            }
            default: {
                throw new HDStoreException(String.format("Cannot map date format '%s'", format));
            }
        }
        return result;
    }
    
    private static Map<String, Object> getSourceMapFromMapping(final MappingMetaData mapping) {
        return (Map<String, Object>)((mapping != null) ? mapping.sourceAsMap() : null);
    }
    
    @Override
    public Iterator<String> getNames() {
        final ImmutableOpenMap<String, MappingMetaData> mappings = this.getMappings(null);
        if (mappings != null) {
            return (Iterator<String>)mappings.keysIt();
        }
        return Collections.emptyIterator();
    }
    
    @Override
    public DataType get(final String dataTypeName, final Map<String, Object> properties) {
        DataType result = null;
        final MappingMetaData mapping = this.getMapping(dataTypeName);
        final JsonNode hdStoreSchema = getSchemaFromMapping(mapping);
        if (hdStoreSchema != null) {
            final DataType hdStore = new DataType(this, dataTypeName, hdStoreSchema, null, null);
            if (hdStore.isValid()) {
                result = hdStore;
            }
        }
        return result;
    }
    
    @Override
    public boolean remove(final String dataTypeName) {
        throw new NotImplementedException("The remove(String dataTypeName) method isn't implemented.");
    }
    
    @Override
    public DataType createDataType(final String dataTypeName, final JsonNode dataTypeSchema) {
        DataType result = null;
        if (this.getMapping(dataTypeName) == null) {
            result = this.setDataType(dataTypeName, dataTypeSchema, null);
        }
        else {
            HDStore.logger.warn((Object)String.format("Context type '%s' already exists in HDStore '%s'", dataTypeName, this.getName()));
        }
        return result;
    }
    
    @Override
    public DataType setDataType(final String dataTypeName, final JsonNode dataTypeSchema, final HStore ws) {
        DataType result = null;
        this.dataTypeName = dataTypeName;
        this.dataTypeSchema = dataTypeSchema;
        final Map<String, Object> idPropertiesMap = getMetadataMappingForId(dataTypeSchema, this.getProperties());
        final DataType hdStore = new DataType(this, dataTypeName, dataTypeSchema, ws, idPropertiesMap);
        final Map<String, Object> sourceMap = hdStore.isValid() ? getSourceMapFromSchema(dataTypeName, dataTypeSchema, this.getProperties()) : null;
        final Client client = this.getClient();
        if (sourceMap != null && client != null) {
            HDStore.logger.debug((Object)String.format("Context type '%s' has mapping '%s'", dataTypeName, sourceMap));
            final PutMappingRequestBuilder request = client.admin().indices().preparePutMapping(new String[0]).setIndices(new String[] { this.indexName }).setType(dataTypeName).setSource((Map)sourceMap);
            request.execute().actionGet();
            HDStore.logger.debug((Object)String.format("Context type '%s' created mapping in HDStore '%s'", dataTypeName, this.getName()));
            result = hdStore;
        }
        return result;
    }
    
    @Override
    public void flush() {
        final Client client = this.getClient();
        if (client != null) {
            final RefreshRequest request = new RefreshRequest(new String[] { this.indexName });
            client.admin().indices().refresh(request).actionGet();
            HDStore.logger.debug((Object)String.format("Flushed HDStore '%s' to memory", this.getName()));
        }
    }
    
    @Override
    public void fsync() {
        final Client client = this.getClient();
        if (client != null) {
            final FlushRequest request = new FlushRequest(new String[] { this.indexName });
            client.admin().indices().flush(request).actionGet();
            HDStore.logger.debug((Object)String.format("Flushed HDStore '%s' to disk", this.getName()));
        }
    }
    
    @Override
    public long getHDCount() {
        long result = 0L;
        final Client client = this.getClient();
        try {
            final SearchRequestBuilder requestBuilder = client.prepareSearch(new String[] { this.indexName });
            final SearchResponse response = (SearchResponse)requestBuilder.execute().actionGet();
            result = response.getHits().totalHits();
            HDStore.logger.debug((Object)String.format("HDStore '%s' contains %d HDs", this.getName(), result));
        }
        catch (IndexNotFoundException ignored) {
            HDStore.logger.warn((Object)String.format("HDStore '%s' does not exist", this.getName()));
        }
        return result;
    }
    
    private ImmutableOpenMap<String, MappingMetaData> getMappings(final String dataTypeName) {
        ImmutableOpenMap<String, MappingMetaData> result = null;
        final Client client = this.getClient();
        try {
            if (client != null) {
                GetMappingsRequestBuilder request = (GetMappingsRequestBuilder)client.admin().indices().prepareGetMappings(new String[0]).setIndices(new String[] { this.indexName });
                if (dataTypeName != null) {
                    request = (GetMappingsRequestBuilder)request.setTypes(new String[] { dataTypeName });
                }
                final GetMappingsResponse response = (GetMappingsResponse)request.execute().actionGet();
                result = (ImmutableOpenMap<String, MappingMetaData>)response.getMappings().get(this.indexName);
            }
        }
        catch (IndexNotFoundException ignored) {
            HDStore.logger.warn((Object)String.format("HDStore '%s' does not exist", this.getName()));
        }
        return result;
    }
    
    private MappingMetaData getMapping(final String dataTypeName) {
        MappingMetaData result = null;
        final ImmutableOpenMap<String, MappingMetaData> mappings = this.getMappings(dataTypeName);
        if (mappings != null) {
            result = (MappingMetaData)mappings.get(dataTypeName);
        }
        return result;
    }
    
    public String getActualName() {
        return this.actualName;
    }
    
    public String getIndexName() {
        return this.indexName;
    }
    
    public String getDataTypeName() {
        return this.dataTypeName;
    }
    
    public JsonNode getDataTypeSchema() {
        return this.dataTypeSchema;
    }
    
    private Client getClient() {
        Client result = null;
        final HDStoreManager manager = (HDStoreManager)this.getManager();
        if (manager != null) {
            result = manager.getClient();
        }
        return result;
    }
    
    static {
        thisClass = HDStore.class;
        logger = Logger.getLogger((Class)HDStore.thisClass);
        ES_METADATA_FIELDS = new String[][] { { "id", "_id", "index", "true", "path", "$id" }, { "checkpoint", "$checkpoint" }, { "any", "_all" } };
        ES_METADATA_FIELDS_DISABLED = new String[][] { { "any", "_all", "enabled", "false" } };
    }
}
