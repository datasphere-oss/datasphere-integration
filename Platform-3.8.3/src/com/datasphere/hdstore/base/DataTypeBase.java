package com.datasphere.hdstore.base;

import com.github.fge.jsonschema.main.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.datasphere.hdstore.exceptions.*;
import com.datasphere.hdstore.*;
import com.github.fge.jsonschema.core.report.*;
import org.apache.log4j.*;
import com.datasphere.persistence.*;
import java.util.*;

public abstract class DataTypeBase<T extends HDStore> implements DataType
{
    private static final Class<?> thisClass;
    private static final Logger logger;
    private static final Level hdLogLevel;
    private static JsonSchema hdStoreSchema;
    private static final Map<String, String> hdTypeToJsonType;
    private final T hdStore;
    private final String name;
    private final JsonNode schemaJson;
    private final ObjectNode dataSchemaJson;
    private final JsonSchema dataSchema;
    
    protected DataTypeBase(final T hdStore, final String name, final JsonNode schemaJson) {
        this.hdStore = hdStore;
        this.name = name;
        this.schemaJson = schemaJson;
        this.dataSchemaJson = Utility.nodeFactory.objectNode();
        this.dataSchema = this.createHDSchema();
    }
    
    private static boolean isHDStoreSchemaValid(final JsonNode schema) {
        return DataTypeBase.hdStoreSchema != null && schema != null && DataTypeBase.hdStoreSchema.validInstanceUnchecked(schema);
    }
    
    private static void addHDSchemaProperty(final ObjectNode properties, final JsonNode attribute, final ArrayNode required) {
        final String attributeName = attribute.get("name").asText();
        final String hdAttributeType = attribute.get("type").asText();
        final String attributeType = translateHDType(hdAttributeType);
        final JsonNode attributeCanBeNull = attribute.get("nullable");
        final boolean attributeNullable = attributeCanBeNull == null || attributeCanBeNull.asBoolean();
        final ObjectNode attributeTypeNode = Utility.nodeFactory.objectNode();
        if (attributeNullable) {
            final ArrayNode attributeTypes = Utility.nodeFactory.arrayNode();
            attributeTypes.add(attributeType);
            attributeTypes.add("null");
            attributeTypeNode.set("type", (JsonNode)attributeTypes);
        }
        else {
            attributeTypeNode.put("type", attributeType);
            required.add(attributeName);
        }
        properties.set(attributeName, (JsonNode)attributeTypeNode);
    }
    
    private static String translateHDType(final String hdType) {
        final String result = DataTypeBase.hdTypeToJsonType.get(hdType);
        if (result == null) {
            throw new HDStoreException(String.format("Unsupported context data type, '%s'", hdType));
        }
        return result;
    }
    
    private static void addHDSchemaEventType(final ObjectNode properties, final JsonNode eventType) {
        final ObjectNode eventTypeSchema = Utility.objectMapper.createObjectNode();
        eventTypeSchema.put("type", "array");
        final ObjectNode eventTypeDescription = Utility.objectMapper.createObjectNode();
        eventTypeDescription.put("type", "object");
        final ObjectNode eventTypeProperties = Utility.objectMapper.createObjectNode();
        final ArrayNode required = Utility.nodeFactory.arrayNode();
        for (final JsonNode attribute : eventType.get("type")) {
            addHDSchemaProperty(eventTypeProperties, attribute, required);
        }
        eventTypeDescription.set("properties", (JsonNode)eventTypeProperties);
        if (required.size() > 0) {
            eventTypeDescription.set("required", (JsonNode)required);
        }
        eventTypeDescription.put("additionalProperties", false);
        eventTypeSchema.set("items", (JsonNode)eventTypeDescription);
        final String eventTypeName = eventType.get("name").asText();
        properties.set(eventTypeName, (JsonNode)eventTypeSchema);
    }
    
    @Override
    public T getHDStore() {
        return this.hdStore;
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public JsonNode getSchemaJson() {
        return this.schemaJson;
    }
    
    @Override
    public JsonSchema getDataSchema() {
        return this.dataSchema;
    }
    
    @Override
    public boolean isValid() {
        return this.hdStore != null && this.schemaJson != null && this.dataSchema != null && this.name != null && !this.name.isEmpty();
    }
    
    private JsonSchema createHDSchema() {
        JsonSchema result = null;
        if (this.isHDSchemaValid()) {
            final ObjectNode properties = Utility.nodeFactory.objectNode();
            final ArrayNode required = Utility.nodeFactory.arrayNode();
            this.startHDSchemaJson(this.name);
            for (final JsonNode attribute : this.schemaJson.get("context")) {
                addHDSchemaProperty(properties, attribute, required);
            }
            if (this.schemaJson.get("events") != null) {
                for (final JsonNode eventType : this.schemaJson.get("events")) {
                    addHDSchemaEventType(properties, eventType);
                }
            }
            this.finishHDSchemaJson(properties, required);
            result = Utility.createJsonSchema((JsonNode)this.dataSchemaJson);
            this.reportSchemaCreateResult(result != null);
        }
        return result;
    }
    
    private void startHDSchemaJson(final String title) {
        this.dataSchemaJson.put("$schema", "http://json-schema.org/draft-04/schema#");
        this.dataSchemaJson.put("title", title);
        this.dataSchemaJson.put("type", "object");
    }
    
    private void finishHDSchemaJson(final ObjectNode properties, final ArrayNode required) {
        this.dataSchemaJson.set("properties", (JsonNode)properties);
        if (required.size() > 0) {
            this.dataSchemaJson.set("required", (JsonNode)required);
        }
        this.dataSchemaJson.put("additionalProperties", true);
    }
    
    private void reportSchemaCreateResult(final boolean valid) {
        if (valid) {
            DataTypeBase.logger.debug((Object)String.format("Created a schema for HDStore '%s' from '%s'", this.name, this.dataSchemaJson.toString()));
        }
        else {
            DataTypeBase.logger.error((Object)String.format("Cannot create a schema for HDStore '%s' from '%s'", this.name, this.dataSchemaJson.toString()));
        }
    }
    
    private boolean isHDSchemaValid() {
        if (this.schemaJson == null) {
            DataTypeBase.logger.error((Object)String.format("HDStore '%s' schema is missing", this.name));
            return false;
        }
        if (!isHDStoreSchemaValid(this.schemaJson)) {
            DataTypeBase.logger.error((Object)String.format("HDStore '%s' schema is invalid: '%s'", this.name, this.schemaJson));
            return false;
        }
        return true;
    }
    
    @Override
    public boolean isValid(final HD hd) {
        boolean result = true;
        if (hd != null) {
            final ProcessingReport report = this.dataSchema.validateUnchecked((JsonNode)hd);
            if (report != null && !report.isSuccess()) {
                this.logInvalidHD(hd, (Iterable<ProcessingMessage>)report);
                result = false;
            }
        }
        return result;
    }
    
    private void logInvalidHD(final HD hd, final Iterable<ProcessingMessage> report) {
        if (DataTypeBase.logger.isEnabledFor((Priority)DataTypeBase.hdLogLevel)) {
            DataTypeBase.logger.log((Priority)DataTypeBase.hdLogLevel, (Object)String.format("Schema  : %s", this.dataSchemaJson));
            DataTypeBase.logger.log((Priority)DataTypeBase.hdLogLevel, (Object)String.format("HD : %s", hd));
            int messageNumber = 1;
            for (final ProcessingMessage message : report) {
                final JsonNode instanceNode = message.asJson().path("instance").path("pointer");
                final String instanceName = instanceNode.isMissingNode() ? "<Unknown>" : instanceNode.asText().replace("/", "");
                final String messageText = message.getMessage();
                DataTypeBase.logger.log((Priority)DataTypeBase.hdLogLevel, (Object)String.format("Error %2d: %s : %s", messageNumber, instanceName, messageText));
                ++messageNumber;
            }
        }
    }
    
    @Override
    public boolean insert(final HD hd) {
        final List<HD> hds = new ArrayList<HD>(1);
        hds.add(hd);
        return this.insert(hds, null);
    }
    
    static {
        thisClass = DataTypeBase.class;
        logger = Logger.getLogger((Class)DataTypeBase.thisClass);
        hdLogLevel = Level.WARN;
        DataTypeBase.hdStoreSchema = Utility.createJsonSchema("/HDStore.json");
        (hdTypeToJsonType = new HashMap<String, String>(10)).put("string", "string");
        DataTypeBase.hdTypeToJsonType.put("text", "string");
        DataTypeBase.hdTypeToJsonType.put("binary64", "string");
        DataTypeBase.hdTypeToJsonType.put("date", "string");
        DataTypeBase.hdTypeToJsonType.put("time", "string");
        DataTypeBase.hdTypeToJsonType.put("boolean", "boolean");
        DataTypeBase.hdTypeToJsonType.put("integer", "integer");
        DataTypeBase.hdTypeToJsonType.put("long", "integer");
        DataTypeBase.hdTypeToJsonType.put("datetime", "integer");
        DataTypeBase.hdTypeToJsonType.put("double", "number");
    }
}
