package com.datasphere.hdstore;

import com.fasterxml.jackson.databind.node.*;
import com.github.fge.jsonschema.main.*;
import com.github.fge.jsonschema.core.exceptions.*;
import com.github.fge.jackson.*;
import org.apache.commons.codec.binary.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import org.apache.log4j.*;
import java.util.*;
import com.fasterxml.jackson.datatype.joda.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.annotation.*;

public final class Utility
{
    public static final JsonNodeFactory nodeFactory;
    public static final ObjectMapper objectMapper;
    private static final Logger logger;
    private static final JsonSchemaFactory schemaFactory;
    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long MESSAGE_THRESHOLD_WARN = 10000L;
    private static final long MESSAGE_THRESHOLD_INFO = 1000L;
    private static final long MESSAGE_THRESHOLD_DEBUG = 500L;
    
    public static JsonSchema createJsonSchema(final JsonNode jsonNode) {
        JsonSchema result = null;
        if (jsonNode != null) {
            try {
                result = Utility.schemaFactory.getJsonSchema(jsonNode);
            }
            catch (ProcessingException exception) {
                Utility.logger.error((Object)"JsonSchema not created", (Throwable)exception);
            }
        }
        return result;
    }
    
    public static JsonSchema createJsonSchema(final String schemaResourceName) {
        JsonNode jsonNode = null;
        try {
            jsonNode = JsonLoader.fromResource(schemaResourceName);
        }
        catch (IOException exception) {
            Utility.logger.error((Object)String.format("Schema resource, '%s', not loaded", schemaResourceName), (Throwable)exception);
        }
        return createJsonSchema(jsonNode);
    }
    
    public static JsonNode readTree(final String content) {
        JsonNode result = null;
        try {
            result = Utility.objectMapper.readTree(content);
        }
        catch (IOException exception) {
            Utility.logger.warn((Object)String.format("Cannot parse JSON value '%s'", content), (Throwable)exception);
        }
        return result;
    }
    
    public static String serializeToBase64String(final Serializable object) {
        String result = null;
        if (object != null) {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try (final ObjectOutput objectOutput = new ObjectOutputStream(stream)) {
                objectOutput.writeObject(object);
                objectOutput.close();
                final byte[] encoded = Base64.encodeBase64(stream.toByteArray());
                result = new String(encoded);
            }
            catch (IOException exception) {
                Utility.logger.warn((Object)exception.getMessage(), (Throwable)exception);
            }
        }
        return result;
    }
    
    public static Object serializeFromBase64String(final String serialized) {
        Object result = null;
        if (serialized != null) {
            final byte[] decoded = Base64.decodeBase64(serialized);
            final InputStream stream = new ByteArrayInputStream(decoded);
            try (final ObjectInput objectInput = new ObjectInputStream(stream)) {
                result = objectInput.readObject();
            }
            catch (IOException | ClassNotFoundException ex2) {
                Utility.logger.warn((Object)ex2.getMessage(), (Throwable)ex2);
            }
        }
        return result;
    }
    
    public static void reportExecutionTime(final Logger reportLogger, final long milliseconds) {
        Level logLevel;
        if (milliseconds < 500L) {
            logLevel = Level.DEBUG;
        }
        else if (milliseconds < 1000L) {
            logLevel = Level.INFO;
        }
        else {
            logLevel = ((milliseconds < 10000L) ? Level.WARN : Level.ERROR);
        }
        reportLogger.log((Priority)logLevel, (Object)String.format("Execution time: %d.%03d seconds", milliseconds / 1000L, milliseconds % 1000L));
        if (Utility.logger.isDebugEnabled()) {
            Utility.logger.debug((Object)String.format("Execution time: %d.%03d seconds", milliseconds / 1000L, milliseconds % 1000L));
        }
    }
    
    public static Integer getPropertyInteger(final Map<String, Object> properties, final String propertyName) {
        Integer result = null;
        final Object property = (properties != null) ? properties.get(propertyName) : null;
        if (property != null) {
            result = getIntegerValue(property);
        }
        return result;
    }
    
    private static Integer getIntegerValue(final Object property) {
        Integer result = null;
        if (property instanceof Number) {
            final Number number = (Number)property;
            result = number.intValue();
        }
        else {
            try {
                result = Integer.valueOf(property.toString());
            }
            catch (NumberFormatException ex) {}
        }
        return result;
    }
    
    public static String getPropertyString(final Map<String, Object> properties, final String propertyName) {
        final Object property = (properties != null) ? properties.get(propertyName) : null;
        return (String)property;
    }
    
    public static int extractInt(final Object object) {
        if (object instanceof Integer) {
            return (int)object;
        }
        return Integer.parseInt(object.toString());
    }
    
    static {
        nodeFactory = new JsonNodeFactory(false);
        objectMapper = new ObjectMapper();
        logger = Logger.getLogger((Class)Utility.class);
        schemaFactory = JsonSchemaFactory.byDefault();
        Utility.objectMapper.registerModule((Module)new JodaModule());
        Utility.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        Utility.objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        Utility.objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }
}
