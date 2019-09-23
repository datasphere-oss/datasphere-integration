package com.datasphere.runtime.utils;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.avro.util.*;
import org.apache.avro.*;
import java.util.*;
import org.apache.avro.generic.*;
import java.nio.*;

public abstract class RecordUtil
{
    static ObjectMapper mapper;
    static JsonNodeFactory jnf;
    
    public static JsonNode JSONFrom(final Object value) {
        if (value != null) {
            try {
                return new ObjectMapper().readTree(value.toString());
            }
            catch (IOException e) {
                throw new RuntimeException("Could not generate JsonNode from " + value + ": " + e);
            }
        }
        return (JsonNode)new ObjectNode(RecordUtil.jnf);
    }
    
    public static JsonNode JSONNew() {
        return JSONFrom(null);
    }
    
    public static JsonNode JSONPut(final JsonNode node, final String field, final Object value) {
        if (node instanceof ObjectNode) {
            if (value instanceof String) {
                ((ObjectNode)node).put(field, (String)value);
            }
            else if (value instanceof Boolean) {
                ((ObjectNode)node).put(field, (Boolean)value);
            }
            else if (value instanceof Double) {
                ((ObjectNode)node).put(field, (Double)value);
            }
            else if (value instanceof Float) {
                ((ObjectNode)node).put(field, (Float)value);
            }
            else if (value instanceof Integer) {
                ((ObjectNode)node).put(field, (Integer)value);
            }
            else if (value instanceof Long) {
                ((ObjectNode)node).put(field, (Long)value);
            }
            else if (value instanceof Short) {
                ((ObjectNode)node).put(field, (Short)value);
            }
            else if (value instanceof JsonNode) {
                ((ObjectNode)node).set(field, (JsonNode)value);
            }
            else if (value == null) {
                ((ObjectNode)node).putNull(field);
            }
        }
        return node;
    }
    
    public static JsonNode JSONArrayAdd(final JsonNode node, final Object value) {
        if (node instanceof ArrayNode) {
            if (value instanceof String) {
                ((ArrayNode)node).add((String)value);
            }
            else if (value instanceof Boolean) {
                ((ArrayNode)node).add((Boolean)value);
            }
            else if (value instanceof Double) {
                ((ArrayNode)node).add((Double)value);
            }
            else if (value instanceof Float) {
                ((ArrayNode)node).add((Float)value);
            }
            else if (value instanceof Integer) {
                ((ArrayNode)node).add((Integer)value);
            }
            else if (value instanceof Long) {
                ((ArrayNode)node).add((Long)value);
            }
            else if (value instanceof Short) {
                ((ArrayNode)node).add((int)(short)value);
            }
            else if (value instanceof JsonNode) {
                ((ArrayNode)node).add((JsonNode)value);
            }
            else if (value instanceof Utf8) {
                ((ArrayNode)node).add(value.toString());
            }
            else if (value == null) {
                ((ArrayNode)node).addNull();
            }
        }
        return node;
    }
    
    public static String JSONGetString(final JsonNode node, final String field) {
        return node.get(field).textValue();
    }
    
    public static Boolean JSONGetBoolean(final JsonNode node, final String field) {
        return node.get(field).booleanValue();
    }
    
    public static Double JSONGetDouble(final JsonNode node, final String field) {
        return node.get(field).doubleValue();
    }
    
    public static Integer JSONGetInteger(final JsonNode node, final String field) {
        return node.get(field).intValue();
    }
    
    public static GenericRecord AvroPut(final GenericRecord parent, final String name, final Object value) {
        parent.put(name, value);
        return parent;
    }
    
    public static Object AvroGet(final GenericRecord parent, final String name) {
        return parent.get(name);
    }
    
    public static JsonNode AvroToJson(final Object datum, final boolean ignoreNulls) {
        return (JsonNode)AvroDatumToJson(datum, ignoreNulls);
    }
    
    protected static Object AvroDatumToJson(final Object datum, final boolean ignoreNulls) {
        if (isRecord(datum)) {
            return AvroRecordToJson(datum, ignoreNulls);
        }
        if (isArray(datum)) {
            return AvroArrayToJson(datum, ignoreNulls);
        }
        if (isMap(datum)) {
            return AvroMapToJson(datum, ignoreNulls);
        }
        if (isString(datum)) {
            return datum.toString();
        }
        return datum;
    }
    
    private static JsonNode AvroRecordToJson(final Object datum, final boolean ignoreNulls) {
        final ObjectNode record = new ObjectNode(RecordUtil.jnf);
        final Schema schema = getRecordSchema(datum);
        for (final Schema.Field f : schema.getFields()) {
            final Object val = AvroDatumToJson(getField(datum, f.name(), f.pos()), ignoreNulls);
            if (val != null || !ignoreNulls) {
                JSONPut((JsonNode)record, f.name(), val);
            }
        }
        return (JsonNode)record;
    }
    
    private static JsonNode AvroArrayToJson(final Object datum, final boolean ignoreNulls) {
        final Collection<?> array = (Collection<?>)datum;
        final ArrayNode an = new ArrayNode(RecordUtil.jnf);
        for (final Object element : array) {
            final Object val = AvroDatumToJson(element, ignoreNulls);
            if (val != null || !ignoreNulls) {
                JSONArrayAdd((JsonNode)an, val);
            }
        }
        return (JsonNode)an;
    }
    
    private static JsonNode AvroMapToJson(final Object datum, final boolean ignoreNulls) {
        final ObjectNode record = new ObjectNode(RecordUtil.jnf);
        final Map<Object, Object> map = (Map<Object, Object>)datum;
        for (final Map.Entry<Object, Object> entry : map.entrySet()) {
            final Object val = AvroDatumToJson(entry.getValue(), ignoreNulls);
            if (val != null || !ignoreNulls) {
                JSONPut((JsonNode)record, entry.getKey().toString(), val);
            }
        }
        return (JsonNode)record;
    }
    
    protected static boolean isArray(final Object datum) {
        return datum instanceof Collection;
    }
    
    protected static boolean isRecord(final Object datum) {
        return datum instanceof IndexedRecord;
    }
    
    protected static Schema getRecordSchema(final Object record) {
        return ((GenericContainer)record).getSchema();
    }
    
    protected static boolean isEnum(final Object datum) {
        return datum instanceof GenericEnumSymbol;
    }
    
    protected static Schema getEnumSchema(final Object enu) {
        return ((GenericContainer)enu).getSchema();
    }
    
    protected static boolean isMap(final Object datum) {
        return datum instanceof Map;
    }
    
    protected static boolean isFixed(final Object datum) {
        return datum instanceof GenericFixed;
    }
    
    protected static Schema getFixedSchema(final Object fixed) {
        return ((GenericContainer)fixed).getSchema();
    }
    
    protected static boolean isString(final Object datum) {
        return datum instanceof CharSequence;
    }
    
    protected static boolean isBytes(final Object datum) {
        return datum instanceof ByteBuffer;
    }
    
    protected static boolean isInteger(final Object datum) {
        return datum instanceof Integer;
    }
    
    protected static boolean isLong(final Object datum) {
        return datum instanceof Long;
    }
    
    protected static boolean isFloat(final Object datum) {
        return datum instanceof Float;
    }
    
    protected static boolean isDouble(final Object datum) {
        return datum instanceof Double;
    }
    
    protected static boolean isBoolean(final Object datum) {
        return datum instanceof Boolean;
    }
    
    public static Object getField(final Object record, final String name, final int position) {
        return ((IndexedRecord)record).get(position);
    }
    
    static {
        RecordUtil.mapper = new ObjectMapper();
        RecordUtil.jnf = JsonNodeFactory.instance;
    }
}
