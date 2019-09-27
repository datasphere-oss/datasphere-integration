package com.datasphere.source.lib.utils;

import java.util.*;
import java.lang.reflect.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import org.apache.commons.lang3.*;
import org.codehaus.jettison.json.*;

class ArrayFieldModifier extends FieldModifier
{
    private BaseArray typedArray;
    private boolean formatAsJson;
    private String nullValue;
    private String emptyJsonObjectString;
    private String openCurlyBrace;
    private String closeCurlyBrace;
    private String quote;
    private String delimiter;
    
    ArrayFieldModifier(final Map<String, Object> modifierProperties) {
        this.formatAsJson = false;
        this.emptyJsonObjectString = "";
        this.openCurlyBrace = "";
        this.closeCurlyBrace = "";
        this.quote = "";
        this.delimiter = ",";
        final Property prop = new Property(modifierProperties);
        final String formatterClassName = (String)modifierProperties.get("handler");
        this.nullValue = prop.getString("nullvalue", "NULL");
        final String delimiter = (String)modifierProperties.get("columndelimiter");
        if (delimiter != null && !delimiter.trim().isEmpty()) {
            this.delimiter = delimiter;
        }
        if (formatterClassName.toLowerCase().contains("json")) {
            this.formatAsJson = true;
            this.nullValue = "null";
            this.emptyJsonObjectString = "{}";
            this.openCurlyBrace = "{";
            this.closeCurlyBrace = "}";
            this.quote = "\"";
        }
    }
    
    public void setTypedArray(final BaseArray typedArray) {
        this.typedArray = typedArray;
    }
    
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) {
        return this.typedArray.toString(fieldValue);
    }
    
    abstract static class BaseArray
    {
        static BaseArray getTypedArray(final Field field, final ArrayFieldModifier arrayFieldModifier) throws AdapterException {
            if (field.getType() == Object[].class) {
                return arrayFieldModifier.new ObjectArray();
            }
            if (field.getType() == int[].class) {
                return arrayFieldModifier.new IntegerArray();
            }
            if (field.getType() == boolean[].class) {
                return arrayFieldModifier.new BooleanArray();
            }
            if (field.getType() == float[].class) {
                return arrayFieldModifier.new FloatArray();
            }
            if (field.getType() == double[].class) {
                return arrayFieldModifier.new DoubleArray();
            }
            if (field.getType() == long[].class) {
                return arrayFieldModifier.new LongArray();
            }
            if (field.getType() == char[].class) {
                return arrayFieldModifier.new CharacterArray();
            }
            if (field.getType() == byte[].class) {
                return arrayFieldModifier.new ByteArray();
            }
            if (field.getType() == short[].class) {
                return arrayFieldModifier.new ShortArray();
            }
            throw new AdapterException("Failure in writer initialization. Unsupported array type " + field.getType().getCanonicalName());
        }
        
        abstract String toString(final Object p0);
    }
    
    class IntegerArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final int[] dataArray = (int[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class BooleanArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final boolean[] dataArray = (boolean[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class FloatArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final float[] dataArray = (float[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class DoubleArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final double[] dataArray = (double[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class ShortArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final short[] dataArray = (short[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class LongArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final long[] dataArray = (long[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class ObjectArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final Object[] dataArray = (Object[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                Object dataValue = dataArray[i];
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                else if (ArrayFieldModifier.this.formatAsJson && !ClassUtils.isPrimitiveOrWrapper((Class)dataValue.getClass())) {
                    dataValue = JSONObject.quote(dataValue.toString());
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class CharacterArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final char[] dataArray = (char[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(ArrayFieldModifier.this.quote + dataValue + ArrayFieldModifier.this.quote);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
    
    class ByteArray extends BaseArray
    {
        @Override
        String toString(final Object fieldValue) {
            final byte[] dataArray = (byte[])fieldValue;
            if (dataArray == null) {
                return ArrayFieldModifier.this.nullValue;
            }
            final int lastIndex = dataArray.length - 1;
            if (lastIndex == -1) {
                return ArrayFieldModifier.this.emptyJsonObjectString;
            }
            final StringBuilder b = new StringBuilder();
            b.append(ArrayFieldModifier.this.openCurlyBrace);
            int i = 0;
            while (true) {
                String dataValue = String.valueOf(dataArray[i]);
                if (dataValue == null) {
                    dataValue = ArrayFieldModifier.this.nullValue;
                }
                if (ArrayFieldModifier.this.formatAsJson) {
                    b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
                }
                b.append(dataValue);
                if (i == lastIndex) {
                    break;
                }
                b.append(ArrayFieldModifier.this.delimiter);
                ++i;
            }
            return b.append(ArrayFieldModifier.this.closeCurlyBrace).toString();
        }
    }
}
