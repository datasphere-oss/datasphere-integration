package com.datasphere.source.lib.utils;

import java.lang.reflect.*;
import java.util.*;

import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;
import com.datasphere.source.lib.formatter.*;

import org.apache.commons.lang3.*;

public abstract class FieldModifier
{
    public static FieldModifier[] getFieldModifiers(final Map<String, Object> properties, final Field[] fields, final String formatterType) throws AdapterException {
        final FieldModifier[] fieldModifier = new FieldModifier[fields.length];
        int i = 0;
        for (final Field field : fields) {
            if (field == null) {
                fieldModifier[i] = new DefaultFieldModifier();
            }
            else if (field.getType() == String.class) {
                if (formatterType.equalsIgnoreCase("json")) {
                    fieldModifier[i] = new JSONStringValueFormatter();
                }
                else {
                    fieldModifier[i] = new DefaultFieldModifier();
                }
            }
            else if (Position.class.isAssignableFrom(field.getType())) {
                if (formatterType.equalsIgnoreCase("xml")) {
                    fieldModifier[i] = new XMLPositionFieldModifier();
                }
            }
            else if (Collection.class.isAssignableFrom(field.getType())) {
                final String collectionDelimiter = (String)properties.get("columndelimiter");
                fieldModifier[i] = new CollectionFieldModifier(collectionDelimiter);
            }
            else if (Map.class.isAssignableFrom(field.getType())) {
                fieldModifier[i] = new MapFieldModifier(properties);
            }
            else if (field.getType().isArray()) {
                if (field.getDeclaringClass().getSimpleName().equalsIgnoreCase("waevent")) {
                    if (formatterType.equalsIgnoreCase("dsv")) {
                        fieldModifier[i] = new DSVCDCArrayFormatter(field, properties);
                    }
                    else if (formatterType.equalsIgnoreCase("json")) {
                        fieldModifier[i] = new JSONCDCArrayFormatter(field, properties);
                    }
                    else {
                        if (!formatterType.equalsIgnoreCase("xml")) {
                            throw new AdapterException("Formatting HDEvent data field value for " + formatterType + "formatter is not supported");
                        }
                        fieldModifier[i] = new XMLCDCArrayFormatter(field);
                    }
                }
                else {
                    final ArrayFieldModifier arrayFieldModifier = new ArrayFieldModifier(properties);
                    arrayFieldModifier.setTypedArray(ArrayFieldModifier.BaseArray.getTypedArray(field, arrayFieldModifier));
                    fieldModifier[i] = arrayFieldModifier;
                }
            }
            else if (formatterType.equalsIgnoreCase("json") && !ClassUtils.isPrimitiveOrWrapper((Class)field.getType())) {
                if (!field.getType().getName().equals("org.apache.avro.generic.GenericRecord") && !field.getType().getName().equals("com.fasterxml.jackson.databind.JsonNode")) {
                    fieldModifier[i] = new JSONStringValueFormatter();
                }
                else {
                    fieldModifier[i] = new DefaultFieldModifier();
                }
            }
            else {
                fieldModifier[i] = new DefaultFieldModifier();
            }
            ++i;
        }
        return fieldModifier;
    }
    
    public abstract String modifyFieldValue(final Object p0, final Object p1) throws Exception;
}
