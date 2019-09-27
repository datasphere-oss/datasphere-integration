package com.datasphere.source.lib.utils;

import org.dom4j.tree.*;
import org.apache.commons.lang3.*;
import org.codehaus.jettison.json.*;
import java.util.*;
import java.io.*;

import com.datasphere.source.lib.formatter.dsv.util.*;
import com.fasterxml.jackson.databind.*;

class MapFieldModifier extends FieldModifier
{
    private boolean formatAsJson;
    private String emptyJsonObjectString;
    private String openCurlyBrace;
    private String closeCurlyBrace;
    private String quote;
    private String delimiter;
    private boolean isXML;
    private DSVFormatterUtil dsvFormatterUtil;
    private ObjectMapper mapper;
    
    MapFieldModifier(final Map<String, Object> modifierProperties) {
        this.formatAsJson = false;
        this.emptyJsonObjectString = "";
        this.openCurlyBrace = "";
        this.closeCurlyBrace = "";
        this.quote = "";
        this.delimiter = ",";
        this.isXML = false;
        this.mapper = new ObjectMapper();
        final String formatterClassName = (String)modifierProperties.get("handler");
        if (formatterClassName.toLowerCase().contains("dsv")) {
            this.dsvFormatterUtil = new DSVFormatterUtil(modifierProperties);
            final String delimiter = this.dsvFormatterUtil.getColumnDelimiter();
            if (delimiter != null && !delimiter.trim().isEmpty()) {
                this.delimiter = delimiter;
            }
        }
        if (formatterClassName.toLowerCase().contains("json")) {
            this.formatAsJson = true;
            this.emptyJsonObjectString = "{}";
            this.openCurlyBrace = "{";
            this.closeCurlyBrace = "}";
            this.quote = "\"";
        }
        if (formatterClassName.toLowerCase().contains("xml")) {
            this.isXML = true;
        }
    }
    
    @Override
    public String modifyFieldValue(final Object fieldValue, final Object event) {
        final Map<?, ?> metadataMap = (Map<?, ?>)fieldValue;
        final Iterator<?> metadataMapIterator = metadataMap.entrySet().iterator();
        if (this.isXML) {
            final DefaultElement element = new DefaultElement("event-metadata");
            if (!metadataMapIterator.hasNext()) {
                return element.asXML();
            }
            while (metadataMapIterator.hasNext()) {
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>)metadataMapIterator.next();
                element.addAttribute(e.getKey().toString(), e.getValue().toString());
            }
            return element.asXML();
        }
        else {
            if (!metadataMapIterator.hasNext()) {
                return this.emptyJsonObjectString;
            }
            final StringBuilder metadataBuilder = new StringBuilder();
            metadataBuilder.append(this.openCurlyBrace);
            while (true) {
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>)metadataMapIterator.next();
                if (e != null) {
                    final String key = e.getKey().toString();
                    Object value = e.getValue();
                    if (this.formatAsJson) {
                        metadataBuilder.append(this.quote + key + this.quote + ":");
                        if (value == null) {
                            value = "null";
                        }
                        else if (!this.isValueJSON(value.toString()) && !ClassUtils.isPrimitiveOrWrapper((Class)value.getClass())) {
                            value = JSONObject.quote(value.toString());
                        }
                    }
                    else if (value != null) {
                        value = this.dsvFormatterUtil.quoteValue(value.toString());
                    }
                    else {
                        value = this.dsvFormatterUtil.getNullValue();
                    }
                    metadataBuilder.append(value);
                    if (!metadataMapIterator.hasNext()) {
                        break;
                    }
                    metadataBuilder.append(this.delimiter);
                }
            }
            return metadataBuilder.append(this.closeCurlyBrace).toString();
        }
    }
    
    private boolean isValueJSON(final String value) {
        try {
            final JsonNode jsonNode = this.mapper.readTree(value);
            return jsonNode.isObject() || jsonNode.isArray();
        }
        catch (IOException e) {
            return false;
        }
    }
}
