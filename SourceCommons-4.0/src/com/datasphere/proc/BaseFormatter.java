package com.datasphere.proc;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.intf.Formatter;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.utils.FieldModifier;

public abstract class BaseFormatter implements Formatter
{
    protected Field[] fields;
    protected String charset;
    protected String rowDelimiter;
    protected FieldModifier[] fieldModifiers;
    protected boolean exactlyOnceProcessing;
    protected boolean validHeaderAndFooter;
    private Map<String, Object> formatterProperties;
    
    public BaseFormatter() {
        this.exactlyOnceProcessing = false;
        this.validHeaderAndFooter = false;
    }
    
    public BaseFormatter(final Map<String, Object> formatterProperties, final Field[] fields) throws AdapterException {
        this.exactlyOnceProcessing = false;
        this.validHeaderAndFooter = false;
        if (fields != null) {
            this.fields = Arrays.copyOf(fields, fields.length);
        }
        if (formatterProperties != null) {
            (this.formatterProperties = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER)).putAll(formatterProperties);
        }
        final Property prop = new Property(formatterProperties);
        final String charset = (String)formatterProperties.get(Property.CHARSET);
        if (charset != null && !charset.isEmpty()) {
            this.charset = charset;
        }
        else {
            this.charset = Charset.defaultCharset().name();
        }
        this.rowDelimiter = prop.getString("rowdelimiter", "\n");
        if (formatterProperties.get("e1p") != null) {
            this.exactlyOnceProcessing = (boolean)formatterProperties.get("e1p");
        }
    }
    
    public Map<String, Object> getFormatterProperties() {
        return this.formatterProperties;
    }
    
    public Field[] getFields() {
        return this.fields;
    }
    
    public boolean hasValidHeaderAndFooter() {
        return this.validHeaderAndFooter;
    }
}
