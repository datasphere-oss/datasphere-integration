package com.datasphere.hd;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datasphere.event.ObjectMapperFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datasphere.uuid.UUID;

import flexjson.JSON;

public class HDContext implements Map<String, Object>, Serializable
{
    private static final transient long serialVersionUID = 1397157221930042651L;
    @JSON(include = false)
    @JsonIgnore
    protected static transient ObjectMapper jsonMapper;
    @JSON(include = false)
    @JsonIgnore
    public final transient Map<String, Object> map;
    public UUID hdID;
    
    public HDContext() {
        this.map = new HashMap<String, Object>();
    }
    
    protected Object getWrappedValue(final boolean val) {
        return new Boolean(val);
    }
    
    protected Object getWrappedValue(final char val) {
        return new Character(val);
    }
    
    protected Object getWrappedValue(final short val) {
        return new Short(val);
    }
    
    protected Object getWrappedValue(final int val) {
        return new Integer(val);
    }
    
    protected Object getWrappedValue(final long val) {
        return new Long(val);
    }
    
    protected Object getWrappedValue(final float val) {
        return new Float(val);
    }
    
    protected Object getWrappedValue(final double val) {
        return new Double(val);
    }
    
    protected Object getWrappedValue(final Object val) {
        return val;
    }
    
    protected String getWrappedValue(final String val) {
        return new String(val);
    }
    
    public String toJSON() {
        try {
            return HDContext.jsonMapper.writeValueAsString((Object)this);
        }
        catch (Exception ex) {
            return "<Undeserializable>";
        }
    }
    
    @Override
    public int size() {
        return this.map.size();
    }
    
    @JSON(include = false)
    @JsonIgnore
    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }
    
    @Override
    public boolean containsKey(final Object key) {
        return this.map.containsKey(key);
    }
    
    @Override
    public boolean containsValue(final Object value) {
        return this.map.containsValue(value);
    }
    
    @Override
    public Object remove(final Object key) {
        return this.map.remove(key);
    }
    
    @Override
    public void putAll(final Map<? extends String, ?> m) {
        this.map.putAll(m);
    }
    
    @Override
    public void clear() {
        this.map.clear();
    }
    
    @Override
    public Set<String> keySet() {
        return this.map.keySet();
    }
    
    @Override
    public Collection<Object> values() {
        return this.map.values();
    }
    
    @Override
    public Set<Entry<String, Object>> entrySet() {
        return this.map.entrySet();
    }
    
    @Override
    public Object get(final Object key) {
        return this.map.get(key);
    }
    
    @Override
    public Object put(final String key, final Object value) {
        return this.map.put(key, value);
    }
    
    @Override
    public String toString() {
        return this.toJSON();
    }
    
    static {
        HDContext.jsonMapper = ObjectMapperFactory.newInstance();
    }
}
