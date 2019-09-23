package com.datasphere.runtime;

import org.apache.log4j.*;
import com.datasphere.hdstore.*;
import java.util.*;

public class HStorePersistencePolicy
{
    private static Logger logger;
    public final Interval howOften;
    public final Type type;
    public final List<Property> properties;
    
    public HStorePersistencePolicy(final Interval howOften, final List<Property> properties) {
        this.properties = new ArrayList<Property>(1);
        if (howOften != null && (properties == null || (properties != null && properties.isEmpty()))) {
            throw new RuntimeException("Invalid Property set.");
        }
        Type hdStoreType = Type.INTERVAL;
        this.howOften = howOften;
        if (properties != null) {
            for (final Property property : properties) {
                final String propertyName = property.name;
                if (propertyName.equalsIgnoreCase("storageProvider")) {
                    final String propertyValueString = (String)property.value;
                    if (!propertyValueString.equalsIgnoreCase("elasticsearch")) {}
                    if (!propertyValueString.equalsIgnoreCase("jdbc")) {
                        hdStoreType = Type.STANDARD;
                    }
                }
                final Type typeProperty = Type.getTypeFromString(propertyName);
                if (typeProperty != null) {
                    hdStoreType = typeProperty;
                }
                else {
                    this.properties.add(property);
                }
            }
        }
        this.type = hdStoreType;
        this.properties.add(new Property(this.type.typeName(), null));
    }
    
    public HStorePersistencePolicy(final Type type, final List<Property> properties) {
        this.properties = new ArrayList<Property>(1);
        this.howOften = null;
        if (properties != null) {
            for (final Property property : properties) {
                final Type typeProperty = Type.getTypeFromString(property.name);
                if (typeProperty == null) {
                    this.properties.add(property);
                }
            }
        }
        this.type = type;
        this.properties.add(new Property(this.type.typeName(), null));
    }
    
    public HStorePersistencePolicy(final List<Property> properties) {
        this.properties = new ArrayList<Property>(1);
        if (properties == null || (properties != null && properties.isEmpty())) {
            throw new RuntimeException("Invalid properties");
        }
        if (properties != null) {
            this.properties.addAll(properties);
        }
        this.type = this.getType(properties);
        if (this.type == Type.UNKNOWN) {
            throw new RuntimeException("provide persistence type.");
        }
        if (this.type == Type.INTERVAL) {
            this.howOften = this.getInterval(properties);
        }
        else {
            this.howOften = null;
        }
        this.properties.add(new Property(this.type.typeName(), null));
    }
    
    public Type getType(final List<Property> properties) {
        Type type = Type.STANDARD;
        if (properties == null) {
            return type;
        }
        final String key = "storageProvider";
        for (final Property prop : properties) {
            if (prop.name.equalsIgnoreCase(key)) {
                final String val = (String)prop.value;
                if (val == null) {
                    break;
                }
                if (val.equalsIgnoreCase("jdbc")) {
                    type = Type.INTERVAL;
                    break;
                }
                if (val.equalsIgnoreCase("elasticsearch")) {
                    type = Type.STANDARD;
                    break;
                }
                if (val.equalsIgnoreCase("inmemory")) {
                    type = Type.IN_MEMORY;
                    break;
                }
                type = Type.UNKNOWN;
                break;
            }
        }
        return type;
    }
    
    public Interval getInterval(final List<Property> properties) {
        Interval interval = null;
        if (properties == null) {
            return interval;
        }
        final String key = "persistence_interval";
        String val = null;
        for (final Property prop : properties) {
            if (prop.name.equalsIgnoreCase(key)) {
                val = (String)prop.value;
                break;
            }
        }
        if (val == null || val.isEmpty()) {
            return interval;
        }
        val = val.trim().toLowerCase();
        if (!val.matches("(\\s*)([0-9]+)(\\s+)(sec.*|min.*)")) {
            return interval;
        }
        final int time = Integer.parseInt(val.split("\\s+")[0]);
        if (val.split("\\s+")[1].startsWith("sec")) {
            interval = new Interval(1000000L * time);
        }
        else if (val.split("\\s+")[1].startsWith("min")) {
            interval = new Interval(60000000L * time);
        }
        return interval;
    }
    
    static {
        HStorePersistencePolicy.logger = Logger.getLogger((Class)HStorePersistencePolicy.class);
    }
}
