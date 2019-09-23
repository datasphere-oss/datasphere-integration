package com.datasphere.hdstore.base;

import java.util.*;
import com.datasphere.hdstore.*;

public abstract class HDStoreBase<T extends HDStoreManagerBase> implements HDStore
{
    private final HDStoreManager manager;
    private final String name;
    private final Map<String, Object> properties;
    
    protected HDStoreBase(final HDStoreManager manager, final String name, final Map<String, Object> properties) {
        this.properties = new HashMap<String, Object>();
        this.manager = manager;
        this.name = name;
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }
    
    @Override
    public HDStoreManager getManager() {
        return this.manager;
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public Map<String, Object> getProperties() {
        return this.properties;
    }
}
