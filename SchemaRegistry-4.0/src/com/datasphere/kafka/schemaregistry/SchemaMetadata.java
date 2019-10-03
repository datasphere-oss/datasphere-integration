package com.datasphere.kafka.schemaregistry;

import com.google.gson.annotations.*;

public class SchemaMetadata
{
    @Expose(serialize = true)
    private String name;
    @Expose(serialize = true)
    private String type;
    @Expose(serialize = true)
    private String compatibility;
    @Expose(serialize = true)
    private String schemaGroup;
    
    public SchemaMetadata() {
        this.schemaGroup = "default";
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getType() {
        return this.type;
    }
    
    public void setType(final String type) {
        this.type = type;
    }
    
    public String getCompatibility() {
        return this.compatibility;
    }
    
    public void setCompatibility(final String compatibility) {
        this.compatibility = compatibility;
    }
}

