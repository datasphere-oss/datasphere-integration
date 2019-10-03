package com.datasphere.kafka.schemaregistry;

import com.google.gson.annotations.*;

public class SchemaMessage
{
    @Expose(serialize = false)
    private String subject;
    @Expose(serialize = false)
    private int id;
    @Expose(serialize = false)
    private int version;
    @Expose(serialize = true)
    private String schema;
    @Expose(serialize = true)
    private String compatibility;
    @Expose(deserialize = true)
    @SerializedName("schemaMetadata")
    private SchemaMetadata metadata;
    @Expose(deserialize = true)
    @SerializedName("compatibilityLevel")
    private String compatibilityLevel;
    
    public SchemaMessage() {
        this.id = -1;
        this.version = -1;
    }
    
    public String getCompatibility() {
        return this.compatibility;
    }
    
    public void setCompatibility(final String compatibility) {
        this.compatibility = compatibility;
    }
    
    public String getSubject() {
        return this.subject;
    }
    
    public void setSubject(final String subject) {
        this.subject = subject;
    }
    
    public int getId() {
        return this.id;
    }
    
    public void setId(final int id) {
        this.id = id;
    }
    
    public int getVersion() {
        return this.version;
    }
    
    public void setVersion(final int version) {
        this.version = version;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String schema) {
        this.schema = schema;
    }
    
    public SchemaMetadata getSchemaMetadata() {
        return this.metadata;
    }
    
    public void setSchemaMetadata(final SchemaMetadata metadata) {
        this.metadata = metadata;
    }
    
    public String getCompatibilityLevel() {
        return this.compatibilityLevel;
    }
    
    public void setCompatibilityLevel(final String compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }
}

