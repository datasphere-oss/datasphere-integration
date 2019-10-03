package com.datasphere.kafka.schemaregistry;

public final class SchemaRegistryConstants
{
    public static final String CONFLUENT_REGISTER_SCHEMA_PATH = "subjects/%s/versions";
    public static final String CONFLUENT_GET_SCHEMA_PATH = "schemas/ids/%d";
    public static final String CONFLUENT_UPDATE_COMPATIBILITY_PATH = "config/%s";
    public static final String HORTONWORKS_REGISTER_SCHEMA_PATH = "confluent/subjects/%s/versions";
    public static final String HORTONWORKS_GET_SCHEMA_PATH = "confluent/schemas/ids/%d";
    public static final String HORTONWORKS_GET_COMPATIBILITY_PATH = "schemaregistry/schemas/%s";
    public static final String HORTONWORKS_UPDATE_COMPATIBILITY_PATH = "schemaregistry/schemas";
    
    public enum RegistryType
    {
        CONFLUENT, 
        HORTONWORKS;
    }
    
    public enum Compatibility
    {
        ALL, 
        NONE, 
        BACKWARD, 
        FORWARD;
    }
}
