package com.datasphere.kafkamessaging;

public class StreamPersistencePolicy
{
    public static final String defaultPolicy = "default";
    private final String fullyQualifiedNameOfPropertyset;
    
    public StreamPersistencePolicy(final String fullyQualifiedNameOfPropertyset) {
        if (fullyQualifiedNameOfPropertyset == null) {
            this.fullyQualifiedNameOfPropertyset = null;
        }
        else if (fullyQualifiedNameOfPropertyset == "default") {
            this.fullyQualifiedNameOfPropertyset = "Global".concat(".").concat("DefaultKafkaProperties");
        }
        else {
            this.fullyQualifiedNameOfPropertyset = fullyQualifiedNameOfPropertyset;
        }
    }
    
    public String getFullyQualifiedNameOfPropertyset() {
        return this.fullyQualifiedNameOfPropertyset;
    }
}
