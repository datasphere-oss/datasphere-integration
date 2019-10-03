package com.datasphere.kafka.schemaregistry;

import org.apache.http.client.*;

public class ConfluentSchemaRegistryClient extends AbstractSchemaRegistryClient
{
    @Override
    public SchemaRegistryConstants.RegistryType getRegistryType() {
        return this.registryType;
    }
    
    public ConfluentSchemaRegistryClient(final String host) {
        this.host = host;
        (this.restClient = new RestClient(host)).setResponseHandler((ResponseHandler<Object>)this);
        this.registryType = SchemaRegistryConstants.RegistryType.CONFLUENT;
    }
    
    @Override
    public SchemaRegistryConstants.Compatibility getCompatibility(final String subject) throws Exception {
        final String resourceUri = String.format("config/%s", subject);
        final Object obj = this.restClient.execute(resourceUri, RestClient.Method.GET);
        if (!(obj instanceof SchemaMessage)) {
            return null;
        }
        final SchemaMessage message = (SchemaMessage)obj;
        if (message.getCompatibilityLevel() == null) {
            throw new RuntimeException("Unknown response.");
        }
        final String compatibility = message.getCompatibilityLevel();
        if (compatibility.equals(SchemaRegistryConstants.Compatibility.ALL.name())) {
            return SchemaRegistryConstants.Compatibility.ALL;
        }
        if (compatibility.equals(SchemaRegistryConstants.Compatibility.FORWARD.name())) {
            return SchemaRegistryConstants.Compatibility.FORWARD;
        }
        if (compatibility.equals(SchemaRegistryConstants.Compatibility.BACKWARD.name())) {
            return SchemaRegistryConstants.Compatibility.BACKWARD;
        }
        return SchemaRegistryConstants.Compatibility.NONE;
    }
    
    @Override
    public boolean updateCompatibility(final String subject, final SchemaRegistryConstants.Compatibility compatibility) throws Exception {
        boolean flag = false;
        final String resourceUri = String.format("config/%s", subject);
        final SchemaMessage message = new SchemaMessage();
        message.setCompatibility(compatibility.name());
        final String jsonBody = this.gson.toJson((Object)message);
        final Object obj = this.restClient.execute(resourceUri, RestClient.Method.PUT, jsonBody);
        if (obj instanceof ErrorMessage) {
            final ErrorMessage errorMessage = (ErrorMessage)obj;
            throw new RuntimeException(errorMessage.getErrorCode() + ", " + errorMessage.getMessage());
        }
        flag = (obj instanceof SchemaMessage && ((SchemaMessage)obj).getCompatibility() != null && ((SchemaMessage)obj).getCompatibility().equals(compatibility.name()));
        return flag;
    }
}

