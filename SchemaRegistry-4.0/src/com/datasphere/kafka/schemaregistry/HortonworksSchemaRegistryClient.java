package com.datasphere.kafka.schemaregistry;

import org.apache.http.client.*;

public class HortonworksSchemaRegistryClient extends AbstractSchemaRegistryClient
{
    @Override
    public SchemaRegistryConstants.RegistryType getRegistryType() {
        return this.registryType;
    }
    
    public HortonworksSchemaRegistryClient(final String host) {
        this.host = host;
        (this.restClient = new RestClient(host + "api/v1")).setResponseHandler((ResponseHandler<Object>)this);
        this.registryType = SchemaRegistryConstants.RegistryType.HORTONWORKS;
    }
    
    @Override
    public SchemaRegistryConstants.Compatibility getCompatibility(final String subject) throws Exception {
        final String resourceUri = String.format("schemaregistry/schemas/%s", subject);
        final Object obj = this.restClient.execute(resourceUri, RestClient.Method.GET);
        if (obj instanceof ErrorMessage) {
            return null;
        }
        if (!(obj instanceof SchemaMessage)) {
            throw new RuntimeException("Unknown response.");
        }
        final SchemaMessage message = (SchemaMessage)obj;
        if (message.getSchemaMetadata() == null || message.getSchemaMetadata().getCompatibility() == null) {
            throw new RuntimeException("Unknown response.");
        }
        final String compatibility = message.getSchemaMetadata().getCompatibility();
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
        String resourceUri;
        if (this.getCompatibility(subject) == null) {
            resourceUri = "schemaregistry/schemas";
        }
        else {
            resourceUri = String.format("schemaregistry/schemas/%s", subject);
        }
        final SchemaMetadata metadata = new SchemaMetadata();
        metadata.setCompatibility(compatibility.name());
        metadata.setName(subject);
        metadata.setType("avro");
        final String jsonBody = this.gson.toJson((Object)metadata);
        final Object obj = this.restClient.execute(resourceUri, RestClient.Method.POST, jsonBody);
        if (obj instanceof ErrorMessage) {
            final ErrorMessage errorMessage = (ErrorMessage)obj;
            System.out.print("Error : " + errorMessage.getResponseMessage());
            throw new RuntimeException(errorMessage.getErrorCode() + ", " + errorMessage.getMessage());
        }
        if (obj instanceof Integer) {
            flag = true;
        }
        else if (obj instanceof SchemaMessage && ((SchemaMessage)obj).getSchemaMetadata() != null && ((SchemaMessage)obj).getSchemaMetadata().getCompatibility().equals(compatibility.name())) {
            flag = true;
        }
        return flag;
    }
}

