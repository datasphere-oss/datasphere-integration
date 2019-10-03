package com.datasphere.kafka.schemaregistry;

import org.apache.avro.*;
import com.google.gson.*;
import org.apache.http.*;
import org.apache.http.util.*;
import org.apache.http.client.*;
import java.io.*;

public abstract class AbstractSchemaRegistryClient implements ResponseHandler<Object>
{
    protected RestClient restClient;
    protected Gson gson;
    protected SchemaRegistryConstants.RegistryType registryType;
    protected String host;
    
    public AbstractSchemaRegistryClient() {
        final GsonBuilder builder = new GsonBuilder();
        builder.excludeFieldsWithoutExposeAnnotation();
        this.gson = builder.create();
    }
    
    public abstract SchemaRegistryConstants.RegistryType getRegistryType();
    
    public int registerSchema(final String subject, final Schema schema) throws Exception {
        String resourceUri = String.format("subjects/%s/versions", subject);
        if (this.registryType == SchemaRegistryConstants.RegistryType.HORTONWORKS) {
            resourceUri = "confluent/" + resourceUri;
        }
        final SchemaMessage requestMessage = new SchemaMessage();
        requestMessage.setSchema(schema.toString());
        try {
            final String jsonBody = this.gson.toJson((Object)requestMessage);
            final Object obj = this.restClient.execute(resourceUri, RestClient.Method.POST, jsonBody);
            if (obj instanceof ErrorMessage) {
                final ErrorMessage errorMessage = (ErrorMessage)obj;
                throw new RuntimeException(errorMessage.getErrorCode() + ", " + errorMessage.getMessage());
            }
            final SchemaMessage responseMessage = (SchemaMessage)obj;
            if (responseMessage.getId() == -1) {
                throw new RuntimeException("Unknown response from schema registry.");
            }
            return responseMessage.getId();
        }
        catch (JsonSyntaxException e) {
            throw new RuntimeException("Json formatting error possibly due to invalid schema. ", (Throwable)e);
        }
    }
    
    public Schema getSchemaById(final int id) throws Exception {
        String resourceUri = String.format("schemas/ids/%d", id);
        if (this.registryType == SchemaRegistryConstants.RegistryType.HORTONWORKS) {
            resourceUri = "confluent/" + resourceUri;
        }
        final Object obj = this.restClient.execute(resourceUri, RestClient.Method.GET);
        if (obj instanceof ErrorMessage) {
            final ErrorMessage errorMessage = (ErrorMessage)obj;
            throw new RuntimeException(errorMessage.getErrorCode() + ", " + errorMessage.getMessage());
        }
        final SchemaMessage responseMessage = (SchemaMessage)obj;
        if (responseMessage.getSchema() == null) {
            throw new RuntimeException("Unknown response from schema registry.");
        }
        return new Schema.Parser().parse(responseMessage.getSchema());
    }
    
    public abstract SchemaRegistryConstants.Compatibility getCompatibility(final String p0) throws Exception;
    
    public abstract boolean updateCompatibility(final String p0, final SchemaRegistryConstants.Compatibility p1) throws Exception;
    
    public Object handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
        final String responseMessage = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201) {
            final ErrorMessage errorMessage = (ErrorMessage)this.gson.fromJson(responseMessage, (Class)ErrorMessage.class);
            return errorMessage;
        }
        SchemaMessage schemaMessage = null;
        try {
            schemaMessage = (SchemaMessage)this.gson.fromJson(responseMessage, (Class)SchemaMessage.class);
            return schemaMessage;
        }
        catch (JsonSyntaxException e) {
            try {
                final Integer id = (Integer)this.gson.fromJson(responseMessage, (Class)Integer.class);
                return id;
            }
            catch (JsonSyntaxException ex) {
                throw new RuntimeException("Json parsing error in response from schema registry.", (Throwable)e);
            }
        }
    }
    
    public void close() {
        try {
            this.restClient.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to close REST client for schema registry.", e);
        }
    }
}

