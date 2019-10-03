package com.datasphere.kafka.schemaregistry;

import org.apache.http.*;
import org.apache.http.util.*;
import com.google.gson.*;
import org.apache.http.client.*;
import java.io.*;
import org.apache.avro.*;

public class SchemaRegistry
{
    private AbstractSchemaRegistryClient client;
    
    public SchemaRegistry(String host) {
        host = (host.endsWith("/") ? host : (host + "/"));
        try {
            if (this.getRegistryType(host) == SchemaRegistryConstants.RegistryType.CONFLUENT) {
                this.client = new ConfluentSchemaRegistryClient(host);
            }
            else {
                this.client = new HortonworksSchemaRegistryClient(host);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public SchemaRegistryConstants.RegistryType getRegistryType(final String host) throws Exception {
        final RestClient restClient = new RestClient(host);
        restClient.setResponseHandler((ResponseHandler<Object>)new ResponseHandler<Object>() {
            public Object handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                final GsonBuilder builder = new GsonBuilder();
                builder.excludeFieldsWithoutExposeAnnotation();
                final Gson gson = builder.create();
                final String responseMessage = EntityUtils.toString(response.getEntity());
                if (response.getStatusLine().getStatusCode() == 404 || response.getStatusLine().getStatusCode() == 500) {
                    final ErrorMessage errorMessage = (ErrorMessage)gson.fromJson(responseMessage, (Class)ErrorMessage.class);
                    return errorMessage;
                }
                throw new RuntimeException("Unexpected response from schema registry. Error is " + responseMessage);
            }
        });
        final Object obj = restClient.execute("api/v1", RestClient.Method.GET);
        SchemaRegistryConstants.RegistryType type = null;
        if (obj instanceof ErrorMessage && ((ErrorMessage)obj).getResponseMessage() != null) {
            type = SchemaRegistryConstants.RegistryType.HORTONWORKS;
        }
        else {
            type = SchemaRegistryConstants.RegistryType.CONFLUENT;
        }
        try {
            restClient.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not close Rest Client.", e);
        }
        return type;
    }
    
    public AbstractSchemaRegistryClient getClient() {
        return this.client;
    }
    
    public int registerSchema(final String subject, final Schema schema) throws Exception {
        return this.client.registerSchema(subject, schema);
    }
    
    public Schema getSchemaById(final int id) throws Exception {
        return this.client.getSchemaById(id);
    }
    
    public SchemaRegistryConstants.Compatibility getCompatibility(final String subject) throws Exception {
        return this.client.getCompatibility(subject);
    }
    
    public boolean isCompatibilitySet(final String subject) throws Exception {
        final SchemaRegistryConstants.Compatibility compatibility = this.getCompatibility(subject);
        return compatibility != null && compatibility == SchemaRegistryConstants.Compatibility.NONE;
    }
    
    public boolean updateCompatibility(final String subject, final SchemaRegistryConstants.Compatibility compatibility) throws Exception {
        return this.client.updateCompatibility(subject, compatibility);
    }
    
    public void close() throws Exception {
        this.client.close();
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}

