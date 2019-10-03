package com.datasphere.kafka.schemaregistry;

import org.apache.http.client.*;
import org.apache.http.impl.conn.*;
import org.apache.http.impl.client.*;
import org.apache.http.conn.*;
import org.apache.http.entity.*;
import org.apache.http.client.methods.*;
import java.io.*;
import org.apache.http.protocol.*;
import org.apache.http.*;

public class RestClient
{
    protected CloseableHttpClient httpClient;
    protected ResponseHandler<Object> handler;
    protected String baseUri;
    private static final int MAX_CONNECTIONS = 100;
    private static final int MAX_RETRIES = 3;
    
    public RestClient(final String baseUri) {
        final PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(100);
        this.httpClient = HttpClients.custom().setConnectionManager((HttpClientConnectionManager)cm).setRetryHandler((exception, executionCount, context) -> executionCount <= 3 && exception instanceof NoHttpResponseException).build();
        this.setBaseUri(baseUri);
    }
    
    public void setBaseUri(String baseUri) {
        if (!baseUri.endsWith("/")) {
            baseUri += "/";
        }
        this.baseUri = baseUri;
    }
    
    public String getBaseUri() {
        return this.baseUri;
    }
    
    public void setResponseHandler(final ResponseHandler<Object> handler) {
        this.handler = handler;
    }
    
    public String buildResourceUri(final String resourcePath) {
        return this.baseUri + resourcePath;
    }
    
    public Object execute(final String resourcePath, final Method method, final String body) {
        final String resourceUri = this.buildResourceUri(resourcePath);
        Object obj = null;
        try {
            switch (method) {
                case GET: {
                    obj = this.doGet(resourceUri);
                    break;
                }
                case PUT: {
                    obj = this.doPut(resourceUri, body);
                    break;
                }
                case POST: {
                    obj = this.doPost(resourceUri, body);
                    break;
                }
                case DELETE: {
                    obj = this.doDelete(resourceUri);
                    break;
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Schema registry server error. Please check schema registry url.", e);
        }
        return obj;
    }
    
    public Object execute(final String resourcePath, final Method method) {
        return this.execute(resourcePath, method, null);
    }
    
    public Object doGet(final String resourceUri) throws Exception {
        final HttpGet request = new HttpGet(resourceUri);
        return this.httpClient.execute((HttpUriRequest)request, (ResponseHandler)this.handler);
    }
    
    public Object doPut(final String resourceUri, final String body) throws Exception {
        final HttpPut request = new HttpPut(resourceUri);
        final StringEntity entity = new StringEntity(body);
        entity.setContentType("application/vnd.schemaregistry.v1+json");
        request.setEntity((HttpEntity)entity);
        return this.httpClient.execute((HttpUriRequest)request, (ResponseHandler)this.handler);
    }
    
    public Object doPost(final String resourceUri, final String body) throws Exception {
        final HttpPost request = new HttpPost(resourceUri);
        final StringEntity entity = new StringEntity(body);
        entity.setContentType("application/vnd.schemaregistry.v1+json");
        request.setEntity((HttpEntity)entity);
        return this.httpClient.execute((HttpUriRequest)request, (ResponseHandler)this.handler);
    }
    
    public Object doDelete(final String resourceUri) throws Exception {
        final HttpDelete request = new HttpDelete(resourceUri);
        return this.httpClient.execute((HttpUriRequest)request, (ResponseHandler)this.handler);
    }
    
    public void close() throws Exception {
        if (this.httpClient != null) {
            this.httpClient.close();
            this.httpClient = null;
        }
    }
    
    public enum Method
    {
        GET, 
        PUT, 
        POST, 
        DELETE;
    }
}

