package com.datasphere.persistence;

public class ElasticSearchSchemaException extends Exception
{
    private String message;
    
    public ElasticSearchSchemaException(final String message) {
        super(message);
    }
}
