package com.datasphere.hdstore.exceptions;

import com.fasterxml.jackson.databind.*;
import com.github.fge.jsonschema.core.report.*;
import java.util.*;

public class QuerySchemaException extends HDStoreException
{
    private static final long serialVersionUID = -6132114902661917869L;
    private static final String MSG_EXC_MESSAGE = "HDStore query schema violation: %s";
    public final String queryString;
    public final List<String> errorMessages;
    
    public QuerySchemaException(final JsonNode queryJson, final Iterable<ProcessingMessage> report) {
        super(String.format("HDStore query schema violation: %s", getErrorMessages(report).get(0)));
        this.errorMessages = new ArrayList<String>();
        this.queryString = queryJson.toString();
        this.errorMessages.addAll(getErrorMessages(report));
    }
    
    public QuerySchemaException(final JsonNode queryJson, final String errorMessage) {
        super(String.format("HDStore query schema violation: %s", errorMessage));
        this.errorMessages = new ArrayList<String>();
        this.queryString = queryJson.toString();
        this.errorMessages.add(errorMessage);
    }
    
    private static List<String> getErrorMessages(final Iterable<ProcessingMessage> processingReport) {
        final List<String> result = new ArrayList<String>();
        for (final ProcessingMessage message : processingReport) {
            final String messageText = message.getMessage();
            if (!messageText.contains("will be ignored")) {
                result.add(messageText);
            }
        }
        return result;
    }
}
