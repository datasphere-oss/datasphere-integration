package com.datasphere.appmanager;

import java.io.*;
import com.datasphere.uuid.*;

public class ChangeApplicationStateResponse implements Serializable
{
    private static final long serialVersionUID = -6801388395840353959L;
    private final UUID requestId;
    private RESULT result;
    private String exceptionMsg;
    
    private ChangeApplicationStateResponse() {
        this.requestId = null;
    }
    
    public ChangeApplicationStateResponse(final UUID requestId, final RESULT result, final String exceptionMsg) {
        this.requestId = requestId;
        this.result = result;
        this.exceptionMsg = exceptionMsg;
    }
    
    public UUID getRequestId() {
        return this.requestId;
    }
    
    public RESULT getResult() {
        return this.result;
    }
    
    public String getExceptionMsg() {
        return this.exceptionMsg;
    }
    
    @Override
    public String toString() {
        return "RequestId: " + this.requestId + " result: " + this.result + " exceptionMsg: " + this.exceptionMsg;
    }
    
    public enum RESULT
    {
        SUCCESS, 
        FAILURE;
    }
}
