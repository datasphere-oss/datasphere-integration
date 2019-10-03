package com.datasphere.kafka.schemaregistry;

import com.google.gson.annotations.*;

public class ErrorMessage
{
    @SerializedName("error_code")
    @Expose(deserialize = true)
    private int errorCode;
    @Expose(deserialize = true)
    private String message;
    @Expose(deserialize = true)
    private String responseMessage;
    
    public int getErrorCode() {
        return this.errorCode;
    }
    
    public void setErrorCode(final int errorCode) {
        this.errorCode = errorCode;
    }
    
    public String getMessage() {
        return this.message;
    }
    
    public void setMessage(final String message) {
        this.message = message;
    }
    
    public String getResponseMessage() {
        return this.responseMessage;
    }
    
    public void setResponseMessage(final String responseMessage) {
        this.responseMessage = responseMessage;
    }
}
