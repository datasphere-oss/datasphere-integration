package com.datasphere.common.exc;

import java.io.Serializable;
import java.text.DecimalFormat;

import com.datasphere.common.errors.Error;

public class AdapterException extends Exception implements Serializable
{
    private static final long serialVersionUID = -3162936038255700823L;
    private static final String DASCCODE = "Data Alliance Source Component";
    private Error error;
    DecimalFormat df;
    String errorMessage;
    String componentName;
    
    public AdapterException() {
        this(Error.GENERIC_EXCEPTION);
    }
    
    public AdapterException(final String message) {
        this(Error.GENERIC_EXCEPTION, message);
    }
    
    public AdapterException(final Error error) {
        this.df = new DecimalFormat("1000");
        this.error = error;
        this.errorMessage = "DA-" + error.toString();
    }
    
    public AdapterException(final Error error, final String message) {
        super(message);
        this.df = new DecimalFormat("1000");
        this.error = error;
        if (message != null) {
            this.errorMessage = "DA-" + error.toString() + ". Cause: " + message;
        }
        else {
            this.errorMessage = "DA-" + error.toString();
        }
    }
    
    public AdapterException(final Error error, final Throwable cause) {
        super(cause);
        this.df = new DecimalFormat("1000");
        this.error = error;
        if (cause.getMessage() != null) {
            this.errorMessage = "DA-" + error.toString() + ". Cause: " + cause.getMessage();
        }
        else {
            this.errorMessage = "DA-" + error.toString();
        }
    }
    
    public AdapterException(final Error error, final String message, final Throwable cause) {
        super(message, cause);
        this.df = new DecimalFormat("1000");
        this.error = error;
    }
    
    public AdapterException(final String message, final Throwable cause) {
        super(message, cause);
        this.df = new DecimalFormat("1000");
        if (cause.getMessage() != null) {
            this.errorMessage = message + ". Cause: " + cause.getMessage();
        }
        else if (cause.getCause() != null && cause.getCause().getMessage() != null) {
            this.errorMessage = message + ". Cause: " + cause.getCause().getMessage();
        }
        else {
            this.errorMessage = message;
        }
    }
    
    public String getCode() {
        return "HD Source Component";
    }
    
    public String getErrorMessage() {
        return this.errorMessage;
    }
    
    @Override
    public String getMessage() {
        return this.errorMessage;
    }
    
    public Error getType() {
        return this.error;
    }
}
