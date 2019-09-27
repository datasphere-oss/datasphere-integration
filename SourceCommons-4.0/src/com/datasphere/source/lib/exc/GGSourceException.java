package com.datasphere.source.lib.exc;

import java.io.*;
import java.text.*;

public class GGSourceException extends Exception implements Serializable
{
    private static final long serialVersionUID = 178882191744299616L;
    private static final String GGCODE = "GG Source: ";
    private Type type;
    DecimalFormat df;
    
    public GGSourceException() {
        this(Type.GENERIC_EXCEPTION);
    }
    
    public GGSourceException(final String message) {
        this(Type.GENERIC_EXCEPTION, message);
    }
    
    public GGSourceException(final Type type) {
        this.df = new DecimalFormat("0000");
        this.type = type;
    }
    
    public GGSourceException(final Type type, final String message) {
        super(message);
        this.df = new DecimalFormat("0000");
        this.type = type;
    }
    
    public GGSourceException(final Type type, final Throwable cause) {
        super(cause);
        this.df = new DecimalFormat("0000");
        this.type = type;
    }
    
    public GGSourceException(final Type type, final String message, final Throwable cause) {
        super(message, cause);
        this.df = new DecimalFormat("0000");
        this.type = type;
    }
    
    public String getCode() {
        return "GG Source: " + this.type;
    }
    
    @Override
    public String getMessage() {
        if (super.getMessage() != null) {
            return this.type.text + "\n" + super.getMessage();
        }
        return this.type.text;
    }
    
    public Type getType() {
        return this.type;
    }
    
    @Override
    public String toString() {
        return "Exception in GoldenGate Trail Source: " + this.getCode() + "\n" + this.getMessage() + "\n";
    }
    
    public enum Type
    {
        GENERIC_EXCEPTION(100, "Unexpected Exception"), 
        NOT_IMPLEMENTED(101, "Method Not Implemented"), 
        END_OF_TRAIL(102, "End of Trail"), 
        IO_ISSUE(103, "Read Error"), 
        INVALID_RECORD(104, "Read Position Invalid"), 
        CHECKPOINT_ERROR(105, "Checkpointing Error");
        
        int type;
        String text;
        
        private Type(final int type, final String text) {
            this.type = type;
            this.text = text;
        }
        
        @Override
        public String toString() {
            return this.type + ":" + this.text;
        }
    }
}
