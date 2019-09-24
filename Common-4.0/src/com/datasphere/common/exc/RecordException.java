package com.datasphere.common.exc;

import java.io.*;

import com.datasphere.common.constants.*;

public class RecordException extends Exception implements Serializable
{
    private static final long serialVersionUID = 6697291494513600574L;
    private Type exceptionType;
    private String errMsg;
    
    public RecordException(final String msg, final Type type) {
        super(msg);
        this.exceptionType = type;
        this.errMsg = msg;
    }
    
    public RecordException(final String msg, final Throwable cause, final Type type) {
        super(msg, cause);
        this.exceptionType = type;
    }
    
    public RecordException(final Type type) {
        this.exceptionType = type;
    }
    
    public Type type() {
        return this.exceptionType;
    }
    
    public void type(final Type t) {
        this.exceptionType = t;
    }
    
    public void errMsg(final String msg) {
        this.errMsg = msg;
    }
    
    public String errMsg() {
        return this.errMsg;
    }
    
    @Override
    public String getMessage() {
        if (this.errMsg != null) {
            return this.errMsg;
        }
        return this.exceptionType.type();
    }
    
    public Constant.recordstatus returnStatus() {
        switch (this.exceptionType) {
            case ERROR: {
                return Constant.recordstatus.ERROR_RECORD;
            }
            case INVALID_RECORD: {
                return Constant.recordstatus.INVALID_RECORD;
            }
            case NO_RECORD: {
                return Constant.recordstatus.NO_RECORD;
            }
            default: {
                return Constant.recordstatus.ERROR_RECORD;
            }
        }
    }
    
    public enum Type
    {
        NO_RECORD("No Record"), 
        INVALID_RECORD("Invalid Record"), 
        ERROR("Error Record"), 
        END_OF_DATASOURCE("End of Data source");
        
        private String type;
        
        private Type(final String sType) {
            this.type = sType;
        }
        
        public String type() {
            return this.type;
        }
    }
}
