package com.datasphere.errorhandling;


import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.errors.IError;

public class DatallRuntimeException extends RuntimeException
{
    private static Logger logger;
    private String componentName;
    private IError error;
    private String[] optionalParameters;
    
    public DatallRuntimeException(final IError error, final Throwable cause, final String componentName, final String... optionalParameters) {
        super(cause);
        this.componentName = "<COMPONENT-NAME-NOT-SET>";
        this.error = Error.NOT_ERROR;
        this.optionalParameters = null;
        if (error != null) {
            this.error = error;
        }
        if (componentName != null) {
            this.componentName = componentName;
        }
        DatallRuntimeException.logger.error(("Error in: " + componentName + " , error is: " + error), (Throwable)this);
        this.optionalParameters = optionalParameters;
    }
    
    @Override
    public String getMessage() {
        final StringBuilder sb = new StringBuilder("Error in: " + this.componentName + " , error is: " + this.error);
        if (this.getCause() != null) {
            sb.append(". Cause: ").append(this.getCause().toString());
        }
        for (final String params : this.optionalParameters) {
            sb.append(". Error details: " + params);
        }
        return sb.toString();
    }
    
    static {
        DatallRuntimeException.logger = Logger.getLogger(DatallRuntimeException.class.toString());
    }
}
