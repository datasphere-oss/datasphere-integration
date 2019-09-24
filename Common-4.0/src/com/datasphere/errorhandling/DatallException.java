package com.datasphere.errorhandling;


import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.errors.IError;

public class DatallException extends Exception
{
    private static Logger logger;
    private String componentName;
    private IError error;
    private String[] optionalParameters;
    
    public DatallException(final IError error, final Throwable cause, final String componentName, final String... optionalParameters) {
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
        DatallException.logger.error("Error in: " + componentName + " , error is: " + error, (Throwable)this);
        this.optionalParameters = optionalParameters;
    }
    
    static {
        DatallException.logger = Logger.getLogger(DatallException.class.toString());
    }
}
