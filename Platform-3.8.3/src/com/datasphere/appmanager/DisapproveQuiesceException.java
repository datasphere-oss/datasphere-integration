package com.datasphere.appmanager;

public class DisapproveQuiesceException extends Exception
{
    private final String componentName;
    
    public DisapproveQuiesceException(final String message, final String componentName) {
        super(message);
        this.componentName = componentName;
    }
    
    public String getComponentName() {
        return this.componentName;
    }
}
