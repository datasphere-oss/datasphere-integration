package com.datasphere.databasewriter;

import com.datasphere.proc.events.*;

public class ExceptionData
{
    public Exception exception;
    public HDEvent eventCaused;
    public boolean ignored;
    
    public ExceptionData(final Exception exp, final HDEvent event) {
        this.exception = exp;
        this.eventCaused = event;
        this.ignored = false;
    }
}
