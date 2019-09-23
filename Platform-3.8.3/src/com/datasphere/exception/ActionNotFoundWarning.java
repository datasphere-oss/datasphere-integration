package com.datasphere.exception;

public class ActionNotFoundWarning extends Warning
{
    public ActionNotFoundWarning() {
    }
    
    public ActionNotFoundWarning(final String s) {
        super(s);
    }
    
    public ActionNotFoundWarning(final String s, final Throwable t) {
        super(s, t);
    }
    
    public ActionNotFoundWarning(final Throwable cause) {
        super(cause);
    }
    
    public ActionNotFoundWarning(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
