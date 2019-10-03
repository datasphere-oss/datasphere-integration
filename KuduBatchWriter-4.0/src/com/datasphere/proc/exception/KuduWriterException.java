package com.datasphere.proc.exception;

import com.datasphere.source.lib.exc.*;

public class KuduWriterException extends TransactionManagerException
{
    private Error errorCode;
    private static final long serialVersionUID = 6364524889611350696L;
    
    public KuduWriterException(final String msg) {
        super(msg, (Exception)null);
    }
    
    public KuduWriterException(final String msg, final Exception exp) {
        super(msg, exp);
    }
    
    public KuduWriterException(final Error errCode, final String msg, final Exception exp) {
        super(errCode.toString() + "  " + msg, exp);
        this.errorCode = errCode;
    }
    
    public KuduWriterException(final Error errCode, final String msg) {
        super(errCode.toString() + "  " + msg);
        this.errorCode = errCode;
        this.setErrorMessage(errCode.toString() + "  " + msg);
        this.setComponentName("KuduWriter");
        final StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
        this.setClassName(frame.getClassName());
        this.setMethodName(frame.getMethodName());
        this.setFileName(frame.getFileName());
        this.setLineNumber(frame.getLineNumber());
    }
    
    public KuduWriterException(final com.datalliance.common.errors.Error errCode, final String msg) {
        super(errCode.toString() + "  " + msg);
        this.setGenericErrorCode(errCode);
        this.setErrorMessage(errCode.toString() + "  " + msg);
        this.setComponentName("KuduWriter");
        final StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
        this.setClassName(frame.getClassName());
        this.setMethodName(frame.getMethodName());
        this.setFileName(frame.getFileName());
        this.setLineNumber(frame.getLineNumber());
    }
    
    public int getErrorCode() {
        if (this.errorCode == null) {
            return 0;
        }
        return this.errorCode.getType();
    }
    
    public void setErrorCode(final Error errCode) {
        this.errorCode = errCode;
    }
}
