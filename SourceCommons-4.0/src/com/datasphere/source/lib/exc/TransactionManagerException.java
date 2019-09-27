package com.datasphere.source.lib.exc;

import com.datasphere.common.errors.Error;

public abstract class TransactionManagerException extends Exception
{
    String className;
    String methodName;
    String fileName;
    int lineNumber;
    String componentName;
    String errorMessage;
    Error genericErrorCode;
    private static final long serialVersionUID = -2685486125213211132L;
    
    public TransactionManagerException(final String msg) {
        super(msg);
        final StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
        this.setClassName(frame.getClassName());
        this.setMethodName(frame.getMethodName());
        this.setFileName(frame.getFileName());
        this.setLineNumber(frame.getLineNumber());
    }
    
    public TransactionManagerException(final String msg, final Exception e) {
        super(msg, e);
        final StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
        this.setClassName(frame.getClassName());
        this.setMethodName(frame.getMethodName());
        this.setFileName(frame.getFileName());
        this.setLineNumber(frame.getLineNumber());
    }
    
    public String getClassName() {
        return this.className;
    }
    
    public void setClassName(final String className) {
        this.className = className;
    }
    
    public String getMethodName() {
        return this.methodName;
    }
    
    public void setMethodName(final String methodName) {
        this.methodName = methodName;
    }
    
    public String getFileName() {
        return this.fileName;
    }
    
    public void setFileName(final String fileName) {
        this.fileName = fileName;
    }
    
    public int getLineNumber() {
        return this.lineNumber;
    }
    
    public void setLineNumber(final int lineNumber) {
        this.lineNumber = lineNumber;
    }
    
    public abstract int getErrorCode();
    
    public void setGenericErrorCode(final Error genErrorCode) {
        this.genericErrorCode = genErrorCode;
    }
    
    public void setErrorMessage(final String errMessage) {
        this.errorMessage = errMessage;
    }
    
    public String getErrorMessage() {
        return this.errorMessage;
    }
    
    public String getComponentName() {
        return this.componentName;
    }
    
    public void setComponentName(final String componentName) {
        this.componentName = componentName;
    }
}
