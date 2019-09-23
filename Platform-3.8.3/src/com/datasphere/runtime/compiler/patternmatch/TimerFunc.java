package com.datasphere.runtime.compiler.patternmatch;

public enum TimerFunc
{
    CREATE("timer"), 
    STOP("stoptimer"), 
    WAIT("wait");
    
    private final String funcName;
    
    private TimerFunc(final String funcName) {
        this.funcName = funcName;
    }
    
    public String getFuncName() {
        return this.funcName;
    }
}
