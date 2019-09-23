package com.datasphere.runtime.components;

public interface Restartable
{
    void start() throws Exception;
    
    void stop() throws Exception;
    
    boolean isRunning();
}
