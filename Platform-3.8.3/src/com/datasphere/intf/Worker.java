package com.datasphere.intf;

public interface Worker
{
    void startWorker();
    
    void stopWorker();
    
    WorkerType getType();
    
    void close() throws Exception;
    
    void setUri(final String p0);
    
    String getUri();
    
    public enum WorkerType
    {
        Q, 
        THREAD, 
        PROCESS;
    }
}
