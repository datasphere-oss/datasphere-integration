package com.datasphere.source.intf;

public interface Restartable
{
    boolean isRestartable();
    
    boolean restart();
    
    void setPosition(final Object p0);
    
    Object getLastPosition();
}
