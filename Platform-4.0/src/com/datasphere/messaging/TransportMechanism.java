package com.datasphere.messaging;

public enum TransportMechanism
{
    INPROC(1), 
    IPC(2), 
    TCP(3);
    
    private final int val;
    
    private TransportMechanism(final int val) {
        this.val = val;
    }
}
