package com.datasphere.runtime;

public class LagInfo
{
    final Long timeStamp;
    final String fromComponent;
    String toComponent;
    
    public LagInfo(final Long timeStamp, final String fromComponent, final String toComponent) {
        this.timeStamp = timeStamp;
        this.fromComponent = fromComponent;
        this.toComponent = toComponent;
    }
    
    @Override
    public String toString() {
        return "fromComponent " + this.fromComponent + " toComponent " + this.toComponent + " at timeStamp " + this.timeStamp + "\n";
    }
}
