package com.datasphere.jmqmessaging;

import java.util.concurrent.*;

public abstract class Executable<O, V> implements Callable<V>
{
    private O on;
    
    public void setOn(final O on) {
        this.on = on;
    }
    
    public O on() {
        return this.on;
    }
    
    @Override
    public abstract V call();
    
    public abstract boolean hasResponse();
}
