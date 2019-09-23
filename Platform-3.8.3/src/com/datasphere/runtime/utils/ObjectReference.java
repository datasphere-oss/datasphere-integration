package com.datasphere.runtime.utils;

public class ObjectReference<T>
{
    private T value;
    
    public void set(final T val) {
        this.value = val;
    }
    
    public T get() {
        return this.value;
    }
}
