package com.datasphere.kafka;

import java.io.*;

public abstract class Offset<T> implements Comparable<Offset>, Serializable
{
    @Override
    public abstract int compareTo(final Offset p0);
    
    public abstract boolean isValid();
    
    public abstract T getOffset();
    
    public abstract void setOffset(final T p0);
    
    public abstract String getName();
}
