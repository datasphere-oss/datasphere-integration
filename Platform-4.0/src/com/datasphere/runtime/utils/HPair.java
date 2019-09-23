package com.datasphere.runtime.utils;

import java.io.*;

public class HPair<A, B> implements Serializable
{
    private static final long serialVersionUID = -799825210823045963L;
    public A first;
    public B second;
    
    private HPair(final A first, final B second) {
        this.first = first;
        this.second = second;
    }
    
    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof HPair)) {
            return false;
        }
        final HPair<A, B> other = (HPair<A, B>)o;
        if (this.first == null) {
            if (other.first != null) {
                return false;
            }
        }
        else if (!this.first.equals(other.first)) {
            return false;
        }
        if ((this.second != null) ? this.second.equals(other.second) : (other.second == null)) {
            return true;
        }
        return false;
    }
    
    @Override
    public final int hashCode() {
        return ((this.first == null) ? 0 : this.first.hashCode()) ^ ((this.second == null) ? 0 : this.second.hashCode());
    }
    
    @Override
    public final String toString() {
        return "(" + this.first + "," + this.second + ")";
    }
    
    public static <A, B> HPair<A, B> make(final A a, final B b) {
        return new HPair<A, B>(a, b);
    }
    
    public void setAll(final A first, final B second) {
        this.first = first;
        this.second = second;
    }
}
