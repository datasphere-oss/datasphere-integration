package com.datasphere.runtime;

import java.io.*;

public class Pair<A, B> implements Serializable
{
    private static final long serialVersionUID = -7998252108293045963L;
    public A first;
    public B second;
    
    public Pair() {
        this.first = null;
        this.second = null;
    }
    
    public Pair(final A first, final B second) {
        this.first = first;
        this.second = second;
    }
    
    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        final Pair<A, B> other = (Pair<A, B>)o;
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
    
    public static <A, B> Pair<A, B> make(final A a, final B b) {
        return new Pair<A, B>(a, b);
    }
}
