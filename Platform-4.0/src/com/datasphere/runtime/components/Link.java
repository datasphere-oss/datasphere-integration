package com.datasphere.runtime.components;

public class Link
{
    public final Subscriber subscriber;
    public final int linkID;
    
    public Link(final Subscriber sub, final int linkID) {
        this.subscriber = sub;
        this.linkID = linkID;
    }
    
    public Link(final Subscriber sub) {
        this(sub, -1);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Link)) {
            return false;
        }
        final Link l = (Link)obj;
        return this.subscriber.equals(l.subscriber) && this.linkID == l.linkID;
    }
    
    @Override
    public final int hashCode() {
        return this.subscriber.hashCode() ^ this.linkID;
    }
    
    @Override
    public String toString() {
        return this.subscriber + " over " + this.linkID;
    }
}
