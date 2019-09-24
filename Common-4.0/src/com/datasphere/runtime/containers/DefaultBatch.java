package com.datasphere.runtime.containers;

import java.util.*;

public class DefaultBatch implements IBatch
{
    private final DARecord containedEvent;
    public static final IBatch EMPTY_BATCH;
    
    public DefaultBatch(final DARecord xevent) {
        this.containedEvent = xevent;
    }
    
    @Override
    public Iterator<DARecord> iterator() {
        return new Iterator<DARecord>() {
            private boolean hasNext = true;
            
            @Override
            public boolean hasNext() {
                return this.hasNext;
            }
            
            @Override
            public DARecord next() {
                if (this.hasNext) {
                    this.hasNext = false;
                    return DefaultBatch.this.containedEvent;
                }
                throw new NoSuchElementException();
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    @Override
    public int size() {
        return 1;
    }
    
    @Override
    public boolean isEmpty() {
        return false;
    }
    
    @Override
    public DARecord first() {
        return this.containedEvent;
    }
    
    @Override
    public DARecord last() {
        return this.containedEvent;
    }
    
    static {
        EMPTY_BATCH = new IBatch() {
            @Override
            public int size() {
                return 0;
            }
            
            @Override
            public DARecord last() {
                return null;
            }
            
            @Override
            public Iterator<DARecord> iterator() {
                return Collections.emptyIterator();
            }
            
            @Override
            public boolean isEmpty() {
                return true;
            }
            
            @Override
            public DARecord first() {
                return null;
            }
        };
    }
}
