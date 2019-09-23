package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class OneItemSnapshot extends Snapshot
{
    private final HEntry element;
    
    OneItemSnapshot(final HEntry e) {
        super(e.id);
        this.element = e;
    }
    
    @Override
    public Iterator<HEntry> itemIterator() {
        return Collections.singleton(this.element).iterator();
    }
    
    @Override
    public int size() {
        return 1;
    }
    
    @Override
    public Iterator<DARecord> iterator() {
        return Collections.singleton(this.element.data).iterator();
    }
    
    @Override
    public DARecord get(final int index) {
        if (index > 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        return this.element.data;
    }
    
    @Override
    public String toString() {
        return "{one item " + this.element + "}";
    }
}
