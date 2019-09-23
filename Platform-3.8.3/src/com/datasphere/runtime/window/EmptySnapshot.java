package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class EmptySnapshot extends Snapshot
{
    EmptySnapshot(final long head) {
        super(head);
    }
    
    @Override
    public Iterator<HEntry> itemIterator() {
        return Collections.emptyIterator();
    }
    
    @Override
    public int size() {
        return 0;
    }
    
    @Override
    public DARecord get(final int index) {
        throw new IndexOutOfBoundsException("Index: " + index);
    }
    
    @Override
    public Iterator<DARecord> iterator() {
        return Collections.emptyIterator();
    }
    
    @Override
    public String toString() {
        return "{empty snapshot}";
    }
}
