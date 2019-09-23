package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

public abstract class Snapshot extends AbstractList<DARecord>
{
    final long vHead;
    
    public Snapshot(final long head) {
        this.vHead = head;
    }
    
    public abstract Iterator<HEntry> itemIterator();
    
    @Override
    public abstract Iterator<DARecord> iterator();
}
