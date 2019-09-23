package com.datasphere.runtime.components;

import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

public class WindowIndex
{
    public Iterator<DARecord> createIterator(final RecordKey key) {
        return Collections.emptyIterator();
    }
    
    public void update(final RecordKey key, final DARecord event, final boolean doadd) {
    }
}
