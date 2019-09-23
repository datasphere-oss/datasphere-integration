package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class SlidingAttributeWindow extends BufWindow
{
    private final CmpAttrs attrComparator;
    
    SlidingAttributeWindow(final BufferManager owner) {
        super(owner);
        this.attrComparator = this.getPolicy().getComparator();
    }
    
    @Override
    protected void update(final IBatch newEntries) {
        if (!newEntries.isEmpty()) {
            final Snapshot newsn = this.makeNewAttrSnapshot(newEntries, this.attrComparator);
            final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
            this.notifyOnUpdate(newEntries, oldEntries);
        }
    }
    
    @Override
    protected void flush() {
        this.snapshot = this.makeEmptySnapshot();
    }
}
