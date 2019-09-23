package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class SlidingTimeAttrWindow extends SlidingTimeWindow
{
    private final CmpAttrs attrComparator;
    
    SlidingTimeAttrWindow(final BufferManager owner) {
        super(owner);
        this.attrComparator = this.getPolicy().getComparator();
    }
    
    @Override
    protected synchronized void update(final IBatch newEntries) {
        if (!newEntries.isEmpty()) {
            final Snapshot newsn = this.makeNewAttrSnapshot(newEntries, this.attrComparator);
            final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
            if (this.task == null) {
                this.task = this.schedule(this.time_interval);
            }
            this.notifyOnUpdate(newEntries, oldEntries);
        }
    }
}
