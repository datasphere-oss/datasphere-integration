package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class SlidingCountWindow extends BufWindow
{
    private final int row_count;
    
    SlidingCountWindow(final BufferManager owner) {
        super(owner);
        this.row_count = this.getPolicy().getRowCount();
    }
    
    @Override
    protected void update(final IBatch newEntries) {
        final Snapshot newsn = this.makeSnapshot(this.snapshot, this.row_count);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        this.notifyOnUpdate(newEntries, oldEntries);
    }
    
    @Override
    protected void flush() {
        this.snapshot = this.makeEmptySnapshot();
    }
}
