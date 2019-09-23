package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class SlidingTimeCountWindow extends SlidingTimeWindow
{
    private final int row_count;
    
    SlidingTimeCountWindow(final BufferManager owner) {
        super(owner);
        this.row_count = this.getPolicy().getRowCount();
    }
    
    @Override
    protected synchronized void update(final IBatch newEntries) {
        final Snapshot newsn = this.makeSnapshot(this.snapshot, this.row_count);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        if (this.task == null) {
            this.task = this.schedule(this.time_interval);
        }
        this.notifyOnUpdate(newEntries, oldEntries);
    }
}
