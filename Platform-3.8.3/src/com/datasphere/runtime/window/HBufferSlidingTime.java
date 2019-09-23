package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class HBufferSlidingTime extends HBufferSliding
{
    LinkedList<TimeIndexEntry> index;
    
    HBufferSlidingTime() {
        this.index = new LinkedList<TimeIndexEntry>();
    }
    
    @Override
    void update(final Batch added, final long now) throws Exception {
        this.addEvents((IBatch)added);
        this.index.add(new TimeIndexEntry(added.size(), now));
        this.publish(added, Collections.emptyList());
    }
    
    public void removeTill(final long now) throws Exception {
        final List<DARecord> removed = new ArrayList<DARecord>();
        while (!this.index.isEmpty()) {
            TimeIndexEntry e = this.index.peek();
            if (this.notExpiredYet(e.createdTimestamp, now)) {
                break;
            }
            e = this.index.remove();
            for (int i = 0; i < e.count; ++i) {
                this.removeEvent(removed);
            }
        }
        this.publish(Collections.emptyList(), removed);
    }
}
