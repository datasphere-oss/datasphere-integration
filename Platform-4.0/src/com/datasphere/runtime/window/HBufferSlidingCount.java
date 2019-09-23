package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class HBufferSlidingCount extends HBufferSliding
{
    @Override
    void update(final Batch added, final long now) throws Exception {
        this.addEvents((IBatch)added);
        final List<DARecord> removed = new ArrayList<DARecord>();
        while (this.countOverflow()) {
            this.removeEvent(removed);
        }
        this.publish(added, removed);
    }
}
