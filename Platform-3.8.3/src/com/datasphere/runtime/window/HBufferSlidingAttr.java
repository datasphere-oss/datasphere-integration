package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class HBufferSlidingAttr extends HBufferSliding
{
    @Override
    void update(final Batch added, final long now) throws Exception {
        final List<DARecord> removed = new ArrayList<DARecord>();
        Iterator iter = added.iterator();
        while (iter.hasNext()) {
        		DARecord ev = (DARecord)iter.next();
            try {
                while (true) {
                    final DARecord firstInBuffer = this.getFirstEvent();
                    if (this.inRange(firstInBuffer, ev)) {
                        break;
                    }
                    this.removeEvent(removed);
                }
            }
            catch (NoSuchElementException ex) {}
            this.addEvent(ev);
        }
        this.publish(added, removed);
    }
}
