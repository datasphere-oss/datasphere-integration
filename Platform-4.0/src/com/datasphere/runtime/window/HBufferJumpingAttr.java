package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class HBufferJumpingAttr extends HBufferJumping
{
    @Override
    void update(final Batch added, final long now) throws Exception {
    		Iterator iter = added.iterator();
        while (iter.hasNext()) {
        	final DARecord ev = (DARecord)iter.next();
            try {
                final DARecord firstInBuffer = this.getFirstEvent();
                if (!this.inRange(firstInBuffer, ev)) {
                    this.doJump();
                }
            }
            catch (NoSuchElementException ex) {}
            this.addEvent(ev);
        }
    }
}
