package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class HBufferJumpingTimeCount extends HBufferJumping
{
    @Override
    void update(final Batch added, final long now) throws Exception {
    		Iterator iter = added.iterator();
    		while (iter.hasNext()) {
        		final DARecord e = (DARecord)iter.next();
            this.addEvent(e);
            if (this.countFull()) {
                this.doJump();
            }
        }
    }
    
    @Override
    void removeAll() throws Exception {
        this.doJump();
    }
}
