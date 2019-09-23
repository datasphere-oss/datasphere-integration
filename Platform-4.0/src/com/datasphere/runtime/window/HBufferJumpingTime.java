package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;

class HBufferJumpingTime extends HBufferJumping
{
    @Override
    void update(final Batch added, final long now) throws Exception {
        this.addEvents((IBatch)added);
    }
    
    @Override
    void removeAll() throws Exception {
        this.doJump();
    }
}
