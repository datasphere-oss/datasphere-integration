package com.datasphere.runtime.window;

import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;

class SimpleBufferPolicy implements HBufferPolicy
{
    HBuffer buffer;
    
    @Override
    public void updateBuffer(final ScalaWindow w, final Batch added, final long now) throws Exception {
        w.updateBuffer(this.buffer, added, now);
    }
    
    @Override
    public void initBuffer(final ScalaWindow w) {
        this.buffer = w.createBuffer(null);
    }
    
    @Override
    public void onJumpingTimer() throws Exception {
        this.buffer.removeAll();
    }
    
    @Override
    public Range createSnapshot() {
        return this.buffer.makeSnapshot();
    }
}
