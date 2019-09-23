package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;

interface HBufferPolicy
{
    void updateBuffer(final ScalaWindow p0, final Batch p1, final long p2) throws Exception;
    
    void initBuffer(final ScalaWindow p0);
    
    void onJumpingTimer() throws Exception;
    
    Range createSnapshot();
}
