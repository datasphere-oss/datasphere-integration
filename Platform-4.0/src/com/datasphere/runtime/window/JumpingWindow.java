package com.datasphere.runtime.window;

abstract class JumpingWindow extends BufWindow
{
    Long nextHead;
    
    JumpingWindow(final BufferManager owner) {
        super(owner);
        this.nextHead = null;
    }
}
