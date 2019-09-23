package org.jctools.queues;

import org.jctools.util.*;

abstract class SpmcArrayQueueConsumerField<E> extends SpmcArrayQueueL2Pad<E>
{
    protected static final long C_INDEX_OFFSET;
    private volatile long consumerIndex;
    
    public SpmcArrayQueueConsumerField(final int capacity) {
        super(capacity);
    }
    
    protected final long lvConsumerIndex() {
        return this.consumerIndex;
    }
    
    protected final boolean casHead(final long expect, final long newValue) {
        return UnsafeAccess.UNSAFE.compareAndSwapLong(this, SpmcArrayQueueConsumerField.C_INDEX_OFFSET, expect, newValue);
    }
    
    static {
        try {
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(SpmcArrayQueueConsumerField.class.getDeclaredField("consumerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
