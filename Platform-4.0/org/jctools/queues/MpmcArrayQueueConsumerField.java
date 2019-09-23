package org.jctools.queues;

import org.jctools.util.*;

abstract class MpmcArrayQueueConsumerField<E> extends MpmcArrayQueueL2Pad<E>
{
    private static final long C_INDEX_OFFSET;
    private volatile long consumerIndex;
    
    public MpmcArrayQueueConsumerField(final int capacity) {
        super(capacity);
    }
    
    protected final long lvConsumerIndex() {
        return this.consumerIndex;
    }
    
    protected final boolean casConsumerIndex(final long expect, final long newValue) {
        return UnsafeAccess.UNSAFE.compareAndSwapLong(this, MpmcArrayQueueConsumerField.C_INDEX_OFFSET, expect, newValue);
    }
    
    static {
        try {
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(MpmcArrayQueueConsumerField.class.getDeclaredField("consumerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
