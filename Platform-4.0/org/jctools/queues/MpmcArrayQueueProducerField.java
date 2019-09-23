package org.jctools.queues;

import org.jctools.util.*;

abstract class MpmcArrayQueueProducerField<E> extends MpmcArrayQueueL1Pad<E>
{
    private static final long P_INDEX_OFFSET;
    private volatile long producerIndex;
    
    public MpmcArrayQueueProducerField(final int capacity) {
        super(capacity);
    }
    
    protected final long lvProducerIndex() {
        return this.producerIndex;
    }
    
    protected final boolean casProducerIndex(final long expect, final long newValue) {
        return UnsafeAccess.UNSAFE.compareAndSwapLong(this, MpmcArrayQueueProducerField.P_INDEX_OFFSET, expect, newValue);
    }
    
    static {
        try {
            P_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(MpmcArrayQueueProducerField.class.getDeclaredField("producerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
