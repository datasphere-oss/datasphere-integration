package org.jctools.queues;

import org.jctools.util.*;

abstract class SpmcArrayQueueProducerField<E> extends SpmcArrayQueueL1Pad<E>
{
    protected static final long P_INDEX_OFFSET;
    protected long producerIndex;
    
    protected final long lvProducerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpmcArrayQueueProducerField.P_INDEX_OFFSET);
    }
    
    protected final void soProducerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpmcArrayQueueProducerField.P_INDEX_OFFSET, v);
    }
    
    public SpmcArrayQueueProducerField(final int capacity) {
        super(capacity);
    }
    
    static {
        try {
            P_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(SpmcArrayQueueProducerField.class.getDeclaredField("producerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
