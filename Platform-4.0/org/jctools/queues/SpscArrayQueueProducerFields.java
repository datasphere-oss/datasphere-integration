package org.jctools.queues;

import org.jctools.util.*;

abstract class SpscArrayQueueProducerFields<E> extends SpscArrayQueueL1Pad<E>
{
    protected static final long P_INDEX_OFFSET;
    protected long producerIndex;
    protected long producerLookAhead;
    
    public SpscArrayQueueProducerFields(final int capacity) {
        super(capacity);
    }
    
    static {
        try {
            P_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(SpscArrayQueueProducerFields.class.getDeclaredField("producerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
