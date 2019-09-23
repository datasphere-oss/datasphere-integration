package org.jctools.queues;

import org.jctools.util.*;

abstract class SpscArrayQueueConsumerField<E> extends SpscArrayQueueL2Pad<E>
{
    protected long consumerIndex;
    protected static final long C_INDEX_OFFSET;
    
    public SpscArrayQueueConsumerField(final int capacity) {
        super(capacity);
    }
    
    static {
        try {
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(SpscArrayQueueConsumerField.class.getDeclaredField("consumerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
