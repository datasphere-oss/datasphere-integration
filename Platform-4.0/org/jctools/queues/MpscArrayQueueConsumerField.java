package org.jctools.queues;

import org.jctools.util.*;

abstract class MpscArrayQueueConsumerField<E> extends MpscArrayQueueL2Pad<E>
{
    private static final long C_INDEX_OFFSET;
    protected long consumerIndex;
    
    public MpscArrayQueueConsumerField(final int capacity) {
        super(capacity);
    }
    
    protected final long lpConsumerIndex() {
        return this.consumerIndex;
    }
    
    protected final long lvConsumerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, MpscArrayQueueConsumerField.C_INDEX_OFFSET);
    }
    
    protected void soConsumerIndex(final long l) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, MpscArrayQueueConsumerField.C_INDEX_OFFSET, l);
    }
    
    static {
        try {
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(MpscArrayQueueConsumerField.class.getDeclaredField("consumerIndex"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
