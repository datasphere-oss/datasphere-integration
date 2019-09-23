package org.jctools.queues;

abstract class MpscArrayQueueHeadCacheField<E> extends MpscArrayQueueMidPad<E>
{
    private volatile long consumerIndexCache;
    
    public MpscArrayQueueHeadCacheField(final int capacity) {
        super(capacity);
    }
    
    protected final long lvConsumerIndexCache() {
        return this.consumerIndexCache;
    }
    
    protected final void svConsumerIndexCache(final long v) {
        this.consumerIndexCache = v;
    }
}
