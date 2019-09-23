package org.jctools.queues;

abstract class SpmcArrayQueueProducerIndexCacheField<E> extends SpmcArrayQueueMidPad<E>
{
    private volatile long producerIndexCache;
    
    public SpmcArrayQueueProducerIndexCacheField(final int capacity) {
        super(capacity);
    }
    
    protected final long lvProducerIndexCache() {
        return this.producerIndexCache;
    }
    
    protected final void svProducerIndexCache(final long v) {
        this.producerIndexCache = v;
    }
}
