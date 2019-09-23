package org.jctools.queues.atomic;

import org.jctools.queues.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public final class SpmcAtomicArrayQueue<E> extends AtomicReferenceArrayQueue<E> implements QueueProgressIndicators
{
    private final AtomicLong consumerIndex;
    private final AtomicLong producerIndex;
    private final AtomicLong producerIndexCache;
    
    public SpmcAtomicArrayQueue(final int capacity) {
        super(capacity);
        this.consumerIndex = new AtomicLong();
        this.producerIndex = new AtomicLong();
        this.producerIndexCache = new AtomicLong();
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final AtomicReferenceArray<E> buffer = this.buffer;
        final int mask = this.mask;
        final long currProducerIndex = this.lvProducerIndex();
        final int offset = this.calcElementOffset(currProducerIndex, mask);
        if (null != this.lvElement(buffer, offset)) {
            final long size = currProducerIndex - this.lvConsumerIndex();
            if (size > mask) {
                return false;
            }
            while (null != this.lvElement(buffer, offset)) {}
        }
        this.spElement(buffer, offset, e);
        this.soTail(currProducerIndex + 1L);
        return true;
    }
    
    @Override
    public E poll() {
        final long currProducerIndexCache = this.lvProducerIndexCache();
        long currentConsumerIndex;
        do {
            currentConsumerIndex = this.lvConsumerIndex();
            if (currentConsumerIndex >= currProducerIndexCache) {
                final long currProducerIndex = this.lvProducerIndex();
                if (currentConsumerIndex >= currProducerIndex) {
                    return null;
                }
                this.svProducerIndexCache(currProducerIndex);
            }
        } while (!this.casHead(currentConsumerIndex, currentConsumerIndex + 1L));
        final int offset = this.calcElementOffset(currentConsumerIndex);
        final AtomicReferenceArray<E> lb = this.buffer;
        final E e = this.lpElement(lb, offset);
        this.soElement(lb, offset, null);
        return e;
    }
    
    @Override
    public E peek() {
        final int mask = this.mask;
        final long currProducerIndexCache = this.lvProducerIndexCache();
        long currentConsumerIndex;
        E e;
        do {
            currentConsumerIndex = this.lvConsumerIndex();
            if (currentConsumerIndex >= currProducerIndexCache) {
                final long currProducerIndex = this.lvProducerIndex();
                if (currentConsumerIndex >= currProducerIndex) {
                    return null;
                }
                this.svProducerIndexCache(currProducerIndex);
            }
        } while (null == (e = this.lvElement(this.calcElementOffset(currentConsumerIndex, mask))));
        return e;
    }
    
    @Override
    public int size() {
        long after = this.lvConsumerIndex();
        long before;
        long currentProducerIndex;
        do {
            before = after;
            currentProducerIndex = this.lvProducerIndex();
            after = this.lvConsumerIndex();
        } while (before != after);
        return (int)(currentProducerIndex - after);
    }
    
    @Override
    public boolean isEmpty() {
        return this.lvConsumerIndex() == this.lvProducerIndex();
    }
    
    @Override
    public long currentProducerIndex() {
        return this.lvProducerIndex();
    }
    
    @Override
    public long currentConsumerIndex() {
        return this.lvConsumerIndex();
    }
    
    protected final long lvProducerIndexCache() {
        return this.producerIndexCache.get();
    }
    
    protected final void svProducerIndexCache(final long v) {
        this.producerIndexCache.set(v);
    }
    
    protected final long lvConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    protected final boolean casHead(final long expect, final long newValue) {
        return this.consumerIndex.compareAndSet(expect, newValue);
    }
    
    protected final long lvProducerIndex() {
        return this.producerIndex.get();
    }
    
    protected final void soTail(final long v) {
        this.producerIndex.lazySet(v);
    }
}
