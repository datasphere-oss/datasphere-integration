package org.jctools.queues.atomic;

import org.jctools.queues.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public final class MpscAtomicArrayQueue<E> extends AtomicReferenceArrayQueue<E> implements QueueProgressIndicators
{
    private final AtomicLong consumerIndex;
    private final AtomicLong producerIndex;
    private volatile long headCache;
    
    public MpscAtomicArrayQueue(final int capacity) {
        super(capacity);
        this.consumerIndex = new AtomicLong();
        this.producerIndex = new AtomicLong();
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final int mask = this.mask;
        final long capacity = mask + 1;
        long consumerIndexCache = this.lvConsumerIndexCache();
        long currentProducerIndex;
        do {
            currentProducerIndex = this.lvProducerIndex();
            final long wrapPoint = currentProducerIndex - capacity;
            if (consumerIndexCache <= wrapPoint) {
                final long currHead = this.lvConsumerIndex();
                if (currHead <= wrapPoint) {
                    return false;
                }
                this.svConsumerIndexCache(currHead);
                consumerIndexCache = currHead;
            }
        } while (!this.casProducerIndex(currentProducerIndex, currentProducerIndex + 1L));
        final int offset = this.calcElementOffset(currentProducerIndex, mask);
        this.soElement(offset, e);
        return true;
    }
    
    public final int weakOffer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final int mask = this.mask;
        final long capacity = mask + 1;
        final long currentTail = this.lvProducerIndex();
        final long consumerIndexCache = this.lvConsumerIndexCache();
        final long wrapPoint = currentTail - capacity;
        if (consumerIndexCache <= wrapPoint) {
            final long currHead = this.lvConsumerIndex();
            if (currHead <= wrapPoint) {
                return 1;
            }
            this.svConsumerIndexCache(currHead);
        }
        if (!this.casProducerIndex(currentTail, currentTail + 1L)) {
            return -1;
        }
        final int offset = this.calcElementOffset(currentTail, mask);
        this.soElement(offset, e);
        return 0;
    }
    
    @Override
    public E poll() {
        final long consumerIndex = this.lvConsumerIndex();
        final int offset = this.calcElementOffset(consumerIndex);
        final AtomicReferenceArray<E> buffer = this.buffer;
        E e = this.lvElement(buffer, offset);
        if (null == e) {
            if (consumerIndex == this.lvProducerIndex()) {
                return null;
            }
            do {
                e = this.lvElement(buffer, offset);
            } while (e == null);
        }
        this.spElement(buffer, offset, null);
        this.soConsumerIndex(consumerIndex + 1L);
        return e;
    }
    
    @Override
    public E peek() {
        final AtomicReferenceArray<E> buffer = this.buffer;
        final long consumerIndex = this.lvConsumerIndex();
        final int offset = this.calcElementOffset(consumerIndex);
        E e = this.lvElement(buffer, offset);
        if (null == e) {
            if (consumerIndex == this.lvProducerIndex()) {
                return null;
            }
            do {
                e = this.lvElement(buffer, offset);
            } while (e == null);
        }
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
    
    private long lvConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    private long lvProducerIndex() {
        return this.producerIndex.get();
    }
    
    protected final long lvConsumerIndexCache() {
        return this.headCache;
    }
    
    protected final void svConsumerIndexCache(final long v) {
        this.headCache = v;
    }
    
    protected final boolean casProducerIndex(final long expect, final long newValue) {
        return this.producerIndex.compareAndSet(expect, newValue);
    }
    
    protected void soConsumerIndex(final long l) {
        this.consumerIndex.lazySet(l);
    }
}
