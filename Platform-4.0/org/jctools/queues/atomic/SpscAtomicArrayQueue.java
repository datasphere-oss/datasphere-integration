package org.jctools.queues.atomic;

import org.jctools.queues.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public final class SpscAtomicArrayQueue<E> extends AtomicReferenceArrayQueue<E> implements QueueProgressIndicators
{
    private static final Integer MAX_LOOK_AHEAD_STEP;
    final AtomicLong producerIndex;
    protected long producerLookAhead;
    final AtomicLong consumerIndex;
    final int lookAheadStep;
    
    public SpscAtomicArrayQueue(final int capacity) {
        super(capacity);
        this.producerIndex = new AtomicLong();
        this.consumerIndex = new AtomicLong();
        this.lookAheadStep = Math.min(capacity / 4, SpscAtomicArrayQueue.MAX_LOOK_AHEAD_STEP);
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final AtomicReferenceArray<E> buffer = this.buffer;
        final int mask = this.mask;
        final long index = this.producerIndex.get();
        final int offset = this.calcElementOffset(index, mask);
        if (index >= this.producerLookAhead) {
            final int step = this.lookAheadStep;
            if (null == this.lvElement(buffer, this.calcElementOffset(index + step, mask))) {
                this.producerLookAhead = index + step;
            }
            else if (null != this.lvElement(buffer, offset)) {
                return false;
            }
        }
        this.soProducerIndex(index + 1L);
        this.soElement(buffer, offset, e);
        return true;
    }
    
    @Override
    public E poll() {
        final long index = this.consumerIndex.get();
        final int offset = this.calcElementOffset(index);
        final AtomicReferenceArray<E> lElementBuffer = this.buffer;
        final E e = this.lvElement(lElementBuffer, offset);
        if (null == e) {
            return null;
        }
        this.soConsumerIndex(index + 1L);
        this.soElement(lElementBuffer, offset, null);
        return e;
    }
    
    @Override
    public E peek() {
        return this.lvElement(this.calcElementOffset(this.consumerIndex.get()));
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
    public long currentProducerIndex() {
        return this.lvProducerIndex();
    }
    
    @Override
    public long currentConsumerIndex() {
        return this.lvConsumerIndex();
    }
    
    private void soProducerIndex(final long newIndex) {
        this.producerIndex.lazySet(newIndex);
    }
    
    private void soConsumerIndex(final long newIndex) {
        this.consumerIndex.lazySet(newIndex);
    }
    
    private long lvConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    private long lvProducerIndex() {
        return this.producerIndex.get();
    }
    
    static {
        MAX_LOOK_AHEAD_STEP = Integer.getInteger("jctools.spsc.max.lookahead.step", 4096);
    }
}
