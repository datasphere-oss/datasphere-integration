package org.jctools.queues.atomic;

import org.jctools.queues.*;
import java.util.concurrent.atomic.*;
import java.util.*;

public class MpmcAtomicArrayQueue<E> extends SequencedAtomicReferenceArrayQueue<E> implements QueueProgressIndicators
{
    private final AtomicLong producerIndex;
    private final AtomicLong consumerIndex;
    
    public MpmcAtomicArrayQueue(final int capacity) {
        super(validateCapacity(capacity));
        this.producerIndex = new AtomicLong();
        this.consumerIndex = new AtomicLong();
    }
    
    private static int validateCapacity(final int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("Minimum size is 2");
        }
        return capacity;
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final int mask = this.mask;
        final int capacity = mask + 1;
        final AtomicLongArray sBuffer = this.sequenceBuffer;
        long cIndex = Long.MAX_VALUE;
        while (true) {
            final long currentProducerIndex = this.lvProducerIndex();
            final int seqOffset = SequencedAtomicReferenceArrayQueue.calcSequenceOffset(currentProducerIndex, mask);
            final long seq = this.lvSequence(sBuffer, seqOffset);
            final long delta = seq - currentProducerIndex;
            if (delta == 0L) {
                if (this.casProducerIndex(currentProducerIndex, currentProducerIndex + 1L)) {
                    final int elementOffset = this.calcElementOffset(currentProducerIndex, mask);
                    this.spElement(elementOffset, e);
                    this.soSequence(sBuffer, seqOffset, currentProducerIndex + 1L);
                    return true;
                }
                continue;
            }
            else {
                if (delta < 0L && currentProducerIndex - capacity <= cIndex && currentProducerIndex - capacity <= (cIndex = this.lvConsumerIndex())) {
                    return false;
                }
                continue;
            }
        }
    }
    
    @Override
    public E poll() {
        final AtomicLongArray lSequenceBuffer = this.sequenceBuffer;
        final int mask = this.mask;
        long pIndex = -1L;
        while (true) {
            final long currentConsumerIndex = this.lvConsumerIndex();
            final int seqOffset = SequencedAtomicReferenceArrayQueue.calcSequenceOffset(currentConsumerIndex, mask);
            final long seq = this.lvSequence(lSequenceBuffer, seqOffset);
            final long delta = seq - (currentConsumerIndex + 1L);
            if (delta == 0L) {
                if (this.casConsumerIndex(currentConsumerIndex, currentConsumerIndex + 1L)) {
                    final int offset = this.calcElementOffset(currentConsumerIndex, mask);
                    final E e = this.lpElement(offset);
                    this.spElement(offset, null);
                    this.soSequence(lSequenceBuffer, seqOffset, currentConsumerIndex + mask + 1L);
                    return e;
                }
                continue;
            }
            else {
                if (delta < 0L && currentConsumerIndex >= pIndex && currentConsumerIndex == (pIndex = this.lvProducerIndex())) {
                    return null;
                }
                continue;
            }
        }
    }
    
    @Override
    public E peek() {
        E e;
        long currConsumerIndex;
        do {
            currConsumerIndex = this.lvConsumerIndex();
            e = this.lpElement(this.calcElementOffset(currConsumerIndex));
        } while (e == null && currConsumerIndex != this.lvProducerIndex());
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
    
    protected final long lvProducerIndex() {
        return this.producerIndex.get();
    }
    
    protected final boolean casProducerIndex(final long expect, final long newValue) {
        return this.producerIndex.compareAndSet(expect, newValue);
    }
    
    protected final long lvConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    protected final boolean casConsumerIndex(final long expect, final long newValue) {
        return this.consumerIndex.compareAndSet(expect, newValue);
    }
}
