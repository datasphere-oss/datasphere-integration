package org.jctools.queues.atomic;

import org.jctools.queues.*;
import java.util.concurrent.atomic.*;
import org.jctools.util.*;
import java.util.*;

public class SpscUnboundedAtomicArrayQueue<E> extends AbstractQueue<E> implements QueueProgressIndicators
{
    static final int MAX_LOOK_AHEAD_STEP;
    protected final AtomicLong producerIndex;
    protected int producerLookAheadStep;
    protected long producerLookAhead;
    protected int producerMask;
    protected AtomicReferenceArray<Object> producerBuffer;
    protected int consumerMask;
    protected AtomicReferenceArray<Object> consumerBuffer;
    protected final AtomicLong consumerIndex;
    private static final Object HAS_NEXT;
    
    public SpscUnboundedAtomicArrayQueue(final int chunkSize) {
        final int p2ChunkSize = Math.max(Pow2.roundToPowerOfTwo(chunkSize), 16);
        final int mask = p2ChunkSize - 1;
        final AtomicReferenceArray<Object> buffer = new AtomicReferenceArray<Object>(p2ChunkSize + 1);
        this.producerBuffer = buffer;
        this.producerMask = mask;
        this.adjustLookAheadStep(p2ChunkSize);
        this.consumerBuffer = buffer;
        this.consumerMask = mask;
        this.producerLookAhead = mask - 1;
        this.producerIndex = new AtomicLong();
        this.consumerIndex = new AtomicLong();
        this.soProducerIndex(0L);
    }
    
    @Override
    public final Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public final boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final AtomicReferenceArray<Object> buffer = this.producerBuffer;
        final long index = this.lpProducerIndex();
        final int mask = this.producerMask;
        final int offset = calcWrappedOffset(index, mask);
        if (index < this.producerLookAhead) {
            return this.writeToQueue(buffer, e, index, offset);
        }
        final int lookAheadStep = this.producerLookAheadStep;
        final int lookAheadElementOffset = calcWrappedOffset(index + lookAheadStep, mask);
        if (null == lvElement(buffer, lookAheadElementOffset)) {
            this.producerLookAhead = index + lookAheadStep - 1L;
            return this.writeToQueue(buffer, e, index, offset);
        }
        if (null == lvElement(buffer, calcWrappedOffset(index + 1L, mask))) {
            return this.writeToQueue(buffer, e, index, offset);
        }
        this.resize(buffer, index, offset, e, mask);
        return true;
    }
    
    private boolean writeToQueue(final AtomicReferenceArray<Object> buffer, final E e, final long index, final int offset) {
        this.soProducerIndex(index + 1L);
        soElement(buffer, offset, e);
        return true;
    }
    
    private void resize(final AtomicReferenceArray<Object> oldBuffer, final long currIndex, final int offset, final E e, final long mask) {
        final int capacity = oldBuffer.length();
        final AtomicReferenceArray<Object> newBuffer = new AtomicReferenceArray<Object>(capacity);
        this.producerBuffer = newBuffer;
        this.producerLookAhead = currIndex + mask - 1L;
        this.soProducerIndex(currIndex + 1L);
        soElement(newBuffer, offset, e);
        this.soNext(oldBuffer, newBuffer);
        soElement(oldBuffer, offset, SpscUnboundedAtomicArrayQueue.HAS_NEXT);
    }
    
    private void soNext(final AtomicReferenceArray<Object> curr, final AtomicReferenceArray<Object> next) {
        soElement(curr, calcDirectOffset(curr.length() - 1), next);
    }
    
    private AtomicReferenceArray<Object> lvNext(final AtomicReferenceArray<Object> curr) {
        return (AtomicReferenceArray<Object>)lvElement(curr, calcDirectOffset(curr.length() - 1));
    }
    
    @Override
    public final E poll() {
        final AtomicReferenceArray<Object> buffer = this.consumerBuffer;
        final long index = this.lpConsumerIndex();
        final int mask = this.consumerMask;
        final int offset = calcWrappedOffset(index, mask);
        final Object e = lvElement(buffer, offset);
        final boolean isNextBuffer = e == SpscUnboundedAtomicArrayQueue.HAS_NEXT;
        if (null != e && !isNextBuffer) {
            this.soConsumerIndex(index + 1L);
            soElement(buffer, offset, null);
            return (E)e;
        }
        if (isNextBuffer) {
            return this.newBufferPoll(this.lvNext(buffer), index, mask);
        }
        return null;
    }
    
    private E newBufferPoll(final AtomicReferenceArray<Object> nextBuffer, final long index, final int mask) {
        this.consumerBuffer = nextBuffer;
        final int offsetInNew = calcWrappedOffset(index, mask);
        final E n = (E)lvElement(nextBuffer, offsetInNew);
        if (null == n) {
            return null;
        }
        this.soConsumerIndex(index + 1L);
        soElement(nextBuffer, offsetInNew, null);
        return n;
    }
    
    @Override
    public final E peek() {
        final AtomicReferenceArray<Object> buffer = this.consumerBuffer;
        final long index = this.lpConsumerIndex();
        final int mask = this.consumerMask;
        final int offset = calcWrappedOffset(index, mask);
        final Object e = lvElement(buffer, offset);
        if (e == SpscUnboundedAtomicArrayQueue.HAS_NEXT) {
            return this.newBufferPeek(this.lvNext(buffer), index, mask);
        }
        return (E)e;
    }
    
    private E newBufferPeek(final AtomicReferenceArray<Object> nextBuffer, final long index, final int mask) {
        this.consumerBuffer = nextBuffer;
        final int offsetInNew = calcWrappedOffset(index, mask);
        return (E)lvElement(nextBuffer, offsetInNew);
    }
    
    @Override
    public final int size() {
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
    
    private void adjustLookAheadStep(final int capacity) {
        this.producerLookAheadStep = Math.min(capacity / 4, SpscUnboundedAtomicArrayQueue.MAX_LOOK_AHEAD_STEP);
    }
    
    private long lvProducerIndex() {
        return this.producerIndex.get();
    }
    
    private long lvConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    private long lpProducerIndex() {
        return this.producerIndex.get();
    }
    
    private long lpConsumerIndex() {
        return this.consumerIndex.get();
    }
    
    private void soProducerIndex(final long v) {
        this.producerIndex.lazySet(v);
    }
    
    private void soConsumerIndex(final long v) {
        this.consumerIndex.lazySet(v);
    }
    
    private static final int calcWrappedOffset(final long index, final int mask) {
        return calcDirectOffset((int)index & mask);
    }
    
    private static final int calcDirectOffset(final int index) {
        return index;
    }
    
    private static final void soElement(final AtomicReferenceArray<Object> buffer, final int offset, final Object e) {
        buffer.lazySet(offset, e);
    }
    
    private static final <E> Object lvElement(final AtomicReferenceArray<Object> buffer, final int offset) {
        return buffer.get(offset);
    }
    
    @Override
    public long currentProducerIndex() {
        return this.lvProducerIndex();
    }
    
    @Override
    public long currentConsumerIndex() {
        return this.lvConsumerIndex();
    }
    
    static {
        MAX_LOOK_AHEAD_STEP = Integer.getInteger("jctools.spsc.max.lookahead.step", 4096);
        HAS_NEXT = new Object();
    }
}
