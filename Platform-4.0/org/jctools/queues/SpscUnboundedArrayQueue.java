package org.jctools.queues;

import java.util.*;
import org.jctools.util.*;
import java.lang.reflect.*;

public class SpscUnboundedArrayQueue<E> extends SpscUnboundedArrayQueueConsumerField<E> implements QueueProgressIndicators
{
    static final int MAX_LOOK_AHEAD_STEP;
    private static final long P_INDEX_OFFSET;
    private static final long C_INDEX_OFFSET;
    private static final long REF_ARRAY_BASE;
    private static final int REF_ELEMENT_SHIFT;
    private static final Object HAS_NEXT;
    
    public SpscUnboundedArrayQueue(final int chunkSize) {
        final int p2capacity = Math.max(Pow2.roundToPowerOfTwo(chunkSize), 16);
        final long mask = p2capacity - 1;
        final E[] buffer = (E[])new Object[p2capacity + 1];
        this.producerBuffer = buffer;
        this.producerMask = mask;
        this.producerLookAheadStep = Math.min(p2capacity / 4, SpscUnboundedArrayQueue.MAX_LOOK_AHEAD_STEP);
        this.consumerBuffer = buffer;
        this.consumerMask = mask;
        this.producerLookAhead = mask - 1L;
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
        final E[] buffer = (E[])this.producerBuffer;
        final long index = this.producerIndex;
        final long mask = this.producerMask;
        final long offset = calcWrappedOffset(index, mask);
        if (index < this.producerLookAhead) {
            this.writeToQueue(buffer, e, index, offset);
        }
        else {
            final int lookAheadStep = this.producerLookAheadStep;
            final long lookAheadElementOffset = calcWrappedOffset(index + lookAheadStep, mask);
            if (null == lvElement(buffer, lookAheadElementOffset)) {
                this.producerLookAhead = index + lookAheadStep - 1L;
                this.writeToQueue(buffer, e, index, offset);
            }
            else if (null == lvElement(buffer, calcWrappedOffset(index + 1L, mask))) {
                this.writeToQueue(buffer, e, index, offset);
            }
            else {
                this.linkNewBuffer(buffer, index, offset, e, mask);
            }
        }
        return true;
    }
    
    private void writeToQueue(final E[] buffer, final E e, final long index, final long offset) {
        this.soProducerIndex(index + 1L);
        soElement(buffer, offset, e);
    }
    
    private void linkNewBuffer(final E[] oldBuffer, final long currIndex, final long offset, final E e, final long mask) {
        final E[] newBuffer = (E[])new Object[oldBuffer.length];
        this.producerBuffer = newBuffer;
        this.producerLookAhead = currIndex + mask - 1L;
        this.writeToQueue(newBuffer, e, currIndex, offset);
        this.soNext(oldBuffer, newBuffer);
        soElement(oldBuffer, offset, SpscUnboundedArrayQueue.HAS_NEXT);
    }
    
    private void soNext(final E[] curr, final E[] next) {
        soElement(curr, calcDirectOffset(curr.length - 1), next);
    }
    
    private E[] lvNext(final E[] curr) {
        return (E[])lvElement(curr, calcDirectOffset(curr.length - 1));
    }
    
    @Override
    public final E poll() {
        final E[] buffer = (E[])this.consumerBuffer;
        final long index = this.consumerIndex;
        final long mask = this.consumerMask;
        final long offset = calcWrappedOffset(index, mask);
        final Object e = lvElement(buffer, offset);
        final boolean isNextBuffer = e == SpscUnboundedArrayQueue.HAS_NEXT;
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
    
    private E newBufferPoll(final E[] nextBuffer, final long index, final long mask) {
        this.consumerBuffer = nextBuffer;
        final long offsetInNew = calcWrappedOffset(index, mask);
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
        final E[] buffer = (E[])this.consumerBuffer;
        final long index = this.consumerIndex;
        final long mask = this.consumerMask;
        final long offset = calcWrappedOffset(index, mask);
        final Object e = lvElement(buffer, offset);
        if (e == SpscUnboundedArrayQueue.HAS_NEXT) {
            return this.newBufferPeek(this.lvNext(buffer), index, mask);
        }
        return (E)e;
    }
    
    private E newBufferPeek(final E[] nextBuffer, final long index, final long mask) {
        this.consumerBuffer = nextBuffer;
        final long offsetInNew = calcWrappedOffset(index, mask);
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
        this.producerLookAheadStep = Math.min(capacity / 4, SpscUnboundedArrayQueue.MAX_LOOK_AHEAD_STEP);
    }
    
    private long lvProducerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscUnboundedArrayQueue.P_INDEX_OFFSET);
    }
    
    private long lvConsumerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscUnboundedArrayQueue.C_INDEX_OFFSET);
    }
    
    private void soProducerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscUnboundedArrayQueue.P_INDEX_OFFSET, v);
    }
    
    private void soConsumerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscUnboundedArrayQueue.C_INDEX_OFFSET, v);
    }
    
    private static final long calcWrappedOffset(final long index, final long mask) {
        return calcDirectOffset(index & mask);
    }
    
    private static final long calcDirectOffset(final long index) {
        return SpscUnboundedArrayQueue.REF_ARRAY_BASE + (index << SpscUnboundedArrayQueue.REF_ELEMENT_SHIFT);
    }
    
    private static final void soElement(final Object[] buffer, final long offset, final Object e) {
        UnsafeAccess.UNSAFE.putOrderedObject(buffer, offset, e);
    }
    
    private static final <E> Object lvElement(final E[] buffer, final long offset) {
        return UnsafeAccess.UNSAFE.getObjectVolatile(buffer, offset);
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
        final int scale = UnsafeAccess.UNSAFE.arrayIndexScale(Object[].class);
        if (4 == scale) {
            REF_ELEMENT_SHIFT = 2;
        }
        else {
            if (8 != scale) {
                throw new IllegalStateException("Unknown pointer size");
            }
            REF_ELEMENT_SHIFT = 3;
        }
        REF_ARRAY_BASE = UnsafeAccess.UNSAFE.arrayBaseOffset(Object[].class);
        try {
            final Field iField = SpscUnboundedArrayQueueProducerFields.class.getDeclaredField("producerIndex");
            P_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(iField);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        try {
            final Field iField = SpscUnboundedArrayQueueConsumerField.class.getDeclaredField("consumerIndex");
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(iField);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
