package org.jctools.queues;

import java.util.*;
import org.jctools.util.*;
import java.lang.reflect.*;

public class SpscGrowableArrayQueue<E> extends SpscGrowableArrayQueueConsumerFields<E> implements QueueProgressIndicators
{
    private static final long P_INDEX_OFFSET;
    private static final long C_INDEX_OFFSET;
    private static final Object JUMP;
    
    public SpscGrowableArrayQueue(final int capacity) {
        this(Pow2.roundToPowerOfTwo(Math.max(capacity, 32) / 2), Math.max(capacity, 32));
    }
    
    public SpscGrowableArrayQueue(final int initialCapacity, final int capacity) {
        final int p2initialCapacity = Pow2.roundToPowerOfTwo(Math.max(initialCapacity, 32) / 2);
        final int p2capacity = Pow2.roundToPowerOfTwo(Math.max(capacity, 32));
        if (p2initialCapacity >= p2capacity) {
            throw new IllegalArgumentException("Initial capacity(" + initialCapacity + ") rounded up to a power of 2 cannot exceed maximum capacity (" + capacity + ")rounded up to a power of 2");
        }
        final long mask = p2initialCapacity - 1;
        final E[] buffer = CircularArrayOffsetCalculator.allocate(p2initialCapacity + 1);
        this.producerBuffer = buffer;
        this.producerMask = mask;
        this.adjustLookAheadStep(p2initialCapacity);
        this.consumerBuffer = buffer;
        this.consumerMask = mask;
        this.maxQueueCapacity = p2capacity;
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
        final long offset = CircularArrayOffsetCalculator.calcElementOffset(index, mask);
        if (index < this.producerLookAhead) {
            this.writeToQueue(buffer, e, index, offset);
            return true;
        }
        return this.offerColdPath(e, buffer, index, mask, offset);
    }
    
    private boolean offerColdPath(final E e, final E[] buffer, final long index, final long mask, final long offset) {
        final int lookAheadStep = this.producerLookAheadStep;
        if (lookAheadStep > 0) {
            final long lookAheadElementOffset = CircularArrayOffsetCalculator.calcElementOffset(index + lookAheadStep, mask);
            if (null == UnsafeRefArrayAccess.lvElement(buffer, lookAheadElementOffset)) {
                this.producerLookAhead = index + lookAheadStep - 1L;
                this.writeToQueue(buffer, e, index, offset);
                return true;
            }
            final int maxCapacity = this.maxQueueCapacity;
            if (mask + 1L != maxCapacity) {
                if (null == UnsafeRefArrayAccess.lvElement(buffer, CircularArrayOffsetCalculator.calcElementOffset(index + 1L, mask))) {
                    this.writeToQueue(buffer, e, index, offset);
                }
                else {
                    final int newCapacity = (int)(2L * (mask + 1L));
                    final E[] newBuffer = CircularArrayOffsetCalculator.allocate(newCapacity + 1);
                    this.producerBuffer = newBuffer;
                    this.producerMask = newCapacity - 1;
                    if (newCapacity == maxCapacity) {
                        final long currConsumerIndex = this.lvConsumerIndex();
                        this.producerLookAheadStep = -(int)(index - currConsumerIndex);
                        this.producerLookAhead = currConsumerIndex + maxCapacity - 1L;
                    }
                    else {
                        this.producerLookAhead = index + this.producerMask - 1L;
                        this.adjustLookAheadStep(newCapacity);
                    }
                    final long offsetInNew = CircularArrayOffsetCalculator.calcElementOffset(index, this.producerMask);
                    this.soProducerIndex(index + 1L);
                    UnsafeRefArrayAccess.soElement(newBuffer, offsetInNew, e);
                    UnsafeRefArrayAccess.soElement(buffer, this.nextArrayOffset(mask), newBuffer);
                    UnsafeRefArrayAccess.soElement(buffer, offset, SpscGrowableArrayQueue.JUMP);
                }
                return true;
            }
            if (null == UnsafeRefArrayAccess.lvElement(buffer, offset)) {
                this.writeToQueue(buffer, e, index, offset);
                return true;
            }
            return false;
        }
        else {
            final int prevElementsInOtherBuffers = -lookAheadStep;
            final long currConsumerIndex2 = this.lvConsumerIndex();
            final int size = (int)(index - currConsumerIndex2);
            final int maxCapacity2 = (int)mask + 1;
            if (size == maxCapacity2) {
                return false;
            }
            final long firstIndexInCurrentBuffer = this.producerLookAhead - maxCapacity2 + prevElementsInOtherBuffers;
            if (currConsumerIndex2 >= firstIndexInCurrentBuffer) {
                this.adjustLookAheadStep(maxCapacity2);
            }
            else {
                this.producerLookAheadStep = (int)(currConsumerIndex2 - firstIndexInCurrentBuffer);
            }
            this.producerLookAhead = currConsumerIndex2 + maxCapacity2;
            this.writeToQueue(buffer, e, index, offset);
            return true;
        }
    }
    
    private void writeToQueue(final E[] buffer, final E e, final long index, final long offset) {
        this.soProducerIndex(index + 1L);
        UnsafeRefArrayAccess.soElement(buffer, offset, e);
    }
    
    @Override
    public final E poll() {
        final E[] buffer = (E[])this.consumerBuffer;
        final long index = this.consumerIndex;
        final long mask = this.consumerMask;
        final long offset = CircularArrayOffsetCalculator.calcElementOffset(index, mask);
        final Object e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null != e) {
            if (e == SpscGrowableArrayQueue.JUMP) {
                final E[] nextBuffer = this.getNextBuffer(buffer, mask);
                return this.newBufferPoll(nextBuffer, index);
            }
            this.soConsumerIndex(index + 1L);
            UnsafeRefArrayAccess.soElement(buffer, offset, (E)null);
        }
        return (E)e;
    }
    
    private E[] getNextBuffer(final E[] buffer, final long mask) {
        return UnsafeRefArrayAccess.lvElement((E[][])(Object)buffer, this.nextArrayOffset(mask));
    }
    
    private long nextArrayOffset(final long mask) {
        return CircularArrayOffsetCalculator.calcElementOffset(mask + 1L, mask << 2);
    }
    
    private E newBufferPoll(final E[] nextBuffer, final long index) {
        this.consumerBuffer = nextBuffer;
        final long newMask = nextBuffer.length - 2;
        this.consumerMask = newMask;
        final long offsetInNew = CircularArrayOffsetCalculator.calcElementOffset(index, newMask);
        final E n = UnsafeRefArrayAccess.lvElement(nextBuffer, offsetInNew);
        if (null == n) {
            throw new IllegalStateException("new buffer must have at least one element");
        }
        this.soConsumerIndex(index + 1L);
        UnsafeRefArrayAccess.soElement(nextBuffer, offsetInNew, (E)null);
        return n;
    }
    
    @Override
    public final E peek() {
        final E[] buffer = (E[])this.consumerBuffer;
        final long index = this.consumerIndex;
        final long mask = this.consumerMask;
        final long offset = CircularArrayOffsetCalculator.calcElementOffset(index, mask);
        final Object e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null == e) {
            return null;
        }
        if (e == SpscGrowableArrayQueue.JUMP) {
            return this.newBufferPeek(this.getNextBuffer(buffer, mask), index);
        }
        return (E)e;
    }
    
    private E newBufferPeek(final E[] nextBuffer, final long index) {
        this.consumerBuffer = nextBuffer;
        final long newMask = nextBuffer.length - 2;
        this.consumerMask = newMask;
        final long offsetInNew = CircularArrayOffsetCalculator.calcElementOffset(index, newMask);
        return UnsafeRefArrayAccess.lvElement(nextBuffer, offsetInNew);
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
        this.producerLookAheadStep = Math.min(capacity / 4, SpscArrayQueue.MAX_LOOK_AHEAD_STEP);
    }
    
    private long lvProducerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscGrowableArrayQueue.P_INDEX_OFFSET);
    }
    
    private long lvConsumerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscGrowableArrayQueue.C_INDEX_OFFSET);
    }
    
    private void soProducerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscGrowableArrayQueue.P_INDEX_OFFSET, v);
    }
    
    private void soConsumerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscGrowableArrayQueue.C_INDEX_OFFSET, v);
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
        try {
            final Field iField = SpscGrowableArrayQueueProducerFields.class.getDeclaredField("producerIndex");
            P_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(iField);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        try {
            final Field iField = SpscGrowableArrayQueueConsumerFields.class.getDeclaredField("consumerIndex");
            C_INDEX_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(iField);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        JUMP = new Object();
    }
}
