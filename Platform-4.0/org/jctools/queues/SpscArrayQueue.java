package org.jctools.queues;

import org.jctools.util.*;

public class SpscArrayQueue<E> extends SpscArrayQueueConsumerField<E> implements QueueProgressIndicators
{
    long p01;
    long p02;
    long p03;
    long p04;
    long p05;
    long p06;
    long p07;
    long p10;
    long p11;
    long p12;
    long p13;
    long p14;
    long p15;
    long p16;
    long p17;
    
    public SpscArrayQueue(final int capacity) {
        super(Math.max(Pow2.roundToPowerOfTwo(capacity), 4));
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long producerIndex = this.producerIndex;
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
        if (producerIndex >= this.producerLookAhead) {
            final int lookAheadStep = this.lookAheadStep;
            if (null == UnsafeRefArrayAccess.lvElement(buffer, ConcurrentCircularArrayQueue.calcElementOffset(producerIndex + lookAheadStep, mask))) {
                this.producerLookAhead = producerIndex + lookAheadStep;
            }
            else if (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {
                return false;
            }
        }
        UnsafeRefArrayAccess.soElement(buffer, offset, e);
        this.soProducerIndex(producerIndex + 1L);
        return true;
    }
    
    @Override
    public E poll() {
        final long consumerIndex = this.consumerIndex;
        final long offset = this.calcElementOffset(consumerIndex);
        final E[] buffer = (E[])this.buffer;
        final E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null == e) {
            return null;
        }
        UnsafeRefArrayAccess.soElement(buffer, offset, (E)null);
        this.soConsumerIndex(consumerIndex + 1L);
        return e;
    }
    
    @Override
    public E peek() {
        return UnsafeRefArrayAccess.lvElement(this.buffer, this.calcElementOffset(this.consumerIndex));
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
    
    private void soProducerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscArrayQueue.P_INDEX_OFFSET, v);
    }
    
    private void soConsumerIndex(final long v) {
        UnsafeAccess.UNSAFE.putOrderedLong(this, SpscArrayQueue.C_INDEX_OFFSET, v);
    }
    
    private long lvProducerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscArrayQueue.P_INDEX_OFFSET);
    }
    
    private long lvConsumerIndex() {
        return UnsafeAccess.UNSAFE.getLongVolatile(this, SpscArrayQueue.C_INDEX_OFFSET);
    }
    
    @Override
    public long currentProducerIndex() {
        return this.lvProducerIndex();
    }
    
    @Override
    public long currentConsumerIndex() {
        return this.lvConsumerIndex();
    }
    
    @Override
    public boolean relaxedOffer(final E message) {
        return this.offer(message);
    }
    
    @Override
    public E relaxedPoll() {
        return this.poll();
    }
    
    @Override
    public E relaxedPeek() {
        return this.peek();
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c) {
        return this.drain(c, this.capacity());
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s) {
        return this.fill(s, this.capacity());
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c, final int limit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long consumerIndex = this.consumerIndex;
        for (int i = 0; i < limit; ++i) {
            final long index = consumerIndex + i;
            final long offset = ConcurrentCircularArrayQueue.calcElementOffset(index, mask);
            final E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
            if (null == e) {
                return i;
            }
            UnsafeRefArrayAccess.soElement(buffer, offset, (E)null);
            this.soConsumerIndex(index + 1L);
            c.accept(e);
        }
        return limit;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s, final int limit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final int lookAheadStep = this.lookAheadStep;
        final long producerIndex = this.producerIndex;
        for (int i = 0; i < limit; ++i) {
            final long index = producerIndex + i;
            final long lookAheadElementOffset = ConcurrentCircularArrayQueue.calcElementOffset(index + lookAheadStep, mask);
            if (null == UnsafeRefArrayAccess.lvElement(buffer, lookAheadElementOffset)) {
                final int lookAheadLimit = Math.min(lookAheadStep, limit - i);
                for (int j = 0; j < lookAheadLimit; ++j) {
                    final long offset = ConcurrentCircularArrayQueue.calcElementOffset(index + j, mask);
                    UnsafeRefArrayAccess.soElement(buffer, offset, s.get());
                    this.soProducerIndex(index + j + 1L);
                }
                i += lookAheadLimit - 1;
            }
            else {
                final long offset2 = ConcurrentCircularArrayQueue.calcElementOffset(index, mask);
                if (null != UnsafeRefArrayAccess.lvElement(buffer, offset2)) {
                    return i;
                }
                UnsafeRefArrayAccess.soElement(buffer, offset2, s.get());
                this.soProducerIndex(index + 1L);
            }
        }
        return limit;
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition exit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        long consumerIndex = this.consumerIndex;
        int counter = 0;
        while (exit.keepRunning()) {
            for (int i = 0; i < 4096; ++i) {
                final long offset = ConcurrentCircularArrayQueue.calcElementOffset(consumerIndex, mask);
                final E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
                if (null == e) {
                    counter = w.idle(counter);
                }
                else {
                    ++consumerIndex;
                    counter = 0;
                    UnsafeRefArrayAccess.soElement(buffer, offset, (E)null);
                    this.soConsumerIndex(consumerIndex);
                    c.accept(e);
                }
            }
        }
    }
    
    @Override
    public void fill(final MessagePassingQueue.Supplier<E> s, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition e) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final int lookAheadStep = this.lookAheadStep;
        long producerIndex = this.producerIndex;
        int counter = 0;
        while (e.keepRunning()) {
            final long lookAheadElementOffset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex + lookAheadStep, mask);
            if (null == UnsafeRefArrayAccess.lvElement(buffer, lookAheadElementOffset)) {
                for (int j = 0; j < lookAheadStep; ++j) {
                    final long offset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
                    ++producerIndex;
                    UnsafeRefArrayAccess.soElement(buffer, offset, s.get());
                    this.soProducerIndex(producerIndex);
                }
            }
            else {
                final long offset2 = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
                if (null != UnsafeRefArrayAccess.lvElement(buffer, offset2)) {
                    counter = w.idle(counter);
                }
                else {
                    ++producerIndex;
                    counter = 0;
                    UnsafeRefArrayAccess.soElement(buffer, offset2, s.get());
                    this.soProducerIndex(producerIndex);
                }
            }
        }
    }
}
