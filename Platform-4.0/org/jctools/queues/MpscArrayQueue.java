package org.jctools.queues;

import org.jctools.util.*;

public class MpscArrayQueue<E> extends MpscArrayQueueConsumerField<E> implements QueueProgressIndicators
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
    
    public MpscArrayQueue(final int capacity) {
        super(capacity);
    }
    
    public boolean offerIfBelowTheshold(final E e, final int threshold) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long mask = this.mask;
        long consumerIndexCache = this.lvConsumerIndexCache();
        long currentProducerIndex;
        do {
            currentProducerIndex = this.lvProducerIndex();
            final long wrapPoint = currentProducerIndex - threshold;
            if (consumerIndexCache <= wrapPoint) {
                final long currHead = this.lvConsumerIndex();
                if (currHead <= wrapPoint) {
                    return false;
                }
                this.svConsumerIndexCache(currHead);
                consumerIndexCache = currHead;
            }
        } while (!this.casProducerIndex(currentProducerIndex, currentProducerIndex + 1L));
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(currentProducerIndex, mask);
        UnsafeRefArrayAccess.soElement(this.buffer, offset, e);
        return true;
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long mask = this.mask;
        final long capacity = mask + 1L;
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
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(currentProducerIndex, mask);
        UnsafeRefArrayAccess.soElement(this.buffer, offset, e);
        return true;
    }
    
    public final int failFastOffer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final long mask = this.mask;
        final long capacity = mask + 1L;
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
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(currentTail, mask);
        UnsafeRefArrayAccess.soElement(this.buffer, offset, e);
        return 0;
    }
    
    @Override
    public E poll() {
        final long consumerIndex = this.lpConsumerIndex();
        final long offset = this.calcElementOffset(consumerIndex);
        final E[] buffer = (E[])this.buffer;
        E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null == e) {
            if (consumerIndex == this.lvProducerIndex()) {
                return null;
            }
            do {
                e = UnsafeRefArrayAccess.lvElement(buffer, offset);
            } while (e == null);
        }
        UnsafeRefArrayAccess.spElement(buffer, offset, (E)null);
        this.soConsumerIndex(consumerIndex + 1L);
        return e;
    }
    
    @Override
    public E peek() {
        final E[] buffer = (E[])this.buffer;
        final long consumerIndex = this.lpConsumerIndex();
        final long offset = this.calcElementOffset(consumerIndex);
        E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null == e) {
            if (consumerIndex == this.lvProducerIndex()) {
                return null;
            }
            do {
                e = UnsafeRefArrayAccess.lvElement(buffer, offset);
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
    
    @Override
    public boolean relaxedOffer(final E e) {
        return this.offer(e);
    }
    
    @Override
    public E relaxedPoll() {
        final E[] buffer = (E[])this.buffer;
        final long consumerIndex = this.lpConsumerIndex();
        final long offset = this.calcElementOffset(consumerIndex);
        final E e = UnsafeRefArrayAccess.lvElement(buffer, offset);
        if (null == e) {
            return null;
        }
        UnsafeRefArrayAccess.spElement(buffer, offset, (E)null);
        this.soConsumerIndex(consumerIndex + 1L);
        return e;
    }
    
    @Override
    public E relaxedPeek() {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long consumerIndex = this.lpConsumerIndex();
        return UnsafeRefArrayAccess.lvElement(buffer, ConcurrentCircularArrayQueue.calcElementOffset(consumerIndex, mask));
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c) {
        return this.drain(c, this.capacity());
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s) {
        long result = 0L;
        final int capacity = this.capacity();
        do {
            final int filled = this.fill(s, MpmcArrayQueue.RECOMENDED_OFFER_BATCH);
            if (filled == 0) {
                return (int)result;
            }
            result += filled;
        } while (result <= capacity);
        return (int)result;
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c, final int limit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long consumerIndex = this.lpConsumerIndex();
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
        final long mask = this.mask;
        final long capacity = mask + 1L;
        long cachedConsumerIndex = this.lvConsumerIndexCache();
        int actualLimit = 0;
        long currentProducerIndex;
        do {
            currentProducerIndex = this.lvProducerIndex();
            long available = capacity - (currentProducerIndex - cachedConsumerIndex);
            if (available <= 0L) {
                final long currConsumerIndex = this.lvConsumerIndex();
                available += currConsumerIndex - cachedConsumerIndex;
                if (available <= 0L) {
                    return 0;
                }
                this.svConsumerIndexCache(currConsumerIndex);
                cachedConsumerIndex = currConsumerIndex;
            }
            actualLimit = Math.min((int)available, limit);
        } while (!this.casProducerIndex(currentProducerIndex, currentProducerIndex + actualLimit));
        final E[] buffer = (E[])this.buffer;
        for (int i = 0; i < actualLimit; ++i) {
            final long offset = ConcurrentCircularArrayQueue.calcElementOffset(currentProducerIndex + i, mask);
            UnsafeRefArrayAccess.soElement(buffer, offset, s.get());
        }
        return actualLimit;
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition exit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        long consumerIndex = this.lpConsumerIndex();
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
    public void fill(final MessagePassingQueue.Supplier<E> s, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition exit) {
        int idleCounter = 0;
        while (exit.keepRunning()) {
            if (this.fill(s, MpmcArrayQueue.RECOMENDED_OFFER_BATCH) == 0) {
                idleCounter = w.idle(idleCounter);
            }
            else {
                idleCounter = 0;
            }
        }
    }
}
