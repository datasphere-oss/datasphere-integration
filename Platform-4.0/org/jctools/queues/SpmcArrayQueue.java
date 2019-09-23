package org.jctools.queues;

import org.jctools.util.*;

public class SpmcArrayQueue<E> extends SpmcArrayQueueL3Pad<E> implements QueueProgressIndicators
{
    public SpmcArrayQueue(final int capacity) {
        super(capacity);
    }
    
    @Override
    public boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long currProducerIndex = this.lvProducerIndex();
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(currProducerIndex, mask);
        if (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {
            final long size = currProducerIndex - this.lvConsumerIndex();
            if (size > mask) {
                return false;
            }
            while (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {}
        }
        UnsafeRefArrayAccess.spElement(buffer, offset, e);
        this.soProducerIndex(currProducerIndex + 1L);
        return true;
    }
    
    @Override
    public E poll() {
        long currProducerIndexCache = this.lvProducerIndexCache();
        long currentConsumerIndex;
        do {
            currentConsumerIndex = this.lvConsumerIndex();
            if (currentConsumerIndex >= currProducerIndexCache) {
                final long currProducerIndex = this.lvProducerIndex();
                if (currentConsumerIndex >= currProducerIndex) {
                    return null;
                }
                currProducerIndexCache = currProducerIndex;
                this.svProducerIndexCache(currProducerIndex);
            }
        } while (!this.casHead(currentConsumerIndex, currentConsumerIndex + 1L));
        return (E)this.removeElement(this.buffer, currentConsumerIndex, this.mask);
    }
    
    private E removeElement(final E[] buffer, final long index, final long mask) {
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(index, mask);
        final E e = UnsafeRefArrayAccess.lpElement(buffer, offset);
        UnsafeRefArrayAccess.soElement(buffer, offset, (E)null);
        return e;
    }
    
    @Override
    public E peek() {
        final long mask = this.mask;
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
        } while (null == (e = UnsafeRefArrayAccess.lvElement(this.buffer, ConcurrentCircularArrayQueue.calcElementOffset(currentConsumerIndex, mask))));
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
        if (null == e) {
            throw new NullPointerException("Null is not a valid element");
        }
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long producerIndex = this.lvProducerIndex();
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
        if (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {
            return false;
        }
        UnsafeRefArrayAccess.spElement(buffer, offset, e);
        this.soProducerIndex(producerIndex + 1L);
        return true;
    }
    
    @Override
    public E relaxedPoll() {
        return this.poll();
    }
    
    @Override
    public E relaxedPeek() {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        final long consumerIndex = this.lvConsumerIndex();
        return UnsafeRefArrayAccess.lvElement(buffer, ConcurrentCircularArrayQueue.calcElementOffset(consumerIndex, mask));
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c) {
        int capacity;
        int sum;
        int drained;
        for (capacity = this.capacity(), sum = 0; sum < capacity; sum += drained) {
            drained = 0;
            if ((drained = this.drain(c, MpmcArrayQueue.RECOMENDED_POLL_BATCH)) == 0) {
                break;
            }
        }
        return sum;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s) {
        return this.fill(s, this.capacity());
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c, final int limit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        long currProducerIndexCache = this.lvProducerIndexCache();
        int adjustedLimit = 0;
        long currentConsumerIndex;
        do {
            currentConsumerIndex = this.lvConsumerIndex();
            if (currentConsumerIndex >= currProducerIndexCache) {
                final long currProducerIndex = this.lvProducerIndex();
                if (currentConsumerIndex >= currProducerIndex) {
                    return 0;
                }
                currProducerIndexCache = currProducerIndex;
                this.svProducerIndexCache(currProducerIndex);
            }
            final int remaining = (int)(currProducerIndexCache - currentConsumerIndex);
            adjustedLimit = Math.min(remaining, limit);
        } while (!this.casHead(currentConsumerIndex, currentConsumerIndex + adjustedLimit));
        for (int i = 0; i < adjustedLimit; ++i) {
            c.accept(this.removeElement(buffer, currentConsumerIndex + i, mask));
        }
        return adjustedLimit;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s, final int limit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        long producerIndex = this.producerIndex;
        for (int i = 0; i < limit; ++i) {
            final long offset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
            if (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {
                return i;
            }
            ++producerIndex;
            UnsafeRefArrayAccess.soElement(buffer, offset, s.get());
            this.soProducerIndex(producerIndex);
        }
        return limit;
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition exit) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        int idleCounter = 0;
        while (exit.keepRunning()) {
            if (this.drain(c, MpmcArrayQueue.RECOMENDED_POLL_BATCH) == 0) {
                idleCounter = w.idle(idleCounter);
            }
            else {
                idleCounter = 0;
            }
        }
    }
    
    @Override
    public void fill(final MessagePassingQueue.Supplier<E> s, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition e) {
        final E[] buffer = (E[])this.buffer;
        final long mask = this.mask;
        long producerIndex = this.producerIndex;
        int counter = 0;
        while (e.keepRunning()) {
            for (int i = 0; i < 4096; ++i) {
                final long offset = ConcurrentCircularArrayQueue.calcElementOffset(producerIndex, mask);
                if (null != UnsafeRefArrayAccess.lvElement(buffer, offset)) {
                    counter = w.idle(counter);
                }
                else {
                    ++producerIndex;
                    counter = 0;
                    UnsafeRefArrayAccess.soElement(buffer, offset, s.get());
                    this.soProducerIndex(producerIndex);
                }
            }
        }
    }
}
