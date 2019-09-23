package org.jctools.queues;

import org.jctools.util.*;

public class MpmcArrayQueue<E> extends MpmcArrayQueueConsumerField<E> implements QueueProgressIndicators
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
    static final int RECOMENDED_POLL_BATCH;
    static final int RECOMENDED_OFFER_BATCH;
    
    public MpmcArrayQueue(final int capacity) {
        super(validateCapacity(capacity));
    }
    
    private static int validateCapacity(final int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("Minimum size is 2");
        }
        return capacity;
    }
    
    @Override
    public boolean offer(final E e) {
        final long mask = this.mask;
        final long capacity = mask + 1L;
        final long[] sBuffer = this.sequenceBuffer;
        long cIndex = Long.MAX_VALUE;
        long seq;
        long pIndex;
        long seqOffset;
        do {
            pIndex = this.lvProducerIndex();
            seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(pIndex, mask);
            seq = this.lvSequence(sBuffer, seqOffset);
            if (seq < pIndex) {
                if (pIndex - capacity <= cIndex && pIndex - capacity <= (cIndex = this.lvConsumerIndex())) {
                    return false;
                }
                seq = pIndex + 1L;
            }
        } while (seq > pIndex || !this.casProducerIndex(pIndex, pIndex + 1L));
        assert null == UnsafeRefArrayAccess.lpElement(this.buffer, ConcurrentCircularArrayQueue.calcElementOffset(pIndex, mask));
        UnsafeRefArrayAccess.spElement(this.buffer, ConcurrentCircularArrayQueue.calcElementOffset(pIndex, mask), e);
        this.soSequence(sBuffer, seqOffset, pIndex + 1L);
        return true;
    }
    
    @Override
    public E poll() {
        final long[] sBuffer = this.sequenceBuffer;
        final long mask = this.mask;
        long pIndex = -1L;
        long seq;
        long expectedSeq;
        long cIndex;
        long seqOffset;
        do {
            cIndex = this.lvConsumerIndex();
            seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(cIndex, mask);
            seq = this.lvSequence(sBuffer, seqOffset);
            expectedSeq = cIndex + 1L;
            if (seq < expectedSeq) {
                if (cIndex >= pIndex && cIndex == (pIndex = this.lvProducerIndex())) {
                    return null;
                }
                seq = expectedSeq + 1L;
            }
        } while (seq > expectedSeq || !this.casConsumerIndex(cIndex, cIndex + 1L));
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(cIndex, mask);
        final E e = UnsafeRefArrayAccess.lpElement(this.buffer, offset);
        assert e != null;
        UnsafeRefArrayAccess.spElement(this.buffer, offset, null);
        this.soSequence(sBuffer, seqOffset, cIndex + mask + 1L);
        return e;
    }
    
    @Override
    public E peek() {
        E e;
        long cIndex;
        do {
            cIndex = this.lvConsumerIndex();
            e = UnsafeRefArrayAccess.lpElement(this.buffer, this.calcElementOffset(cIndex));
        } while (e == null && cIndex != this.lvProducerIndex());
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
        final long mask = this.mask;
        final long[] sBuffer = this.sequenceBuffer;
        long seq;
        long pIndex;
        long seqOffset;
        do {
            pIndex = this.lvProducerIndex();
            seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(pIndex, mask);
            seq = this.lvSequence(sBuffer, seqOffset);
            if (seq < pIndex) {
                return false;
            }
        } while (seq > pIndex || !this.casProducerIndex(pIndex, pIndex + 1L));
        UnsafeRefArrayAccess.spElement(this.buffer, ConcurrentCircularArrayQueue.calcElementOffset(pIndex, mask), e);
        this.soSequence(sBuffer, seqOffset, pIndex + 1L);
        return true;
    }
    
    @Override
    public E relaxedPoll() {
        final long[] sBuffer = this.sequenceBuffer;
        final long mask = this.mask;
        long seq;
        long expectedSeq;
        long cIndex;
        long seqOffset;
        do {
            cIndex = this.lvConsumerIndex();
            seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(cIndex, mask);
            seq = this.lvSequence(sBuffer, seqOffset);
            expectedSeq = cIndex + 1L;
            if (seq < expectedSeq) {
                return null;
            }
        } while (seq > expectedSeq || !this.casConsumerIndex(cIndex, cIndex + 1L));
        final long offset = ConcurrentCircularArrayQueue.calcElementOffset(cIndex, mask);
        final E e = UnsafeRefArrayAccess.lpElement(this.buffer, offset);
        UnsafeRefArrayAccess.spElement(this.buffer, offset, null);
        this.soSequence(sBuffer, seqOffset, cIndex + mask + 1L);
        return e;
    }
    
    @Override
    public E relaxedPeek() {
        final long currConsumerIndex = this.lvConsumerIndex();
        return UnsafeRefArrayAccess.lpElement(this.buffer, this.calcElementOffset(currConsumerIndex));
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
        final long[] sBuffer = this.sequenceBuffer;
        final long mask = this.mask;
        final E[] buffer = (E[])this.buffer;
        for (int i = 0; i < limit; ++i) {
            long seq;
            long expectedSeq;
            long cIndex;
            long seqOffset;
            do {
                cIndex = this.lvConsumerIndex();
                seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(cIndex, mask);
                seq = this.lvSequence(sBuffer, seqOffset);
                expectedSeq = cIndex + 1L;
                if (seq < expectedSeq) {
                    return i;
                }
            } while (seq > expectedSeq || !this.casConsumerIndex(cIndex, cIndex + 1L));
            final long offset = ConcurrentCircularArrayQueue.calcElementOffset(cIndex, mask);
            final E e = UnsafeRefArrayAccess.lpElement(buffer, offset);
            UnsafeRefArrayAccess.spElement(buffer, offset, (E)null);
            this.soSequence(sBuffer, seqOffset, cIndex + mask + 1L);
            c.accept(e);
        }
        return limit;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s, final int limit) {
        final long[] sBuffer = this.sequenceBuffer;
        final long mask = this.mask;
        final E[] buffer = (E[])this.buffer;
        for (int i = 0; i < limit; ++i) {
            long seq;
            long pIndex;
            long seqOffset;
            do {
                pIndex = this.lvProducerIndex();
                seqOffset = ConcurrentSequencedCircularArrayQueue.calcSequenceOffset(pIndex, mask);
                seq = this.lvSequence(sBuffer, seqOffset);
                if (seq < pIndex) {
                    return i;
                }
            } while (seq > pIndex || !this.casProducerIndex(pIndex, pIndex + 1L));
            UnsafeRefArrayAccess.spElement(buffer, ConcurrentCircularArrayQueue.calcElementOffset(pIndex, mask), s.get());
            this.soSequence(sBuffer, seqOffset, pIndex + 1L);
        }
        return limit;
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy w, final MessagePassingQueue.ExitCondition exit) {
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
    
    static {
        RECOMENDED_POLL_BATCH = Runtime.getRuntime().availableProcessors() * 4;
        RECOMENDED_OFFER_BATCH = Runtime.getRuntime().availableProcessors() * 4;
    }
}
