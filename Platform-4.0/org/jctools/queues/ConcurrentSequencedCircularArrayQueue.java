package org.jctools.queues;

import org.jctools.util.*;

public abstract class ConcurrentSequencedCircularArrayQueue<E> extends ConcurrentCircularArrayQueue<E>
{
    private static final long ARRAY_BASE;
    private static final int ELEMENT_SHIFT;
    protected static final int SEQ_BUFFER_PAD;
    protected final long[] sequenceBuffer;
    
    public ConcurrentSequencedCircularArrayQueue(final int capacity) {
        super(capacity);
        final int actualCapacity = (int)(this.mask + 1L);
        this.sequenceBuffer = new long[(actualCapacity << SparsePaddedCircularArrayOffsetCalculator.SPARSE_SHIFT) + ConcurrentSequencedCircularArrayQueue.SEQ_BUFFER_PAD * 2];
        for (long i = 0L; i < actualCapacity; ++i) {
            this.soSequence(this.sequenceBuffer, this.calcSequenceOffset(i), i);
        }
    }
    
    protected final long calcSequenceOffset(final long index) {
        return calcSequenceOffset(index, this.mask);
    }
    
    protected static final long calcSequenceOffset(final long index, final long mask) {
        return ConcurrentSequencedCircularArrayQueue.ARRAY_BASE + ((index & mask) << ConcurrentSequencedCircularArrayQueue.ELEMENT_SHIFT);
    }
    
    protected final void soSequence(final long[] buffer, final long offset, final long e) {
        UnsafeAccess.UNSAFE.putOrderedLong(buffer, offset, e);
    }
    
    protected final long lvSequence(final long[] buffer, final long offset) {
        return UnsafeAccess.UNSAFE.getLongVolatile(buffer, offset);
    }
    
    static {
        final int scale = UnsafeAccess.UNSAFE.arrayIndexScale(long[].class);
        if (8 == scale) {
            ELEMENT_SHIFT = 3 + SparsePaddedCircularArrayOffsetCalculator.SPARSE_SHIFT;
            SEQ_BUFFER_PAD = JvmInfo.CACHE_LINE_SIZE * 2 / scale;
            ARRAY_BASE = UnsafeAccess.UNSAFE.arrayBaseOffset(long[].class) + ConcurrentSequencedCircularArrayQueue.SEQ_BUFFER_PAD * scale;
            return;
        }
        throw new IllegalStateException("Unexpected long[] element size");
    }
}
