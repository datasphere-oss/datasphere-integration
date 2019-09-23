package org.jctools.queues;

public final class SparsePaddedCircularArrayOffsetCalculator
{
    static final int SPARSE_SHIFT;
    private static final long REF_ARRAY_BASE;
    private static final int REF_ELEMENT_SHIFT;
    
    public static <E> E[] allocate(final int capacity) {
        return (E[])new Object[(capacity << SparsePaddedCircularArrayOffsetCalculator.SPARSE_SHIFT) + PaddedCircularArrayOffsetCalculator.REF_BUFFER_PAD * 2];
    }
    
    public static long calcElementOffset(final long index, final long mask) {
        return SparsePaddedCircularArrayOffsetCalculator.REF_ARRAY_BASE + ((index & mask) << SparsePaddedCircularArrayOffsetCalculator.REF_ELEMENT_SHIFT);
    }
    
    static {
        SPARSE_SHIFT = Integer.getInteger("org.jctools.sparse.shift", 0);
        REF_ELEMENT_SHIFT = CircularArrayOffsetCalculator.REF_ELEMENT_SHIFT + SparsePaddedCircularArrayOffsetCalculator.SPARSE_SHIFT;
        REF_ARRAY_BASE = PaddedCircularArrayOffsetCalculator.REF_ARRAY_BASE;
    }
}
