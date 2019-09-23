package org.jctools.queues;

import org.jctools.util.*;

public final class PaddedCircularArrayOffsetCalculator
{
    static final int REF_BUFFER_PAD;
    static final long REF_ARRAY_BASE;
    
    public static <E> E[] allocate(final int capacity) {
        return (E[])new Object[capacity + PaddedCircularArrayOffsetCalculator.REF_BUFFER_PAD * 2];
    }
    
    protected static long calcElementOffset(final long index, final long mask) {
        return PaddedCircularArrayOffsetCalculator.REF_ARRAY_BASE + ((index & mask) << CircularArrayOffsetCalculator.REF_ELEMENT_SHIFT);
    }
    
    static {
        REF_BUFFER_PAD = JvmInfo.CACHE_LINE_SIZE * 2 >> CircularArrayOffsetCalculator.REF_ELEMENT_SHIFT;
        final int paddingOffset = PaddedCircularArrayOffsetCalculator.REF_BUFFER_PAD << CircularArrayOffsetCalculator.REF_ELEMENT_SHIFT;
        REF_ARRAY_BASE = CircularArrayOffsetCalculator.REF_ARRAY_BASE + paddingOffset;
    }
}
