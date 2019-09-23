package org.jctools.queues;

import org.jctools.util.*;

public final class CircularArrayOffsetCalculator
{
    static final long REF_ARRAY_BASE;
    static final int REF_ELEMENT_SHIFT;
    
    public static <E> E[] allocate(final int capacity) {
        return (E[])new Object[capacity];
    }
    
    public static long calcElementOffset(final long index, final long mask) {
        return CircularArrayOffsetCalculator.REF_ARRAY_BASE + ((index & mask) << CircularArrayOffsetCalculator.REF_ELEMENT_SHIFT);
    }
    
    static {
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
    }
}
