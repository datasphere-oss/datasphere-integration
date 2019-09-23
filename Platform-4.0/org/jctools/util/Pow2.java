package org.jctools.util;

public final class Pow2
{
    public static int roundToPowerOfTwo(final int value) {
        return 1 << 32 - Integer.numberOfLeadingZeros(value - 1);
    }
    
    public static boolean isPowerOfTwo(final int value) {
        return (value & value - 1) == 0x0;
    }
    
    public static long align(final long value, final int alignment) {
        if (!isPowerOfTwo(alignment)) {
            throw new IllegalArgumentException("alignment must be a power of 2:" + alignment);
        }
        return value + (alignment - 1) & ~(alignment - 1);
    }
}
