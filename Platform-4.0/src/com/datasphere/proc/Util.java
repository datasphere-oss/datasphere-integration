package com.datasphere.proc;

import java.util.*;

class Util
{
    public static Random nRandom;
    
    public static String ranString(final Random rng, final String characters, final int length) {
        final char[] ranStr = new char[length];
        for (int i = 0; i < length; ++i) {
            ranStr[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(ranStr);
    }
    
    public static int ranInt(final Random a, final int start, final int end) {
        final int range = end - start + 1;
        final double frac = range * a.nextDouble();
        final int ranNumber = (int)(frac + start);
        return ranNumber;
    }
    
    static {
        Util.nRandom = new Random();
    }
}
