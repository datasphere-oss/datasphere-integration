package com.datasphere.uuid;

import java.io.*;

public final class Hex
{
    private static final char[] DIGITS;
    
    public static Appendable append(final Appendable a, final short in) {
        return append(a, (long)in, 4);
    }
    
    public static Appendable append(final Appendable a, final short in, final int length) {
        return append(a, (long)in, length);
    }
    
    public static Appendable append(final Appendable a, final int in) {
        return append(a, (long)in, 8);
    }
    
    public static Appendable append(final Appendable a, final int in, final int length) {
        return append(a, (long)in, length);
    }
    
    public static Appendable append(final Appendable a, final long in) {
        return append(a, in, 16);
    }
    
    public static Appendable append(final Appendable a, final long in, final int length) {
        try {
            for (int lim = (length << 2) - 4; lim >= 0; lim -= 4) {
                a.append(Hex.DIGITS[(byte)(in >> lim) & 0xF]);
            }
        }
        catch (IOException ex) {}
        return a;
    }
    
    public static Appendable append(final Appendable a, final byte[] bytes) {
        try {
            for (final byte b : bytes) {
                a.append(Hex.DIGITS[(byte)((b & 0xF0) >> 4)]);
                a.append(Hex.DIGITS[(byte)(b & 0xF)]);
            }
        }
        catch (IOException ex) {}
        return a;
    }
    
    public static long parseLong(final CharSequence s) {
        long out = 0L;
        byte shifts = 0;
        for (int i = 0; i < s.length() && shifts < 16; ++i) {
            final char c = s.charAt(i);
            if (c > '/' && c < ':') {
                ++shifts;
                out <<= 4;
                out |= c - '0';
            }
            else if (c > '@' && c < 'G') {
                ++shifts;
                out <<= 4;
                out |= c - '7';
            }
            else if (c > '`' && c < 'g') {
                ++shifts;
                out <<= 4;
                out |= c - 'W';
            }
        }
        return out;
    }
    
    public static short parseShort(final String s) {
        short out = 0;
        byte shifts = 0;
        for (int i = 0; i < s.length() && shifts < 4; ++i) {
            final char c = s.charAt(i);
            if (c > '/' && c < ':') {
                ++shifts;
                out <<= 4;
                out |= (short)(c - '0');
            }
            else if (c > '@' && c < 'G') {
                ++shifts;
                out <<= 4;
                out |= (short)(c - '7');
            }
            else if (c > '`' && c < 'g') {
                ++shifts;
                out <<= 4;
                out |= (short)(c - 'W');
            }
        }
        return out;
    }
    
    static {
        DIGITS = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    }
}
