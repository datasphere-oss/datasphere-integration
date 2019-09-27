package com.datasphere.source.lib.utils;

public class ByteUtil
{
    public static int bytesToShort(final byte[] bytes, final int startPos) {
        return ((bytes[startPos] & 0xFF) << 8) + (bytes[startPos + 1] & 0xFF);
    }
}
