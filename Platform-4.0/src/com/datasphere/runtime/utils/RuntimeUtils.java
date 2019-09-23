package com.datasphere.runtime.utils;

import com.datasphere.uuid.*;

public class RuntimeUtils
{
    public static String genRandomName(final String prefix) {
        final long a = UUIDGen.newTime();
        final long b = UUIDGen.getClockSeqAndNode();
        return ((prefix == null) ? "X" : prefix) + Long.toHexString(a) + Long.toHexString(b);
    }
}
