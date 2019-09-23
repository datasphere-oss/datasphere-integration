package com.datasphere.messaging;

public class Constants
{
    public static final String TCP = "tcp://";
    public static final String IPC = "ipc://";
    public static final String INPROC = "inproc://";
    public static final String SEPARATOR = ":";
    public static final String HYPHENSEPARATOR = "-";
    public static final long NANOSECONDS_PER_SECOND = 1000000000L;
    public static final int MB = 1048576;
    public static final String NOVAL = "NOVAL";
    
    public static boolean toBoolean(final byte[] data) {
        return data != null && data.length != 0 && data[0] != 0;
    }
    
    public static float ns2s(final long nsecs) {
        final float f = nsecs;
        return f / 1.0E9f;
    }
    
    public static float b2mb(final long bytes) {
        final float bb = bytes;
        final float ll = 1048576.0f;
        return bb / ll;
    }
    
    public static String stringIndexer(final String ss, final String delimiter, final int start, final int end) {
        if (ss == null) {
            throw new IllegalArgumentException("Can't Index into a NULL String");
        }
        if (delimiter == null) {
            throw new NullPointerException("Can't take NULL as delimiter");
        }
        if (start < 0 || end < 0 || start > ss.length() || end > ss.length()) {
            throw new ArrayIndexOutOfBoundsException("Can't Index to the String, Please check start and end values");
        }
        final int firstIndex = ss.indexOf(":");
        final int lastIndex = ss.lastIndexOf(":");
        return ss.substring(firstIndex + 1, lastIndex);
    }
}
