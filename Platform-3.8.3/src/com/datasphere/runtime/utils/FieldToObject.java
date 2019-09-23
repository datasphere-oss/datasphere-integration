package com.datasphere.runtime.utils;

public class FieldToObject
{
    public static Object convert(final Object obj) {
        return obj;
    }
    
    public static Object convert(final boolean obj) {
        return obj;
    }
    
    public static Object convert(final byte obj) {
        return obj;
    }
    
    public static Object convert(final char obj) {
        return obj;
    }
    
    public static Object convert(final short obj) {
        return obj;
    }
    
    public static Object convert(final long obj) {
        return obj;
    }
    
    public static Object convert(final int obj) {
        return obj;
    }
    
    public static Object convert(final float obj) {
        return obj;
    }
    
    public static Object convert(final double obj) {
        return obj;
    }
    
    public static String genConvert(final String arg) {
        return FieldToObject.class.getName() + ".convert(" + arg + ")";
    }
}
