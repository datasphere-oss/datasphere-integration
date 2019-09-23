package com.datasphere.runtime.utils;

import java.util.*;

public class NameHelper
{
    public static String toString(final List<String> list) {
        return StringUtils.join((List)list, ".");
    }
    
    public static String getPrefix(final String name) {
        final int dot = name.lastIndexOf(46);
        if (dot == -1) {
            return "";
        }
        return name.substring(0, dot);
    }
    
    public static String getBasename(final String name) {
        final int dot = name.lastIndexOf(46);
        if (dot == -1) {
            return name;
        }
        return name.substring(dot + 1);
    }
}
