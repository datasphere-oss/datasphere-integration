package com.datasphere.runtime.converters;

import java.util.*;
import java.text.*;
import java.util.concurrent.*;

public class DateConverter
{
    private static Map<String, SimpleDateFormat> sdfs;
    
    public static final Date stringToDate(final String value, final String format) {
        SimpleDateFormat sdf = DateConverter.sdfs.get(format);
        if (sdf == null) {
            sdf = new SimpleDateFormat(format);
            sdf.setLenient(true);
            DateConverter.sdfs.put(format, sdf);
        }
        try {
            return sdf.parse(value);
        }
        catch (ParseException e) {
            return null;
        }
    }
    
    public static final String dateToString(final Date value, final String format) {
        SimpleDateFormat sdf = DateConverter.sdfs.get(format);
        if (sdf == null) {
            sdf = new SimpleDateFormat(format);
            sdf.setLenient(true);
            DateConverter.sdfs.put(format, sdf);
        }
        return sdf.format(value);
    }
    
    public static final int stringToInt(final String value) {
        return Integer.parseInt(value);
    }
    
    public static final double stringToDouble(final String value) {
        return Double.parseDouble(value);
    }
    
    static {
        DateConverter.sdfs = new ConcurrentHashMap<String, SimpleDateFormat>();
    }
}
