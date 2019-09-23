package com.datasphere.exceptionhandling;

import com.datasphere.runtime.*;
import java.util.*;

public enum ExceptionType
{
    ArithmeticException, 
    NumberFormatException, 
    ClassCastException, 
    InvalidDataException, 
    NullPointerException, 
    SystemException, 
    ConnectionException, 
    UnknownException;
    
    public static ExceptionType getType(final String val) {
        for (final ExceptionType et : values()) {
            if (et.name().equalsIgnoreCase(val)) {
                return et;
            }
        }
        return ExceptionType.UnknownException;
    }
    
    public static ExceptionType getExceptionType(final Exception ex) {
        if (ex == null) {
            return ExceptionType.UnknownException;
        }
        final String name = ex.getClass().getSimpleName();
        return getType(name);
    }
    
    public static boolean contains(final String val) {
        for (final ExceptionType et : values()) {
            if (et.name().equalsIgnoreCase(val)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean validate(final Map<String, Object> ehandlers) {
        if (ehandlers == null || ehandlers.isEmpty()) {
            return true;
        }
        for (final String et : ehandlers.keySet()) {
            if (!contains(et)) {
                return false;
            }
            final String val = (String)ehandlers.get(et);
            if (!ActionType.contains(val)) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean isDataException(final Throwable ex) {
        if (ex == null) {
            return false;
        }
        String str = ex.getClass().getSimpleName();
        final int ik = str.lastIndexOf(".");
        if (ik < 0) {
            str = str.substring(0, str.length());
        }
        else {
            str = str.substring(str.lastIndexOf(".") + 1, str.length());
        }
        return contains(str);
    }
}
