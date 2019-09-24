package com.datasphere.classloading;

import java.util.*;

import com.datasphere.intf.*;
import com.datasphere.intf.Formatter;

import java.lang.reflect.*;
/*
 * 加载格式化器
 * TODO 待改进
 */
public class FormatterLoader
{
    private static final String FORMATTER_NAME = "handler";
    
    public static Formatter loadFormatter(final Map<String, Object> formatterProperties, final Field[] fields) throws Exception {
        final String formatterClassName = (String)formatterProperties.get("handler");
        final Class<?> formatterClass = Class.forName(formatterClassName, false, ClassLoader.getSystemClassLoader());
        final Formatter formatter = (Formatter)formatterClass.getConstructor(Map.class, Field[].class).newInstance(formatterProperties, fields);
        return formatter;
    }
}
