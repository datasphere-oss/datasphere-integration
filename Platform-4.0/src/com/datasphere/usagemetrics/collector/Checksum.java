package com.datasphere.usagemetrics.collector;

import org.apache.log4j.*;
import java.lang.reflect.*;
import java.security.*;
import javax.persistence.*;
import java.lang.annotation.*;
import com.datasphere.uuid.*;
import java.util.*;
import java.io.*;

public class Checksum
{
    private static final byte[] BAD_ALGORITHM;
    private static final byte[] INVALID_CLASS;
    private static final String NULL_FIELD_VALUE = "<null>";
    private static final Logger logger;
    private static final String HASH_ALGORITHM = "MD5";
    private final byte[] checksum;
    
    public Checksum(final Object object) {
        final Class objectClass = object.getClass();
        final List<Method> columnMethods = getColumnMethods(objectClass);
        if (columnMethods.isEmpty()) {
            Checksum.logger.error((Object)("Class '" + objectClass.getClass().getName() + "' does not contain any @Column members"));
            this.checksum = Checksum.INVALID_CLASS;
            return;
        }
        final String fieldData = getFieldData(columnMethods, object);
        final byte[] fieldBytes = fieldData.getBytes();
        this.checksum = calculateChecksum(fieldBytes, "MD5");
    }
    
    private static String getFieldData(final Iterable<Method> methods, final Object object) {
        final StringBuilder result = new StringBuilder(256);
        for (final Method method : methods) {
            final String fieldValue = getFieldValue(method, object);
            result.append(fieldValue);
        }
        return result.toString();
    }
    
    private static String getFieldValue(final Method method, final Object object) {
        Object fieldValue = null;
        try {
            fieldValue = method.invoke(object, new Object[0]);
        }
        catch (InvocationTargetException ex) {}
        catch (IllegalArgumentException ex2) {}
        catch (IllegalAccessException ex3) {}
        return (fieldValue != null) ? fieldValue.toString() : "<null>";
    }
    
    private static byte[] calculateChecksum(final byte[] fieldData, final String algorithm) {
        try {
            final MessageDigest digest = MessageDigest.getInstance(algorithm);
            return digest.digest(fieldData);
        }
        catch (NoSuchAlgorithmException ignored) {
            Checksum.logger.error((Object)("MessageDigest algorithm '" + algorithm + "' not available"));
            return Checksum.BAD_ALGORITHM;
        }
    }
    
    private static List<Method> getColumnMethods(final Class objectClass) {
        final Method[] declaredMethods = objectClass.getDeclaredMethods();
        final List<Method> columnMethods = new ArrayList<Method>(declaredMethods.length);
        for (final Method method : declaredMethods) {
            addIfColumnMethod(method, columnMethods);
        }
        Collections.sort(columnMethods, new MethodComparator());
        return columnMethods;
    }
    
    private static void addIfColumnMethod(final Method method, final Collection<Method> columnMethods) {
        final String methodName = method.getName();
        if (method.isAnnotationPresent((Class<? extends Annotation>)Column.class) && !"getChecksum".equals(methodName)) {
            columnMethods.add(method);
        }
    }
    
    private static String hexString(final byte[] bytes) {
        final StringBuffer result = new StringBuffer(40);
        for (final byte item : bytes) {
            Hex.append((Appendable)result, (short)item, 2);
        }
        return result.toString();
    }
    
    @Override
    public String toString() {
        return hexString(this.checksum);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(this.checksum);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        final Checksum other = (Checksum)obj;
        return Arrays.equals(this.checksum, other.checksum);
    }
    
    static {
        BAD_ALGORITHM = "Wrong algorithm.".getBytes();
        INVALID_CLASS = "Class not valid.".getBytes();
        logger = Logger.getLogger((Class)Checksum.class);
    }
    
    private static class MethodComparator implements Comparator<Method>, Serializable
    {
        private static final long serialVersionUID = 4922952488464579560L;
        
        @Override
        public int compare(final Method o1, final Method o2) {
            final String name1 = o1.getName();
            final String name2 = o2.getName();
            return name1.compareTo(name2);
        }
    }
}
