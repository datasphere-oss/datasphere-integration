package com.datasphere.runtime.utils;

import java.lang.reflect.*;
import java.util.*;

public class NamePolicy
{
    private static NamePolicyImpl policy;
    
    public static Field getField(final Class<?> type, final String fieldName) throws NoSuchFieldException, SecurityException {
        return NamePolicy.policy.getField(type, fieldName);
    }
    
    public static <V> HashMap<String, V> makeNameMap() {
        return (HashMap<String, V>)new CaseInsensitiveHashMap();
    }
    
    public static <V> LinkedHashMap<String, V> makeNameLinkedMap() {
        return (LinkedHashMap<String, V>)new CaseInsensitiveLinkedHashMap();
    }
    
    public static HashSet<String> makeNameSet() {
        return new CaseInsensitiveSet();
    }
    
    public static boolean isEqual(final String name1, final String name2) {
        return NamePolicy.policy.isEqual(name1, name2);
    }
    
    public static String makeKey(final String s) {
        return s.toUpperCase();
    }
    
    static {
        NamePolicy.policy = new IgnoreCaseNamePolicyImpl();
    }
    
    static class DefaultNamePolicyImpl implements NamePolicyImpl
    {
        @Override
        public Field getField(final Class<?> type, final String fieldName) throws NoSuchFieldException, SecurityException {
            final Field ret = type.getField(fieldName);
            return ret;
        }
        
        @Override
        public boolean isEqual(final String name1, final String name2) {
            return name1.equals(name2);
        }
    }
    
    static class IgnoreCaseNamePolicyImpl implements NamePolicyImpl
    {
        @Override
        public Field getField(final Class<?> type, final String fieldName) throws NoSuchFieldException, SecurityException {
            for (final Field f : type.getFields()) {
                if (f.getName().equalsIgnoreCase(fieldName)) {
                    return f;
                }
            }
            throw new NoSuchFieldException(fieldName);
        }
        
        @Override
        public boolean isEqual(final String name1, final String name2) {
            return name1.equalsIgnoreCase(name2);
        }
    }
    
    private static class CaseInsensitiveHashMap<V> extends HashMap<String, V>
    {
        private static final long serialVersionUID = -1456385725381468246L;
        
        @Override
        public V get(final Object key) {
            if (key instanceof String) {
                return super.get(NamePolicy.makeKey((String)key));
            }
            return super.get(key);
        }
        
        @Override
        public boolean containsKey(final Object key) {
            if (key instanceof String) {
                return super.containsKey(NamePolicy.makeKey((String)key));
            }
            return super.containsKey(key);
        }
        
        @Override
        public V remove(final Object key) {
            if (key instanceof String) {
                return super.remove(NamePolicy.makeKey((String)key));
            }
            return super.remove(key);
        }
        
        @Override
        public V put(final String key, final V value) {
            return super.put(NamePolicy.makeKey(key), value);
        }
    }
    
    private static class CaseInsensitiveLinkedHashMap<V> extends LinkedHashMap<String, V>
    {
        private static final long serialVersionUID = -1456385725381468246L;
        
        @Override
        public V get(final Object key) {
            if (key instanceof String) {
                return super.get(NamePolicy.makeKey((String)key));
            }
            return super.get(key);
        }
        
        @Override
        public boolean containsKey(final Object key) {
            if (key instanceof String) {
                return super.containsKey(NamePolicy.makeKey((String)key));
            }
            return super.containsKey(key);
        }
        
        @Override
        public V remove(final Object key) {
            if (key instanceof String) {
                return super.remove(NamePolicy.makeKey((String)key));
            }
            return super.remove(key);
        }
        
        @Override
        public V put(final String key, final V value) {
            return super.put(NamePolicy.makeKey(key), value);
        }
    }
    
    private static class CaseInsensitiveSet extends HashSet<String>
    {
        private static final long serialVersionUID = -1456385725381468246L;
        
        @Override
        public boolean contains(final Object o) {
            if (o instanceof String) {
                return super.contains(NamePolicy.makeKey((String)o));
            }
            return super.contains(o);
        }
        
        @Override
        public boolean remove(final Object o) {
            if (o instanceof String) {
                return super.remove(NamePolicy.makeKey((String)o));
            }
            return super.remove(o);
        }
        
        @Override
        public boolean add(final String key) {
            return super.add(NamePolicy.makeKey(key));
        }
    }
    
    interface NamePolicyImpl
    {
        Field getField(final Class<?> p0, final String p1) throws NoSuchFieldException, SecurityException;
        
        boolean isEqual(final String p0, final String p1);
    }
}
