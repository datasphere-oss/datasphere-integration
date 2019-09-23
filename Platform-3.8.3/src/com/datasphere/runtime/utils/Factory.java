package com.datasphere.runtime.utils;

import java.util.*;

public class Factory
{
    public static <K, V> HashMap<K, V> makeMap() {
        return new HashMap<K, V>();
    }
    
    public static TreeMap<String, Object> makeCaseInsensitiveMap() {
        return new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
    }
    
    public static <K, V> LinkedHashMap<K, V> makeLinkedMap() {
        return new LinkedHashMap<K, V>();
    }
    
    public static <V> HashMap<String, V> makeNameMap() {
        return NamePolicy.makeNameMap();
    }
    
    public static <V> LinkedHashMap<String, V> makeNameLinkedMap() {
        return NamePolicy.makeNameLinkedMap();
    }
    
    public static HashSet<String> makeNameSet() {
        return NamePolicy.makeNameSet();
    }
    
    public static <T> List<T> makeList() {
        return new ArrayList<T>();
    }
}
