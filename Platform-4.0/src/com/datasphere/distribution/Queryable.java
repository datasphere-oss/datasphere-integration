package com.datasphere.distribution;

import java.util.*;
/*
 * 查询接口
 */
public interface Queryable<K, V>
{
    Map<K, Map<String, Object>> query(final HQuery<K, V> p0);
    
    void simpleQuery(final HSimpleQuery<K, V> p0);
    
    HQuery.ResultStats queryStats(final HQuery<K, V> p0);
    
     <I> Map<K, V> getIndexedRange(final String p0, final I p1, final I p2);
    
     <I> Map<K, V> getIndexedEqual(final String p0, final I p1);
    
    V get(final K p0);
    
    Set<K> localKeys();
    
    Map<K, V> localEntries();
}
