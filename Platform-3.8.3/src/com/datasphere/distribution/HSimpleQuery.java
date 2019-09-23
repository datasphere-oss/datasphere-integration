package com.datasphere.distribution;

import com.datasphere.waction.*;
import java.util.*;

public interface HSimpleQuery<K, V>
{
    List<V> executeQuery(final Queryable<K, V> p0);
    
    void addQueryResults(final Iterable<Waction> p0, final Collection<V> p1);
    
    List<V> getQueryResults();
    
    void mergeQueryResults(final List<V> p0);
}
