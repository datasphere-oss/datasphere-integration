package com.datasphere.distribution;

import java.util.*;
import java.io.*;

public interface HQuery<K, V>
{
    boolean usesSingleKey();
    
    K getSingleKey();
    
    void setSingleKey(final K p0);
    
    boolean usesPartitionKey();
    
    Object getPartitionKey();
    
    ResultStats getResultStats(final Queryable<K, V> p0);
    
    void run(final Queryable<K, V> p0);
    
    void runOne(final V p0);
    
    Map<K, Map<String, Object>> getResults();
    
    void mergeResults(final Map<K, Map<String, Object>> p0);
    
    public static class ResultStats implements Serializable
    {
        private static final long serialVersionUID = -798485429514059103L;
        public long startTime;
        public long endTime;
        public long count;
        
        @Override
        public String toString() {
            return "{startTime:" + this.startTime + ", endTime:" + this.endTime + ", count:" + this.count + "}";
        }
    }
}
