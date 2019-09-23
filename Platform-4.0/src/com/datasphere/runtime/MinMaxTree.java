package com.datasphere.runtime;

import org.apache.log4j.*;
import java.util.*;

class MinMaxTree<K> extends TreeMap<K, Long>
{
    private static Logger logger;
    private static final long serialVersionUID = 2045578403717126481L;
    
    public void addKey(final K key) {
        if (key == null) {
            return;
        }
        final Long n = this.get(key);
        if (n == null) {
            this.put(key, 1L);
        }
        else {
            this.put(key, n + 1L);
        }
    }
    
    public void removeKey(final K key) {
        if (key == null) {
            return;
        }
        final Long n = this.get(key);
        if (n != null) {
            if (n == 1L) {
                this.remove(key);
            }
            else {
                this.put(key, n - 1L);
            }
        }
        else {
            MinMaxTree.logger.warn((Object)"remove key from MinMaxTree which has not been added");
        }
    }
    
    public K getMin() {
        if (this.size() > 0) {
            return this.firstKey();
        }
        return null;
    }
    
    public K getMax() {
        if (this.size() > 0) {
            return this.lastKey();
        }
        return null;
    }
    
    public K getMaxOccurs() {
        if (this.size() == 0) {
            return null;
        }
        long maxCount = 0L;
        K maxKey = null;
        for (final Map.Entry<K, Long> entry : this.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxKey = entry.getKey();
            }
        }
        return maxKey;
    }
    
    static {
        MinMaxTree.logger = Logger.getLogger((Class)MinMaxTree.class);
    }
}
