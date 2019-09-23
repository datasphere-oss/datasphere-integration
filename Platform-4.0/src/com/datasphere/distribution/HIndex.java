package com.datasphere.distribution;

import com.datasphere.runtime.*;
import java.util.*;

public class HIndex<I, K, V>
{
    Type type;
    Map<I, List<Entry>> index;
    FieldAccessor<I, V> accessor;
    IPartitionManager pm;
    
    public HIndex(final Type type, final FieldAccessor<I, V> accessor, final IPartitionManager pm) {
        this.type = type;
        this.accessor = accessor;
        this.pm = pm;
        
        switch (type) {
        		// 构建HashIndex
            case hash: {
                this.index = new HashMap<I, List<Entry>>();
                break;
            }
            // 构建TreeIndex
            case tree: {
                this.index = new TreeMap<I, List<Entry>>();
                break;
            }
        }
    }
    // 添加索引
    public void add(final int partId, final K key, final V value) {
        final I indexVal = this.accessor.getField(value);
        if (indexVal != null) {
            synchronized (this.index) {
                List<Entry> entries = this.index.get(indexVal);
                if (entries == null) {
                    entries = new ArrayList<Entry>();
                }
                synchronized (entries) {
                    entries.add(new Entry(partId, key, value));
                }
                this.index.put(indexVal, entries);
            }
        }
    }
    // 删除索引
    public void remove(final K key, final V value) {
        final I indexVal = this.accessor.getField(value);
        if (indexVal != null) {
            synchronized (this.index) {
                final List<Entry> entries = this.index.get(indexVal);
                if (entries != null) {
                    synchronized (entries) {
                        final Iterator<Entry> it = entries.iterator();
                        while (it.hasNext()) {
                            final Entry entry = it.next();
                            if (key.equals(entry.key)) {
                                it.remove();
                                break;
                            }
                        }
                    }
                    if (entries.size() == 0) {
                        this.index.remove(indexVal);
                    }
                }
            }
        }
    }
    
    public Map<K, V> getEqual(final I indexVal) {
        final Map<K, V> res = new HashMap<K, V>();
        final List<Entry> entries = this.index.get(indexVal);
        if (entries != null) {
            synchronized (entries) {
                for (final Entry entry : entries) {
                    if (this.pm.isLocalPartitionById(entry.partId, 0)) {
                        res.put(entry.key, entry.value);
                    }
                }
            }
        }
        return res;
    }
    
    public int size() {
        return this.index.size();
    }
    // 获得范围值
    public Map<K, V> getRange(I startVal, I endVal) {
        if (this.type != Type.tree) {
            throw new RuntimeException("Range index queries only possible on tree indexes");
        }
        final Map<K, V> res = new LinkedHashMap<K, V>();
        if (this.index.isEmpty()) {
            return res;
        }
        final TreeMap<I, List<Entry>> tree = (TreeMap<I, List<Entry>>)(TreeMap)this.index;
        if (startVal == null) {
            startVal = tree.firstKey();
        }
        else {
            startVal = tree.ceilingKey(startVal);
        }
        if (endVal == null) {
            endVal = tree.lastKey();
        }
        else {
            endVal = tree.floorKey(endVal);
        }
        if (startVal == null || endVal == null) {
            return res;
        }
        I hashVal = startVal;
        boolean done = false;
        while (!done) {
            final List<Entry> entries = tree.get(hashVal);
            if (entries != null) {
                synchronized (entries) {
                    for (final Entry entry : entries) {
                        if (this.pm.isLocalPartitionById(entry.partId, 0)) {
                            res.put(entry.key, entry.value);
                        }
                    }
                }
            }
            if (endVal.equals(hashVal)) {
                done = true;
            }
            else {
                hashVal = tree.higherKey(hashVal);
                if (hashVal != null) {
                    continue;
                }
                done = true;
            }
        }
        return res;
    }
    
    public void clear() {
        this.index.clear();
    }
    
    public enum Type
    {
        hash, 
        tree;
    }
    
    public class Entry
    {
        final int partId;
        final K key;
        final V value;
        
        public Entry(final int partId, final K key, final V value) {
            this.partId = partId;
            this.key = key;
            this.value = value;
        }
        
        @Override
        public boolean equals(final Object obj) {
            return this.key.equals(obj);
        }
        
        @Override
        public int hashCode() {
            return this.key.hashCode();
        }
    }
    
    public interface FieldAccessor<I, V>
    {
        I getField(final V p0);
    }
}
