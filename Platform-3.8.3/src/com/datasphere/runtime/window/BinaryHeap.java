package com.datasphere.runtime.window;

import java.util.*;

public class BinaryHeap<T>
{
    private final List<Item<T>> heap;
    
    public BinaryHeap() {
        this.heap = new ArrayList<Item<T>>();
    }
    
    public void push(final Item<T> it) {
        it.index = this.heap.size();
        this.heap.add(it);
        this.pushUp(it.index);
    }
    
    public Item<T> pop() {
        if (this.heap.size() > 0) {
            this.swap(0, this.heap.size() - 1);
            final Item<T> result = this.heap.remove(this.heap.size() - 1);
            this.pushDown(0);
            return result;
        }
        return null;
    }
    
    public Item<T> getFirst() {
        return this.get(0);
    }
    
    public Item<T> get(final int index) {
        return this.heap.get(index);
    }
    
    public int size() {
        return this.heap.size();
    }
    
    public boolean isEmpty() {
        return this.heap.isEmpty();
    }
    
    private boolean isKeyGreaterOrEqual(final int first, final int last) {
        return isGreaterOrEqual(this.get(first).key, this.get(last).key);
    }
    
    private static boolean isGreaterOrEqual(final long key1, final long key2) {
        return key2 >= key1;
    }
    
    private int parent(final int i) {
        return (i - 1) / 2;
    }
    
    private int left(final int i) {
        return 2 * i + 1;
    }
    
    private int right(final int i) {
        return 2 * i + 2;
    }
    
    private void swap(final int i, final int j) {
        final Item<T> tmpi = this.heap.get(i);
        final Item<T> tmpj = this.heap.get(j);
        tmpj.index = i;
        this.heap.set(i, tmpj);
        tmpi.index = j;
        this.heap.set(j, tmpi);
    }
    
    public void check(final int i) {
        if (this.heap.get(i).index != i) {
            throw new RuntimeException("index error");
        }
        final Item<T> val = this.get(i);
        final int left = this.left(i);
        if (left < this.heap.size()) {
            final Item<T> lval = this.get(left);
            if (!isGreaterOrEqual(val.key, lval.key)) {
                throw new RuntimeException("broken heap");
            }
            this.check(left);
        }
        final int right = this.right(i);
        if (right < this.heap.size()) {
            final Item<T> rval = this.get(right);
            if (!isGreaterOrEqual(val.key, rval.key)) {
                throw new RuntimeException("broken heap");
            }
            this.check(right);
        }
    }
    
    private void pushDown(final int i) {
        final int left = this.left(i);
        final int right = this.right(i);
        int largest = i;
        if (left < this.heap.size() && !this.isKeyGreaterOrEqual(largest, left)) {
            largest = left;
        }
        if (right < this.heap.size() && !this.isKeyGreaterOrEqual(largest, right)) {
            largest = right;
        }
        if (largest != i) {
            this.swap(largest, i);
            this.pushDown(largest);
        }
    }
    
    private void pushUp(int i) {
        while (i > 0 && !this.isKeyGreaterOrEqual(this.parent(i), i)) {
            this.swap(this.parent(i), i);
            i = this.parent(i);
        }
    }
    
    public void changePriority(final Item<T> it, final long newkey) {
        final long oldkey = it.key;
        it.key = newkey;
        if (!isGreaterOrEqual(newkey, oldkey)) {
            this.pushDown(it.index);
        }
        else {
            this.pushUp(it.index);
        }
    }
    
    @Override
    public String toString() {
        final StringBuffer s = new StringBuffer("Heap:\n");
        int rowStart = 0;
        int rowSize = 1;
        for (int i = 0; i < this.heap.size(); ++i) {
            if (i == rowStart + rowSize) {
                s.append('\n');
                rowStart = i;
                rowSize *= 2;
            }
            final Item<T> it = this.heap.get(i);
            assert it.index == i;
            s.append(it);
            s.append(" ");
        }
        return s.toString();
    }
    
    public List<T> toList() {
        final List<T> l = new ArrayList<T>();
        for (final Item<T> it : this.heap) {
            l.add(it.value);
        }
        return l;
    }
    
    public static class Item<T>
    {
        final T value;
        long key;
        int index;
        
        Item(final T value, final long key) {
            this.value = value;
            this.key = key;
        }
        
        @Override
        public String toString() {
            return this.key + ":" + this.value;
        }
    }
}
