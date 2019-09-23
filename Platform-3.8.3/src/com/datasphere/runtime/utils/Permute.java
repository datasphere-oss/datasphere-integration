package com.datasphere.runtime.utils;

import java.util.*;

public class Permute
{
    private static <T> void makePermutations(final List<List<T>> res, final T[] buf, final List<T> input, final BitSet index, final int inputSize) {
        final int pos = index.cardinality();
        if (pos == inputSize) {
            res.add(new ArrayList<T>((Collection<? extends T>)Arrays.asList(buf)));
            return;
        }
        for (int i = 0; (i = index.nextClearBit(i)) >= 0 && i < inputSize; ++i) {
            buf[pos] = input.get(i);
            index.set(i);
            makePermutations(res, buf, input, index, inputSize);
            index.clear(i);
        }
    }
    
    public static <T> List<List<T>> makePermutations(final List<T> in) {
        final List<List<T>> res = new ArrayList<List<T>>();
        final int size = in.size();
        makePermutations(res, new Object[size], in, new BitSet(size), size);
        return res;
    }
    
    @SafeVarargs
    public static <T> void printPermutations(final T... in) {
        int i = 0;
        for (final List<T> res : makePermutations(Arrays.asList((T[])in))) {
            System.out.println(String.format("%3d", ++i) + " " + res);
        }
        System.out.println("-------------------");
    }
    
    public static void main(final String[] args) {
        printPermutations(1, 2);
        printPermutations("1_", "2_", "3_");
        printPermutations(1.0, 2.0, 3.0, 4.1);
    }
}
