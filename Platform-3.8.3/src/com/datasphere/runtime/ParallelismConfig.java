package com.datasphere.runtime;

public class ParallelismConfig
{
    final String parallelismKey;
    final int parallelismFactor;
    
    public ParallelismConfig(final String parallelismKey, final int parallelismFactor) {
        this.parallelismKey = parallelismKey;
        this.parallelismFactor = parallelismFactor;
    }
    
    public String getParallelismKey() {
        return this.parallelismKey;
    }
    
    public int getParallelismFactor() {
        return this.parallelismFactor;
    }
}
