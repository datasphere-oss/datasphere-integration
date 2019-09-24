package com.datasphere.distribution;
/*
 * 获得分区的基本信息
 */

public interface Partitionable
{
    public static final int NUM_PARTITIONS = 1023;
    
    boolean usePartitionId();
    
    Object getPartitionKey();
    
    int getPartitionId();
}
