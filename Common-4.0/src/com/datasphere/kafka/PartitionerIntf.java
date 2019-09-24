package com.datasphere.kafka;
/*
 * 分区接口
 */
public interface PartitionerIntf
{
    int partition(final String p0, final Object p1, final Object p2, final int p3);
    
    void close();
}
