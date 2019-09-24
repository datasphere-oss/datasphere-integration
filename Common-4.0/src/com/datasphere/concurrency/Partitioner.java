package com.datasphere.concurrency;
/*
 * 获得某个分区的数据桶
 */
public interface Partitioner
{
    Object getBucket();
}
