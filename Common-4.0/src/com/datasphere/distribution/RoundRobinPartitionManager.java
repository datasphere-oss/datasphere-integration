package com.datasphere.distribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datasphere.uuid.UUID;
/*
 * 采用RoundRobin模式对任务或数据进行分区
 */
public class RoundRobinPartitionManager
{
    private List<UUID> buckets;
    private Integer nextBucketIndex;
    
    public RoundRobinPartitionManager(final List<UUID> servers) {
        this.nextBucketIndex = -1;
        (this.buckets = new ArrayList<UUID>(servers.size())).addAll(servers);
        Collections.sort(this.buckets);
    }
    
    public UUID getOwnerForPartition() {
        if (this.nextBucketIndex == this.buckets.size() - 1) {
            this.nextBucketIndex = 0;
        }
        else {
            final Integer nextBucketIndex = this.nextBucketIndex;
            ++this.nextBucketIndex;
        }
        return this.buckets.get(this.nextBucketIndex);
    }
}
