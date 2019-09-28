package com.datasphere.target.kafka;

import com.datasphere.kafka.*;
import org.apache.kafka.common.utils.*;

public class DefaultKWPartitioner implements PartitionerIntf
{
    public int partition(final String topic, final Object keylist, final Object event, final int noOfPartitions) {
        if (keylist != null) {
            return Utils.abs(keylist.hashCode()) % noOfPartitions;
        }
        return 0;
    }
    
    public void close() {
    }
}