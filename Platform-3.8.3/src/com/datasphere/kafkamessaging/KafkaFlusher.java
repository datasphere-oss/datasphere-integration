package com.datasphere.kafkamessaging;

import com.datasphere.kafka.*;
import com.datasphere.runtime.components.*;
import java.util.*;

public class KafkaFlusher implements Runnable
{
    private final Stream streamRuntime;
    List<PositionedBuffer> partitions;
    
    public KafkaFlusher(final Stream streamRuntime) {
        this.partitions = new ArrayList<PositionedBuffer>();
        this.streamRuntime = streamRuntime;
    }
    
    @Override
    public void run() {
        try {
            for (final PositionedBuffer dataBuffer : this.partitions) {
                dataBuffer.flushToKafka();
            }
        }
        catch (Exception e) {
            this.streamRuntime.notifyAppMgr(EntityType.STREAM, this.streamRuntime.getMetaFullName(), this.streamRuntime.getMetaID(), e, null, new Object[0]);
        }
    }
    
    public void add(final PositionedBuffer dataBuffer) {
        this.partitions.add(dataBuffer);
    }
}
