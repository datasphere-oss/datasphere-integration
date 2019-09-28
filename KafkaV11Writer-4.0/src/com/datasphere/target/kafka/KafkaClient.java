package com.datasphere.target.kafka;

import com.datasphere.recovery.*;
import java.util.concurrent.*;
import com.datasphere.kafka.*;
import com.datasphere.intf.*;
import com.datasphere.uuid.*;

public interface KafkaClient
{
    void initProducerRecord(final String p0, final int p1, final byte[] p2, final byte[] p3);
    
    void asyncSend(final ImmutableStemma p0, final AsyncProducer p1);
    
    long syncSend(final String p0, final int p1, final byte[] p2, final byte[] p3) throws InterruptedException, ExecutionException, TimeoutException;
    
    void closeProducer();
    
    OffsetPosition updatePartitionWaitPosition(final String p0, final int p1, final OffsetPosition p2, final Offset p3, final Offset p4, final Formatter p5, final UUID p6) throws Exception;
    
    long verifyAndGetOffset(final String p0, final int p1, final long p2, final byte[] p3);
    
    Offset getNewOffset();
    
    int getTotalNoofPartitions(final String p0);
    
    String getClientType();
}
