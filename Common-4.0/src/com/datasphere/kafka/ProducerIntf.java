package com.datasphere.kafka;

import java.util.*;

public interface ProducerIntf
{
    long write(final String p0, final int p1, final byte[] p2, final String p3, final List<String> p4, final long p5, final int p6) throws KafkaException;
    
    void close();
}
