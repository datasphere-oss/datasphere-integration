package org.jctools.queues;

public interface QueueProgressIndicators
{
    long currentProducerIndex();
    
    long currentConsumerIndex();
}
