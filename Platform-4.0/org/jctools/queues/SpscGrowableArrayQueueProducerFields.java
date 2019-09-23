package org.jctools.queues;

abstract class SpscGrowableArrayQueueProducerFields<E> extends SpscGrowableArrayQueueProducerColdFields<E>
{
    protected long producerIndex;
}
