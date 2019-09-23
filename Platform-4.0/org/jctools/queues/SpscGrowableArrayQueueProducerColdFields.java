package org.jctools.queues;

abstract class SpscGrowableArrayQueueProducerColdFields<E> extends SpscGrowableArrayQueuePrePad<E>
{
    protected int maxQueueCapacity;
    protected int producerLookAheadStep;
    protected long producerLookAhead;
    protected long producerMask;
    protected E[] producerBuffer;
}
