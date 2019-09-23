package org.jctools.queues;

abstract class SpscUnboundedArrayQueueProducerColdFields<E> extends SpscUnboundedArrayQueuePrePad<E>
{
    protected int producerLookAheadStep;
    protected long producerLookAhead;
    protected long producerMask;
    protected E[] producerBuffer;
}
