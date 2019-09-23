package org.jctools.queues;

abstract class SpscUnboundedArrayQueueProducerFields<E> extends SpscUnboundedArrayQueueProducerColdFields<E>
{
    protected long producerIndex;
}
