package org.jctools.queues;

abstract class SpscUnboundedArrayQueueConsumerField<E> extends SpscUnboundedArrayQueueConsumerColdField<E>
{
    protected long consumerIndex;
}
