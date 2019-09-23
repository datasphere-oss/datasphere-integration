package org.jctools.queues;

abstract class SpscUnboundedArrayQueueConsumerColdField<E> extends SpscUnboundedArrayQueueL2Pad<E>
{
    protected long consumerMask;
    protected E[] consumerBuffer;
}
