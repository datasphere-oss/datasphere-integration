package org.jctools.queues;

abstract class SpscGrowableArrayQueueConsumerFields<E> extends SpscGrowableArrayQueueL2Pad<E>
{
    protected long consumerMask;
    protected E[] consumerBuffer;
    protected long consumerIndex;
}
