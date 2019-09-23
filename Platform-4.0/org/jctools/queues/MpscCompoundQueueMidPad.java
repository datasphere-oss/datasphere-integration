package org.jctools.queues;

abstract class MpscCompoundQueueMidPad<E> extends MpscCompoundQueueColdFields<E>
{
    long p01;
    long p02;
    long p03;
    long p04;
    long p05;
    long p06;
    long p07;
    long p10;
    long p11;
    long p12;
    long p13;
    long p14;
    long p15;
    long p16;
    long p17;
    
    public MpscCompoundQueueMidPad(final int capacity, final int queueParallelism) {
        super(capacity, queueParallelism);
    }
}
