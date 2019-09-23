package org.jctools.queues;

abstract class SpmcArrayQueueL1Pad<E> extends ConcurrentCircularArrayQueue<E>
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
    
    public SpmcArrayQueueL1Pad(final int capacity) {
        super(capacity);
    }
}
