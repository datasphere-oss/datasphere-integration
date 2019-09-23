package org.jctools.queues;

import java.util.*;

abstract class BaseLinkedQueue<E> extends BaseLinkedQueueConsumerNodeRef<E>
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
    
    @Override
    public final Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public final int size() {
        LinkedQueueNode<E> chaserNode;
        LinkedQueueNode<E> producerNode;
        int size;
        LinkedQueueNode<E> next;
        for (chaserNode = this.lvConsumerNode(), producerNode = this.lvProducerNode(), size = 0; chaserNode != producerNode && size < Integer.MAX_VALUE; chaserNode = next, ++size) {
            while ((next = chaserNode.lvNext()) == null) {}
        }
        return size;
    }
    
    @Override
    public final boolean isEmpty() {
        return this.lvConsumerNode() == this.lvProducerNode();
    }
    
    @Override
    public int capacity() {
        return -1;
    }
}
