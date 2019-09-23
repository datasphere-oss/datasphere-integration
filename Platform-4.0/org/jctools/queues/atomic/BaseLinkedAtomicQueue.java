package org.jctools.queues.atomic;

import java.util.concurrent.atomic.*;
import java.util.*;

abstract class BaseLinkedAtomicQueue<E> extends AbstractQueue<E>
{
    private final AtomicReference<LinkedQueueAtomicNode<E>> producerNode;
    private final AtomicReference<LinkedQueueAtomicNode<E>> consumerNode;
    
    public BaseLinkedAtomicQueue() {
        this.producerNode = new AtomicReference<LinkedQueueAtomicNode<E>>();
        this.consumerNode = new AtomicReference<LinkedQueueAtomicNode<E>>();
    }
    
    protected final LinkedQueueAtomicNode<E> lvProducerNode() {
        return this.producerNode.get();
    }
    
    protected final LinkedQueueAtomicNode<E> lpProducerNode() {
        return this.producerNode.get();
    }
    
    protected final void spProducerNode(final LinkedQueueAtomicNode<E> node) {
        this.producerNode.lazySet(node);
    }
    
    protected final LinkedQueueAtomicNode<E> xchgProducerNode(final LinkedQueueAtomicNode<E> node) {
        return this.producerNode.getAndSet(node);
    }
    
    protected final LinkedQueueAtomicNode<E> lvConsumerNode() {
        return this.consumerNode.get();
    }
    
    protected final LinkedQueueAtomicNode<E> lpConsumerNode() {
        return this.consumerNode.get();
    }
    
    protected final void spConsumerNode(final LinkedQueueAtomicNode<E> node) {
        this.consumerNode.lazySet(node);
    }
    
    @Override
    public final Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public final int size() {
        LinkedQueueAtomicNode<E> chaserNode;
        LinkedQueueAtomicNode<E> producerNode;
        int size;
        LinkedQueueAtomicNode<E> next;
        for (chaserNode = this.lvConsumerNode(), producerNode = this.lvProducerNode(), size = 0; chaserNode != producerNode && size < Integer.MAX_VALUE; chaserNode = next, ++size) {
            while ((next = chaserNode.lvNext()) == null) {}
        }
        return size;
    }
    
    @Override
    public final boolean isEmpty() {
        return this.lvConsumerNode() == this.lvProducerNode();
    }
}
