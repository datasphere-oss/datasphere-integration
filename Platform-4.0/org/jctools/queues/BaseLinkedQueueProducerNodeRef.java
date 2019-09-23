package org.jctools.queues;

import org.jctools.util.*;

abstract class BaseLinkedQueueProducerNodeRef<E> extends BaseLinkedQueuePad0<E>
{
    protected static final long P_NODE_OFFSET;
    protected LinkedQueueNode<E> producerNode;
    
    protected final void spProducerNode(final LinkedQueueNode<E> node) {
        this.producerNode = node;
    }
    
    protected final LinkedQueueNode<E> lvProducerNode() {
        return (LinkedQueueNode<E>)UnsafeAccess.UNSAFE.getObjectVolatile(this, BaseLinkedQueueProducerNodeRef.P_NODE_OFFSET);
    }
    
    protected final LinkedQueueNode<E> lpProducerNode() {
        return this.producerNode;
    }
    
    static {
        try {
            P_NODE_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(BaseLinkedQueueProducerNodeRef.class.getDeclaredField("producerNode"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
