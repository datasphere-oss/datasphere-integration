package org.jctools.queues;

import org.jctools.util.*;

abstract class BaseLinkedQueueConsumerNodeRef<E> extends BaseLinkedQueuePad1<E>
{
    protected static final long C_NODE_OFFSET;
    protected LinkedQueueNode<E> consumerNode;
    
    protected final void spConsumerNode(final LinkedQueueNode<E> node) {
        this.consumerNode = node;
    }
    
    protected final LinkedQueueNode<E> lvConsumerNode() {
        return (LinkedQueueNode<E>)UnsafeAccess.UNSAFE.getObjectVolatile(this, BaseLinkedQueueConsumerNodeRef.C_NODE_OFFSET);
    }
    
    protected final LinkedQueueNode<E> lpConsumerNode() {
        return this.consumerNode;
    }
    
    static {
        try {
            C_NODE_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(BaseLinkedQueueConsumerNodeRef.class.getDeclaredField("consumerNode"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
