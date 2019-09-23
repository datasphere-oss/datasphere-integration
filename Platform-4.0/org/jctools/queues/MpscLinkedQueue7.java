package org.jctools.queues;

import org.jctools.util.*;

public class MpscLinkedQueue7<E> extends MpscLinkedQueue<E>
{
    @Override
    protected final LinkedQueueNode<E> xchgProducerNode(final LinkedQueueNode<E> newVal) {
        Object oldVal;
        do {
            oldVal = this.producerNode;
        } while (!UnsafeAccess.UNSAFE.compareAndSwapObject(this, MpscLinkedQueue7.P_NODE_OFFSET, oldVal, newVal));
        return (LinkedQueueNode<E>)oldVal;
    }
}
