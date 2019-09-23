package org.jctools.queues.atomic;

import java.util.concurrent.atomic.*;

public final class LinkedQueueAtomicNode<E> extends AtomicReference<LinkedQueueAtomicNode<E>>
{
    private static final long serialVersionUID = 2404266111789071508L;
    private E value;
    
    LinkedQueueAtomicNode() {
    }
    
    LinkedQueueAtomicNode(final E val) {
        this.spValue(val);
    }
    
    public E getAndNullValue() {
        final E temp = this.lpValue();
        this.spValue(null);
        return temp;
    }
    
    public E lpValue() {
        return this.value;
    }
    
    public void spValue(final E newValue) {
        this.value = newValue;
    }
    
    public void soNext(final LinkedQueueAtomicNode<E> n) {
        this.lazySet(n);
    }
    
    public LinkedQueueAtomicNode<E> lvNext() {
        return this.get();
    }
}
