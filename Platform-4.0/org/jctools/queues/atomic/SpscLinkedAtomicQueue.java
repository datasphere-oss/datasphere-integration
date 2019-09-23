package org.jctools.queues.atomic;

public final class SpscLinkedAtomicQueue<E> extends BaseLinkedAtomicQueue<E>
{
    public SpscLinkedAtomicQueue() {
        final LinkedQueueAtomicNode<E> node = new LinkedQueueAtomicNode<E>();
        this.spProducerNode(node);
        this.spConsumerNode(node);
        node.soNext(null);
    }
    
    @Override
    public boolean offer(final E nextValue) {
        if (nextValue == null) {
            throw new IllegalArgumentException("null elements not allowed");
        }
        final LinkedQueueAtomicNode<E> nextNode = new LinkedQueueAtomicNode<E>(nextValue);
        this.lpProducerNode().soNext(nextNode);
        this.spProducerNode(nextNode);
        return true;
    }
    
    @Override
    public E poll() {
        final LinkedQueueAtomicNode<E> nextNode = this.lpConsumerNode().lvNext();
        if (nextNode != null) {
            final E nextValue = nextNode.getAndNullValue();
            this.spConsumerNode(nextNode);
            return nextValue;
        }
        return null;
    }
    
    @Override
    public E peek() {
        final LinkedQueueAtomicNode<E> nextNode = this.lpConsumerNode().lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        return null;
    }
}
