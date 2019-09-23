package org.jctools.queues.atomic;

public final class MpscLinkedAtomicQueue<E> extends BaseLinkedAtomicQueue<E>
{
    public MpscLinkedAtomicQueue() {
        final LinkedQueueAtomicNode<E> node = new LinkedQueueAtomicNode<E>();
        this.spConsumerNode(node);
        this.xchgProducerNode(node);
    }
    
    @Override
    public final boolean offer(final E nextValue) {
        if (nextValue == null) {
            throw new IllegalArgumentException("null elements not allowed");
        }
        final LinkedQueueAtomicNode<E> nextNode = new LinkedQueueAtomicNode<E>(nextValue);
        final LinkedQueueAtomicNode<E> prevProducerNode = this.xchgProducerNode(nextNode);
        prevProducerNode.soNext(nextNode);
        return true;
    }
    
    @Override
    public final E poll() {
        final LinkedQueueAtomicNode<E> currConsumerNode = this.lpConsumerNode();
        LinkedQueueAtomicNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            final E nextValue = nextNode.getAndNullValue();
            this.spConsumerNode(nextNode);
            return nextValue;
        }
        if (currConsumerNode != this.lvProducerNode()) {
            while ((nextNode = currConsumerNode.lvNext()) == null) {}
            final E nextValue = nextNode.getAndNullValue();
            this.spConsumerNode(nextNode);
            return nextValue;
        }
        return null;
    }
    
    @Override
    public final E peek() {
        final LinkedQueueAtomicNode<E> currConsumerNode = this.lpConsumerNode();
        LinkedQueueAtomicNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        if (currConsumerNode != this.lvProducerNode()) {
            while ((nextNode = currConsumerNode.lvNext()) == null) {}
            return nextNode.lpValue();
        }
        return null;
    }
}
