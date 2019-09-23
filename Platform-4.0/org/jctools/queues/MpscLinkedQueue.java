package org.jctools.queues;

abstract class MpscLinkedQueue<E> extends BaseLinkedQueue<E>
{
    protected MpscLinkedQueue() {
        this.xchgProducerNode((LinkedQueueNode<E>)(this.consumerNode = (LinkedQueueNode<E>)new LinkedQueueNode<Object>()));
    }
    
    protected abstract LinkedQueueNode<E> xchgProducerNode(final LinkedQueueNode<E> p0);
    
    @Override
    public final boolean offer(final E nextValue) {
        if (nextValue == null) {
            throw new IllegalArgumentException("null elements not allowed");
        }
        final LinkedQueueNode<E> nextNode = new LinkedQueueNode<E>(nextValue);
        final LinkedQueueNode<E> prevProducerNode = this.xchgProducerNode(nextNode);
        prevProducerNode.soNext(nextNode);
        return true;
    }
    
    @Override
    public final E poll() {
        final LinkedQueueNode<E> currConsumerNode = this.lpConsumerNode();
        LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            final E nextValue = nextNode.getAndNullValue();
            this.spConsumerNode(nextNode);
            return nextValue;
        }
        if (currConsumerNode != this.lvProducerNode()) {
            while ((nextNode = currConsumerNode.lvNext()) == null) {}
            final E nextValue = nextNode.getAndNullValue();
            this.consumerNode = nextNode;
            return nextValue;
        }
        return null;
    }
    
    @Override
    public final E peek() {
        final LinkedQueueNode<E> currConsumerNode = this.consumerNode;
        LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        if (currConsumerNode != this.lvProducerNode()) {
            while ((nextNode = currConsumerNode.lvNext()) == null) {}
            return nextNode.lpValue();
        }
        return null;
    }
    
    @Override
    public boolean relaxedOffer(final E message) {
        return this.offer(message);
    }
    
    @Override
    public E relaxedPoll() {
        final LinkedQueueNode<E> currConsumerNode = this.lpConsumerNode();
        final LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            final E nextValue = nextNode.getAndNullValue();
            this.spConsumerNode(nextNode);
            return nextValue;
        }
        return null;
    }
    
    @Override
    public E relaxedPeek() {
        final LinkedQueueNode<E> currConsumerNode = this.consumerNode;
        LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        if (currConsumerNode != this.lvProducerNode()) {
            while ((nextNode = currConsumerNode.lvNext()) == null) {}
            return nextNode.lpValue();
        }
        return null;
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c) {
        long result = 0L;
        int drained;
        do {
            drained = this.drain(c, 4096);
            result += drained;
        } while (drained == 4096 && result <= 2147479551L);
        return (int)result;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s) {
        long result = 0L;
        do {
            this.fill(s, 4096);
            result += 4096L;
        } while (result <= 2147479551L);
        return (int)result;
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c, final int limit) {
        LinkedQueueNode<E> chaserNode = this.consumerNode;
        for (int i = 0; i < limit; ++i) {
            chaserNode = chaserNode.lvNext();
            if (chaserNode == null) {
                return i;
            }
            final E nextValue = chaserNode.getAndNullValue();
            this.consumerNode = chaserNode;
            c.accept(nextValue);
        }
        return limit;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s, final int limit) {
        final LinkedQueueNode<E> chaserNode = this.producerNode;
        for (int i = 0; i < limit; ++i) {
            this.offer(s.get());
        }
        return limit;
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy wait, final MessagePassingQueue.ExitCondition exit) {
        LinkedQueueNode<E> chaserNode = this.consumerNode;
        int idleCounter = 0;
        while (exit.keepRunning()) {
            for (int i = 0; i < 4096; ++i) {
                final LinkedQueueNode<E> next = chaserNode.lvNext();
                if (next == null) {
                    idleCounter = wait.idle(idleCounter);
                }
                else {
                    chaserNode = next;
                    idleCounter = 0;
                    final E nextValue = chaserNode.getAndNullValue();
                    this.consumerNode = chaserNode;
                    c.accept(nextValue);
                }
            }
        }
    }
    
    @Override
    public void fill(final MessagePassingQueue.Supplier<E> s, final MessagePassingQueue.WaitStrategy wait, final MessagePassingQueue.ExitCondition exit) {
        while (exit.keepRunning()) {
            this.fill(s, 4096);
        }
    }
}
