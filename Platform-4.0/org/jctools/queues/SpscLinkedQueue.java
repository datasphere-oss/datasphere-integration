package org.jctools.queues;

public class SpscLinkedQueue<E> extends BaseLinkedQueue<E>
{
    public SpscLinkedQueue() {
        this.spProducerNode(new LinkedQueueNode<E>());
        this.spConsumerNode(this.producerNode);
        this.consumerNode.soNext(null);
    }
    
    @Override
    public boolean offer(final E nextValue) {
        if (nextValue == null) {
            throw new IllegalArgumentException("null elements not allowed");
        }
        final LinkedQueueNode<E> nextNode = new LinkedQueueNode<E>(nextValue);
        this.producerNode.soNext(nextNode);
        this.producerNode = nextNode;
        return true;
    }
    
    @Override
    public E poll() {
        final LinkedQueueNode<E> nextNode = this.consumerNode.lvNext();
        if (nextNode != null) {
            final E nextValue = nextNode.getAndNullValue();
            this.consumerNode = nextNode;
            return nextValue;
        }
        return null;
    }
    
    @Override
    public E peek() {
        final LinkedQueueNode<E> nextNode = this.consumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        return null;
    }
    
    @Override
    public boolean relaxedOffer(final E e) {
        return this.offer(e);
    }
    
    @Override
    public E relaxedPoll() {
        return this.poll();
    }
    
    @Override
    public E relaxedPeek() {
        return this.peek();
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
        LinkedQueueNode<E> chaserNode = this.producerNode;
        for (int i = 0; i < limit; ++i) {
            final LinkedQueueNode<E> nextNode = new LinkedQueueNode<E>(s.get());
            chaserNode.soNext(nextNode);
            chaserNode = nextNode;
            this.producerNode = chaserNode;
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
        LinkedQueueNode<E> chaserNode = this.producerNode;
        while (exit.keepRunning()) {
            for (int i = 0; i < 4096; ++i) {
                final LinkedQueueNode<E> nextNode = new LinkedQueueNode<E>(s.get());
                chaserNode.soNext(nextNode);
                chaserNode = nextNode;
                this.producerNode = chaserNode;
            }
        }
    }
}
