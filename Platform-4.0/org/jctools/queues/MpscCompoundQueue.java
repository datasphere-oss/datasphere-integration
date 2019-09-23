package org.jctools.queues;

import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.*;

public class MpscCompoundQueue<E> extends MpscCompoundQueueConsumerQueueIndex<E> implements BlockingQueue<E>
{
    private static final int CPUS;
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
    
    public MpscCompoundQueue(final int capacity) {
        this(capacity, MpscCompoundQueue.CPUS);
    }
    
    public MpscCompoundQueue(final int capacity, final int queueParallelism) {
        super(capacity, queueParallelism);
    }
    
    @Override
    public boolean offer(final E e) {
        final int id = (int)(Thread.currentThread().getId() & this.parallelQueuesMask);
        if (this.queues[id].offer((E)e)) {
            final Thread waiterLocal = this.emptyWaiter.getAndSet(null);
            if (waiterLocal != null) {
                LockSupport.unpark(waiterLocal);
            }
            return true;
        }
        return false;
    }
    
    @Override
    public E poll() {
        int qIndex = this.consumerQueueIndex & this.parallelQueuesMask;
        final int limit = qIndex + this.parallelQueues;
        E e = null;
        while (qIndex < limit) {
            final int id = qIndex & this.parallelQueuesMask;
            e = (E)this.queues[id].poll();
            if (e != null) {
                final Thread waiterLocal = this.fullWaiters[id].getAndSet(null);
                if (waiterLocal != null) {
                    LockSupport.unpark(waiterLocal);
                    break;
                }
                break;
            }
            else {
                ++qIndex;
            }
        }
        this.consumerQueueIndex = qIndex;
        return e;
    }
    
    @Override
    public E peek() {
        int qIndex = this.consumerQueueIndex & this.parallelQueuesMask;
        final int limit = qIndex + this.parallelQueues;
        E e = null;
        while (qIndex < limit) {
            e = (E)this.queues[qIndex & this.parallelQueuesMask].peek();
            if (e != null) {
                break;
            }
            ++qIndex;
        }
        this.consumerQueueIndex = qIndex;
        return e;
    }
    
    @Override
    public int size() {
        int size = 0;
        for (final MpscArrayQueue<E> lane : this.queues) {
            size += lane.size();
        }
        return size;
    }
    
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean relaxedOffer(final E e) {
        final int parallelQueuesMask = this.parallelQueuesMask;
        final int id = (int)(Thread.currentThread().getId() & parallelQueuesMask);
        final MpscArrayQueue<E>[] queues = this.queues;
        if (queues[id].failFastOffer(e) == 0) {
            final Thread waiterLocal = this.emptyWaiter.getAndSet(null);
            if (waiterLocal != null) {
                this.emptyWaiter = null;
                LockSupport.unpark(waiterLocal);
            }
            return true;
        }
        return false;
    }
    
    @Override
    public E relaxedPoll() {
        int qIndex = this.consumerQueueIndex & this.parallelQueuesMask;
        final int limit = qIndex + this.parallelQueues;
        E e = null;
        while (qIndex < limit) {
            final int id = qIndex & this.parallelQueuesMask;
            e = (E)this.queues[id].relaxedPoll();
            if (e != null) {
                final Thread waiterLocal = this.fullWaiters[id].getAndSet(null);
                if (waiterLocal != null) {
                    LockSupport.unpark(waiterLocal);
                    break;
                }
                break;
            }
            else {
                ++qIndex;
            }
        }
        this.consumerQueueIndex = qIndex;
        return e;
    }
    
    @Override
    public E relaxedPeek() {
        int qIndex = this.consumerQueueIndex & this.parallelQueuesMask;
        final int limit = qIndex + this.parallelQueues;
        E e = null;
        while (qIndex < limit) {
            e = (E)this.queues[qIndex & this.parallelQueuesMask].relaxedPeek();
            if (e != null) {
                break;
            }
            ++qIndex;
        }
        this.consumerQueueIndex = qIndex;
        return e;
    }
    
    @Override
    public int capacity() {
        return this.queues.length * this.queues[0].capacity();
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c) {
        final int limit = this.capacity();
        return this.drain(c, limit);
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int drain(final MessagePassingQueue.Consumer<E> c, final int limit) {
        for (int i = 0; i < limit; ++i) {
            final E e = this.relaxedPoll();
            if (e == null) {
                return i;
            }
            c.accept(e);
        }
        return limit;
    }
    
    @Override
    public int fill(final MessagePassingQueue.Supplier<E> s, final int limit) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void drain(final MessagePassingQueue.Consumer<E> c, final MessagePassingQueue.WaitStrategy wait, final MessagePassingQueue.ExitCondition exit) {
        int idleCounter = 0;
        while (exit.keepRunning()) {
            final E e = this.relaxedPoll();
            if (e == null) {
                idleCounter = wait.idle(idleCounter);
            }
            else {
                idleCounter = 0;
                c.accept(e);
            }
        }
    }
    
    @Override
    public void fill(final MessagePassingQueue.Supplier<E> s, final MessagePassingQueue.WaitStrategy wait, final MessagePassingQueue.ExitCondition exit) {
        int idleCounter = 0;
        while (exit.keepRunning()) {
            final E e = s.get();
            while (!this.relaxedOffer(e)) {
                idleCounter = wait.idle(idleCounter);
            }
            idleCounter = 0;
        }
    }
    
    @Override
    public E take() throws InterruptedException {
        return this.awaitNotEmpty(false, 0L);
    }
    
    @Override
    public E poll(final long time, final TimeUnit unit) throws InterruptedException {
        return this.awaitNotEmpty(true, unit.toNanos(time));
    }
    
    private E awaitNotEmpty(final boolean timed, long nanos) throws InterruptedException {
        final Thread w = Thread.currentThread();
        while (true) {
            final E retval = this.poll();
            if (retval != null) {
                return retval;
            }
            if (w.isInterrupted()) {
                throw new InterruptedException();
            }
            if (timed && nanos <= 0L) {
                return null;
            }
            if (!this.emptyWaiter.compareAndSet(null, w)) {
                continue;
            }
            if (timed) {
                LockSupport.parkNanos(this, nanos);
                nanos = 0L;
                this.emptyWaiter.set(null);
            }
            else {
                LockSupport.park(this);
                this.emptyWaiter.set(null);
            }
        }
    }
    
    private boolean awaitNotFull(final E e, final boolean timed, long nanos) throws InterruptedException {
        final Thread w = Thread.currentThread();
        while (true) {
            final boolean retval = this.offer(e);
            if (retval) {
                return retval;
            }
            if (w.isInterrupted()) {
                throw new InterruptedException();
            }
            if (timed && nanos <= 0L) {
                return false;
            }
            final int id = (int)(Thread.currentThread().getId() & this.parallelQueuesMask);
            if (!this.fullWaiters[id].compareAndSet(null, w)) {
                continue;
            }
            if (timed) {
                LockSupport.parkNanos(this, nanos);
                nanos = 0L;
                this.fullWaiters[id].set(null);
            }
            else {
                LockSupport.park(this);
                this.fullWaiters[id].set(null);
            }
        }
    }
    
    @Override
    public void put(final E e) throws InterruptedException {
        this.awaitNotFull(e, false, 0L);
    }
    
    @Override
    public boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {
        return this.awaitNotFull(e, true, unit.toNanos(timeout));
    }
    
    @Override
    public int remainingCapacity() {
        return this.capacity() - this.size();
    }
    
    @Override
    public int drainTo(final Collection<? super E> c) {
        final int limit = this.capacity();
        return this.drainTo(c, limit);
    }
    
    @Override
    public int drainTo(final Collection<? super E> c, final int maxElements) {
        for (int i = 0; i < maxElements; ++i) {
            final E e = this.relaxedPoll();
            if (e == null) {
                return i;
            }
            c.add((Object)e);
        }
        return maxElements;
    }
    
    static {
        CPUS = Runtime.getRuntime().availableProcessors();
    }
}
