package com.datasphere.concurrency;

import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import java.util.concurrent.*;
/*
 * 分区数据桶执行器
 */
public class PartitionedBucketExecutor extends AbstractExecutorService
{
    private final ExecutorService delegatedExecutor;
    private final ReentrantLock lock;
    private final Condition terminatingCondition;
    private final Map<Object, InorderExecutor> executorMap;
    private static final ThreadLocal<Object> partitions;
    private AtomicBoolean isTerminated;
    
    private PartitionedBucketExecutor(final ExecutorService service) {
        this.lock = new ReentrantLock();
        this.terminatingCondition = this.lock.newCondition();
        this.executorMap = new HashMap<Object, InorderExecutor>();
        this.isTerminated = new AtomicBoolean(Boolean.FALSE);
        this.delegatedExecutor = service;
    }
    
    public PartitionedBucketExecutor(final String name) {
        this(Executors.newCachedThreadPool(new NamedThreadFactory(name)));
    }
    
    public PartitionedBucketExecutor(final String name, final int numThreads) {
        this(Executors.newFixedThreadPool(numThreads, new NamedThreadFactory(name)));
    }
    
    public PartitionedBucketExecutor(final String name, final int numThreads, final ThreadExceptionObserver observer) {
        this(new ExceptionNotifyingThreadPool(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(name), observer));
    }
    
    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
        this.savePartition(runnable);
        return super.newTaskFor(runnable, value);
    }
    
    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
        this.savePartition(callable);
        return super.newTaskFor(callable);
    }
    
    private void savePartition(final Object task) {
        if (task instanceof Partitioner) {
            PartitionedBucketExecutor.partitions.set(((Partitioner)task).getBucket());
        }
    }
    
    @Override
    public void execute(final Runnable command) {
        this.lock.lock();
        try {
            if (this.isTerminated.get()) {
                throw new RejectedExecutionException("Executor not running");
            }
            final Object partitionForCommand = this.getPartition(command);
            if (partitionForCommand != null) {
                InorderExecutor partitionedExecutor = this.executorMap.get(partitionForCommand);
                if (partitionedExecutor == null) {
                    partitionedExecutor = new InorderExecutor(partitionForCommand);
                    this.executorMap.put(partitionForCommand, partitionedExecutor);
                }
                partitionedExecutor.execute(command);
            }
            else {
                this.delegatedExecutor.execute(command);
            }
        }
        finally {
            this.lock.unlock();
        }
    }
    
    private Object getPartition(final Runnable command) {
        Object ret;
        if (command instanceof Partitioner) {
            ret = ((Partitioner)command).getBucket();
        }
        else {
            ret = PartitionedBucketExecutor.partitions.get();
        }
        PartitionedBucketExecutor.partitions.remove();
        return ret;
    }
    
    @Override
    public void shutdown() {
        this.lock.lock();
        try {
            this.isTerminated.set(true);
            if (this.executorMap.isEmpty()) {
                this.delegatedExecutor.shutdown();
            }
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public List<Runnable> shutdownNow() {
        this.lock.lock();
        try {
            this.shutdown();
            final List<Runnable> result = new ArrayList<Runnable>();
            for (final InorderExecutor ex : this.executorMap.values()) {
                ex.tasks.drainTo(result);
            }
            result.addAll(this.delegatedExecutor.shutdownNow());
            return result;
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public boolean isShutdown() {
        this.lock.lock();
        try {
            return this.isTerminated.get();
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public boolean isTerminated() {
        this.lock.lock();
        try {
            if (!this.isTerminated.get()) {
                return false;
            }
            for (final InorderExecutor ex : this.executorMap.values()) {
                if (!ex.isAvailable()) {
                    return false;
                }
            }
            return this.delegatedExecutor.isTerminated();
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        this.lock.lock();
        try {
            final long waitTime = System.nanoTime() + unit.toNanos(timeout);
            long remainingWaitTime;
            while ((remainingWaitTime = waitTime - System.nanoTime()) > 0L && !this.executorMap.isEmpty()) {
                this.terminatingCondition.awaitNanos(remainingWaitTime);
            }
            return remainingWaitTime > 0L && this.executorMap.isEmpty() && this.delegatedExecutor.awaitTermination(remainingWaitTime, TimeUnit.NANOSECONDS);
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public Future<?> submit(final Runnable task) {
        return this.submit(task, (Object)null);
    }
    
    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        this.lock.lock();
        try {
            if (this.isTerminated.get()) {
                throw new RejectedExecutionException("Executor not running");
            }
            if (task instanceof Partitioner) {
                return super.submit(task);
            }
            return this.delegatedExecutor.submit(task);
        }
        finally {
            this.lock.unlock();
        }
    }
    
    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
        this.lock.lock();
        try {
            if (this.isTerminated.get()) {
                throw new RejectedExecutionException("Executor not running");
            }
            if (task instanceof Partitioner) {
                return super.submit(task, result);
            }
            return this.delegatedExecutor.submit(task, result);
        }
        finally {
            this.lock.unlock();
        }
    }
    
    static {
        partitions = new ThreadLocal<Object>();
    }
    
    private class InorderExecutor implements Executor
    {
        private final BlockingQueue<Runnable> tasks;
        private Runnable currentActiveTask;
        private final Object partition;
        
        private InorderExecutor(final Object partition) {
            this.tasks = new LinkedBlockingQueue<Runnable>();
            this.partition = partition;
        }
        
        @Override
        public void execute(final Runnable r) {
            PartitionedBucketExecutor.this.lock.lock();
            try {
                if (r instanceof FutureTask) {
                    final FutureTask ft = (FutureTask)r;
                    final Callable c = new Callable() {
                        @Override
                        public Object call() throws Exception {
                            ft.run();
                            if (!ft.isCancelled()) {
                                return ft.get();
                            }
                            return null;
                        }
                    };
                    this.tasks.add(new FutureTask(c) {
                        @Override
                        public void run() {
                            try {
                                super.run();
                            }
                            catch (Throwable t) {
                                this.setException(t);
                            }
                            finally {
                                InorderExecutor.this.executeNext();
                            }
                        }
                    });
                }
                else {
                    this.tasks.add(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                r.run();
                            }
                            finally {
                                InorderExecutor.this.executeNext();
                            }
                        }
                    });
                }
                if (this.currentActiveTask == null) {
                    this.executeNext();
                }
            }
            finally {
                PartitionedBucketExecutor.this.lock.unlock();
            }
        }
        
        private void executeNext() {
            PartitionedBucketExecutor.this.lock.lock();
            try {
                final Runnable currentActiveTask = this.tasks.poll();
                this.currentActiveTask = currentActiveTask;
                if (currentActiveTask != null) {
                    PartitionedBucketExecutor.this.delegatedExecutor.execute(this.currentActiveTask);
                    PartitionedBucketExecutor.this.terminatingCondition.signalAll();
                }
            }
            finally {
                PartitionedBucketExecutor.this.lock.unlock();
            }
        }
        
        public boolean isAvailable() {
            PartitionedBucketExecutor.this.lock.lock();
            try {
                return this.currentActiveTask == null && this.tasks.isEmpty();
            }
            finally {
                PartitionedBucketExecutor.this.lock.unlock();
            }
        }
    }
}
