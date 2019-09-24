package com.datasphere.concurrency;

import java.util.concurrent.*;
/*
 * 对异常事件发出通知
 */
public class ExceptionNotifyingThreadPool extends ThreadPoolExecutor
{
    private final ThreadExceptionObserver observer;
    
    public ExceptionNotifyingThreadPool(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime, final TimeUnit unit, final BlockingQueue<Runnable> workQueue, final ThreadFactory threadFactory, final ThreadExceptionObserver observer) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.observer = observer;
    }
    
    @Override
    protected void afterExecute(final Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future) {
            try {
                final Future<?> future = (Future<?>)r;
                if (future.isDone()) {
                    future.get();
                }
            }
            catch (CancellationException ce) {
                t = ce;
            }
            catch (ExecutionException ee) {
                t = ee.getCause();
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        if (t != null) {
            this.observer.throwThreadExeption(t);
        }
    }
}
