package com.datasphere.hdstore;

import org.apache.log4j.*;
import java.util.concurrent.atomic.*;
import com.datasphere.persistence.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

public abstract class BackgroundTask implements Runnable
{
    private static final Class<BackgroundTask> thisClass;
    private static final Logger logger;
    private final Lock backgroundThreadLock;
    private final Condition backgroundThreadSignal;
    private final AtomicBoolean terminationRequest;
    private final String hdStoreName;
    private final HStore ws;
    private final String threadName;
    private final long timeoutMilliseconds;
    private Thread backgroundThread;
    
    protected BackgroundTask(final String hdStoreName, final String dataTypeName, final long timeoutMilliseconds, final HStore ws) {
        this.backgroundThreadLock = new ReentrantLock();
        this.backgroundThreadSignal = this.backgroundThreadLock.newCondition();
        this.terminationRequest = new AtomicBoolean(false);
        this.backgroundThread = null;
        this.hdStoreName = hdStoreName;
        this.threadName = String.format("BackgroundTask-%s-%s", hdStoreName, dataTypeName);
        this.timeoutMilliseconds = timeoutMilliseconds;
        this.ws = ws;
    }
    
    public HStore getHDStoreObject() {
        return this.ws;
    }
    
    public void waitForSignal() {
        this.backgroundThreadLock.lock();
        try {
            this.backgroundThreadSignal.await(this.timeoutMilliseconds, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex) {}
        finally {
            this.backgroundThreadLock.unlock();
        }
    }
    
    public void signalBackgroundThread() {
        this.backgroundThreadLock.lock();
        try {
            this.backgroundThreadSignal.signalAll();
        }
        finally {
            this.backgroundThreadLock.unlock();
        }
    }
    
    public void start() {
        synchronized (this.backgroundThreadLock) {
            if (!this.isAlive()) {
                this.terminationRequest.set(false);
                this.backgroundThread = new Thread(this, this.threadName);
                HDStores.registerBackgroundTask(this);
                this.backgroundThread.start();
                BackgroundTask.logger.info((Object)String.format("Started background task '%s' for HDStore '%s'", this.threadName, this.hdStoreName));
            }
        }
    }
    
    public void terminate() {
        synchronized (this.backgroundThreadLock) {
            HDStores.unregisterBackgroundTask(this);
            this.terminationRequest.set(true);
            if (this.isAlive()) {
                BackgroundTask.logger.info((Object)String.format("Terminating background task '%s' for HDStore '%s'", this.threadName, this.hdStoreName));
                this.signalBackgroundThread();
                try {
                    this.backgroundThread.join(this.timeoutMilliseconds);
                    if (this.isAlive()) {
                        BackgroundTask.logger.warn((Object)("Failed to terminate the background thread " + String.format("Terminating background task '%s' for HDStore '%s'", this.threadName, this.hdStoreName)));
                    }
                }
                catch (InterruptedException ex) {}
            }
            else {
                BackgroundTask.logger.info((Object)String.format("Background task '%s' for HDStore '%s' already terminated", this.threadName, this.hdStoreName));
            }
        }
    }
    
    public boolean isTerminationRequested() {
        return this.terminationRequest.get();
    }
    
    public boolean isAlive() {
        synchronized (this.backgroundThreadLock) {
            return this.backgroundThread != null && this.backgroundThread.isAlive();
        }
    }
    
    public String getHDStoreName() {
        return this.hdStoreName;
    }
    
    static {
        thisClass = BackgroundTask.class;
        logger = Logger.getLogger((Class)BackgroundTask.thisClass);
    }
}
