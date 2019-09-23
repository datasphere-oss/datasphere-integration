package com.datasphere.persistence;

import org.apache.log4j.*;

import com.datasphere.hd.*;
import com.datasphere.intf.*;
import com.datasphere.runtime.*;
import com.datasphere.hdstore.*;
import java.util.concurrent.*;
import com.datasphere.runtime.components.*;
import java.util.*;

public class PeriodicPersistencePolicy implements PersistencePolicy
{
    private static Logger logger;
    private static final int DEFAULT_MAX_PERSIST_QUEUE_SIZE = 100000;
    private final PersistenceLayer persistenceLayer;
    protected long periodMillis;
    final ScheduledThreadPoolExecutor executor;
    ScheduledFuture<?> scheduledFlusher;
    private final BlockingQueue<HD> persistQueue;
    private final HStore ws;
    private Thread emergencyFlushThread;
    private Object emergencyFlushThreadFlag;
    private int batchWritingSize;
    
    public PeriodicPersistencePolicy(final int milliseconds, final PersistenceLayer persistenceLayer, final BaseServer srv, final Map<String, Object> props, final HStore ws) {
        this((long)milliseconds, persistenceLayer, srv, props, ws);
    }
    
    public PeriodicPersistencePolicy(final long milliseconds, final PersistenceLayer persistenceLayer, final BaseServer srv, final Map<String, Object> props, final HStore ws) {
        this.scheduledFlusher = null;
        this.emergencyFlushThread = null;
        this.emergencyFlushThreadFlag = new Object();
        this.periodMillis = milliseconds;
        this.persistenceLayer = persistenceLayer;
        this.ws = ws;
        this.persistenceLayer.setHDStore(ws);
        (this.executor = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("PeriodicPersistence_Scheduler"))).setRemoveOnCancelPolicy(true);
        final Runnable flushRunnable = new Runnable() {
            @Override
            public void run() {
                PeriodicPersistencePolicy.this.flush();
            }
        };
        this.scheduledFlusher = this.executor.scheduleWithFixedDelay(flushRunnable, this.periodMillis, this.periodMillis, TimeUnit.MILLISECONDS);
        try {
            this.batchWritingSize = Utility.extractInt(props.get("BATCH_WRITING_SIZE"));
        }
        catch (Exception e) {
            this.batchWritingSize = 10000;
        }
        int capacity;
        try {
            capacity = Utility.extractInt(props.get("MAX_PERSIST_QUEUE_SIZE"));
        }
        catch (Exception e2) {
            try {
                capacity = 2 * Utility.extractInt(props.get("MAX_THREAD_NUM")) * this.batchWritingSize;
            }
            catch (Exception e3) {
                try {
                    capacity = Integer.parseInt(System.getProperty("com.datasphere.optimalBackPressureThreshold", "10000"));
                }
                catch (Exception e4) {
                    PeriodicPersistencePolicy.logger.warn((Object)"Invalid setting for com.datasphere.optimalBackPressureThreshold, defaulting to 10000");
                    capacity = 10000;
                }
            }
        }
        this.persistQueue = new LinkedBlockingQueue<HD>(capacity);
    }
    
    @Override
    public void close() {
        this.flush();
        this.scheduledFlusher.cancel(true);
        this.shutdownExecutor();
    }
    
    public void shutdownExecutor() {
        this.executor.shutdown();
        try {
            if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS)) {
                this.executor.shutdownNow();
                if (!this.executor.awaitTermination(60L, TimeUnit.SECONDS)) {
                    System.err.println("PeriodicPersistence_Scheduler did not terminate");
                }
            }
        }
        catch (InterruptedException ie) {
            this.executor.shutdown();
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public synchronized void flush() {
        do {
            if (this.ws.isCrashed) {
                if (PeriodicPersistencePolicy.logger.isInfoEnabled()) {
                    PeriodicPersistencePolicy.logger.info((Object)("HD store :" + this.ws.getMetaFullName() + ", is flagged as crashed. Notifying app manager."));
                }
                this.ws.notifyAppMgr(EntityType.HDSTORE, this.ws.getMetaName(), this.ws.getMetaID(), this.ws.crashCausingException, "Persist hd", new Object[0]);
            }
            long TIME1 = 0L;
            long TIME2 = 0L;
            if (this.persistQueue.isEmpty()) {
                return;
            }
            final List<HD> writeHDs = new ArrayList<HD>();
            synchronized (this.persistQueue) {
                writeHDs.addAll(this.persistQueue);
            }
            if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                TIME1 = System.currentTimeMillis();
            }
            if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                PeriodicPersistencePolicy.logger.debug((Object)("Number of hds to be persisted: " + writeHDs.size()));
            }
            final PersistenceLayer.Range[] successRanges = this.persistenceLayer.persist(writeHDs);
            if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                TIME2 = System.currentTimeMillis();
            }
            if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                PeriodicPersistencePolicy.logger.debug((Object)("HDStore Periodic Persistence : Total time taken to persist " + writeHDs.size() + " hds is :" + (TIME2 - TIME1) + " milli secs. Rate : " + Math.floor(writeHDs.size() * 1000 / (TIME2 - TIME1))));
            }
            if (successRanges != null && successRanges.length > 0) {
                synchronized (this.persistQueue) {
                    int index = 0;
                    final Iterator<HD> iter = this.persistQueue.iterator();
                    if (!iter.hasNext()) {
                        continue;
                    }
                    for (int ij = 0; ij < successRanges.length; ++ij) {
                        final int startIndex = successRanges[ij].getStart();
                        final int endIndex = successRanges[ij].getEnd();
                        if (startIndex != 0 || endIndex != 0) {
                            if (successRanges[ij].isSuccessful()) {
                                if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                                    PeriodicPersistencePolicy.logger.debug((Object)("removing hds : index=" + index + ", startIndex=" + startIndex + ", endIndex=" + endIndex));
                                }
                                while (index >= startIndex && index < endIndex) {
                                    iter.next();
                                    iter.remove();
                                    ++index;
                                }
                            }
                            else {
                                if (PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                                    PeriodicPersistencePolicy.logger.debug((Object)("skipping hds : index=" + index + ", startIndex=" + startIndex + ", endIndex=" + endIndex));
                                }
                                while (index >= startIndex && index < endIndex) {
                                    iter.next();
                                    ++index;
                                }
                            }
                        }
                    }
                }
            }
            else {
                if (!PeriodicPersistencePolicy.logger.isDebugEnabled()) {
                    continue;
                }
                PeriodicPersistencePolicy.logger.debug((Object)"Empty PersistenceLayer.Range  returned from persist method.");
            }
        } while (this.persistQueue.size() >= this.batchWritingSize);
    }
    
    @Override
    public boolean addHD(final HD w) {
        try {
            if (this.emergencyFlushThread == null && this.persistQueue.remainingCapacity() <= this.persistQueue.size() / 19) {
                synchronized (this.emergencyFlushThreadFlag) {
                    if (this.emergencyFlushThread == null) {
                        final Runnable emergencyFlushRunnable = new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    PeriodicPersistencePolicy.this.flush();
                                }
                                finally {
                                    PeriodicPersistencePolicy.this.emergencyFlushThread = null;
                                }
                            }
                        };
                        (this.emergencyFlushThread = new Thread(emergencyFlushRunnable)).start();
                    }
                }
            }
            this.persistQueue.put(w);
            return true;
        }
        catch (InterruptedException e) {
            PeriodicPersistencePolicy.logger.error((Object)"Unable to add hd to persist queue", (Throwable)e);
            return false;
        }
    }
    
    @Override
    public Set<HD> getUnpersistedHDs() {
        return new HashSet<HD>(this.persistQueue);
    }
    
    static {
        PeriodicPersistencePolicy.logger = Logger.getLogger((Class)PeriodicPersistencePolicy.class);
    }
}
