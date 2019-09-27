package com.datasphere.source.lib.reader;

import org.apache.log4j.*;
import java.util.*;

import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;

import java.io.*;
import java.util.concurrent.*;

public class QueueReader extends ReaderBase
{
    protected LinkedBlockingQueue<Object> msgQueue;
    private final String QUEUE_SIZE = "QueueSize";
    private final int DEFAULT_QUEUE_SIZE = 10000;
    Map<Integer, Map<String, Object>> metadataMap;
    private Logger logger;
    private boolean stopCalled;
    
    protected QueueReader() throws AdapterException {
        super((ReaderBase)null);
        this.logger = Logger.getLogger((Class)QueueReader.class);
        this.metadataMap = new HashMap<Integer, Map<String, Object>>();
    }
    
    public QueueReader(final ReaderBase linkStrategy) throws AdapterException {
        super(linkStrategy);
        this.logger = Logger.getLogger((Class)QueueReader.class);
        this.metadataMap = new HashMap<Integer, Map<String, Object>>();
    }
    
    @Override
    protected void init() throws AdapterException {
        if (this.upstream != null && this.upstream instanceof QueueReader) {
            final LinkedBlockingQueue<Object> msgQueue = ((QueueReader)this.upstream).msgQueue;
            if (msgQueue != null) {
                this.msgQueue = msgQueue;
                this.hasQueue = true;
            }
        }
        if (!this.hasQueue) {
            final int maxQueueSize = this.property().getInt("QueueSize", 10000);
            this.msgQueue = new LinkedBlockingQueue<Object>(maxQueueSize);
            this.hasQueue = true;
        }
        super.init();
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        final Object dataBlock = this.msgQueue.poll();
        if (dataBlock != null) {
            final DataBlock db = (DataBlock)dataBlock;
            this.eventMetadataMap = (Map<String, Object>)db.metadata;
            return db.data;
        }
        return dataBlock;
    }
    
    @Override
    public void close() throws IOException {
        if (this.msgQueue.size() > 0) {
            this.msgQueue.clear();
        }
        this.metadataMap.clear();
        this.linkedStrategy.close();
        this.stopCalled = true;
    }
    
    protected void enqueue(final Object obj, final Map<String, Object> metadata) {
        Map<String, Object> metaObj = null;
        if (metadata != null) {
            metaObj = new HashMap<String, Object>();
            metaObj.putAll(metadata);
        }
        final DataBlock db = new DataBlock(obj, metaObj);
        this.enqueue(db);
    }
    
    @Override
    protected void enqueue(final Object obj) {
        final DataBlock db = new DataBlock(obj, null);
        this.enqueue(db);
    }
    
    protected void enqueue(final DataBlock obj) {
        boolean sent = false;
        do {
            try {
                sent = this.msgQueue.offer(obj, 100L, TimeUnit.MILLISECONDS);
                if (this.logger.isDebugEnabled() && !sent) {
                    this.logger.debug((Object)"Adding data in Queue is timedout, going to try again");
                }
            }
            catch (InterruptedException exp) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)"Adding data in Queue is interrupted, going to try again");
                }
            }
        } while (!this.stopCalled && !sent);
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.recoveryCheckpoint;
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        return this.eventMetadataMap;
    }
    
    class DataBlock
    {
        public Object data;
        public Object metadata;
        
        public DataBlock(final Object data, final Object meta) {
            this.data = data;
            this.metadata = meta;
        }
    }
}
