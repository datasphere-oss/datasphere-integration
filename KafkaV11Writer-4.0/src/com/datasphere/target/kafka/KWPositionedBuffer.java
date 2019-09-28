package com.datasphere.target.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.classloading.FormatterLoader;
import com.datasphere.classloading.ModuleClassLoader;
import com.datasphere.common.exc.SystemException;
import com.datasphere.event.Event;
import com.datasphere.intf.Formatter;
import com.datasphere.kafka.KafkaException;
import com.datasphere.kafka.PositionedBuffer;
import com.datasphere.proc.BaseFormatter;
import com.datasphere.recovery.PathManager;
import com.datasphere.runtime.components.ReceiptCallback;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.target.kafka.serializer.DefaultByteWithSchemaRegistrySerializer;
import com.datasphere.target.kafka.serializer.KafkaMessageSerilaizer;
import com.datasphere.target.kafka.serializer.SyncByteArraySerializer;
import com.datasphere.target.kafka.serializer.SyncNativeAvroSerializer;
import com.datasphere.uuid.UUID;

public class KWPositionedBuffer extends PositionedBuffer
{
    protected Formatter formatter;
    protected volatile boolean stopProducing;
    protected BatchedSyncProducer batchedProducer;
    private KafkaMessageSerilaizer serializer;
    private String topic;
    private KafkaClient client;
    private long batchTimeout;
    private int retries;
    private long retryBackOffMs;
    private boolean hasValidHeaderOrFooter;
    private ReceiptCallback receiptCallback;
    private Thread flushTask;
    private ScheduledExecutorService flushservice;
    private static Logger logger;
    private static final int KAFKA_API_META_LENGTH = 100;
    private static final int FOOTER_LENGTH = 200;
    private List<Event> eventsToBeAcked;
    private long prevSendCall;
    private long totaltimeTakenBtwSendCalls;
    private long maxtimeTakenBtwSendCalls;
    private long totallatency;
    private long maxLatency;
    private long eventCount;
    private long noOfSends;
    
    public KWPositionedBuffer(final int partition_id, final int sendBufferSize, final long batchTimeout, final int retries, final long retryBackOffMs, final KafkaClient client, final Producer producer) throws Exception {
        super(partition_id, sendBufferSize - 100);
        this.stopProducing = false;
        this.hasValidHeaderOrFooter = false;
        this.receiptCallback = null;
        this.eventsToBeAcked = new ArrayList<Event>();
        this.prevSendCall = 0L;
        this.totaltimeTakenBtwSendCalls = 0L;
        this.maxtimeTakenBtwSendCalls = 0L;
        this.totallatency = 0L;
        this.maxLatency = 0L;
        this.eventCount = 0L;
        this.noOfSends = 0L;
        this.topic = producer.topic;
        this.client = client;
        this.batchTimeout = batchTimeout;
        this.retries = retries;
        this.retryBackOffMs = retryBackOffMs;
        this.receiptCallback = producer.receiptCallback;
        this.batchedProducer = (BatchedSyncProducer)producer;
        this.initFormatter(producer.formatter);
        final byte[] header = this.formatter.addHeader();
        if (header != null) {
            this.byteBuffer.put(header);
        }
        this.initializeTimer();
    }
    
    protected void initFormatter(final Formatter formatter) throws Exception {
        if (((BaseFormatter)formatter).hasValidHeaderAndFooter()) {
            this.hasValidHeaderOrFooter = ((BaseFormatter)formatter).hasValidHeaderAndFooter();
            this.formatter = FormatterLoader.loadFormatter(((BaseFormatter)formatter).getFormatterProperties(), ((BaseFormatter)formatter).getFields());
        }
        else {
            this.formatter = formatter;
        }
        if (this.batchedProducer.formatOptions.equals((Object)Constant.FormatOptions.Native) || this.batchedProducer.formatOptions.equals((Object)Constant.FormatOptions.Table)) {
            this.serializer = new SyncNativeAvroSerializer(this.formatter, this.batchedProducer.schemaRegistryClient);
        }
        else if (this.batchedProducer.formatOptions.equals((Object)Constant.FormatOptions.Default) && this.batchedProducer.schemaRegistryClient != null) {
            this.serializer = new DefaultByteWithSchemaRegistrySerializer(this.formatter, this.batchedProducer.schemaRegistryClient);
        }
        else {
            this.serializer = new SyncByteArraySerializer(this.formatter, this.batchedProducer.isBinaryFormatter);
        }
    }
    
    private void stoptimer() {
        this.flushservice.shutdown();
        this.flushservice = null;
    }
    
    private void initializeTimer() {
        this.flushservice = Executors.newSingleThreadScheduledExecutor();
        this.flushTask = new Thread(new FlushTask());
        this.flushservice.scheduleAtFixedRate(this.flushTask, this.batchTimeout, this.batchTimeout, TimeUnit.MILLISECONDS);
    }
    
    protected byte[] convertToBytes(final Object data) throws Exception {
        return this.serializer.convertToBytes(data);
    }
    
    public synchronized void put(final ITaskEvent data) throws Exception {
        throw new IllegalArgumentException("Unexpected KafkaWriter cannot handle data type: " + data.getClass());
    }
    
    @Override
	public void put(DARecord data) throws Exception {
    	if (this.stopProducing) {
            return;
        }
        byte[] bytes = this.convertToBytes(data);
        if (bytes == null) {
            if (super.recordsInMemory != null && data.position != null) {
                super.recordsInMemory.mergeHigherPositions(data.position);
            }
            ++this.pendingCount;
            return;
        }
        if (bytes.length > this.byteBuffer.capacity()) {
            throw new RecordBatchTooLargeException("Size of the incoming event " + bytes.length + " is greater than the batch.size " + this.byteBuffer.capacity() + ". Please increase the batch.size and max.message.bytes (topic configration) respectively.");
        }
        if (this.byteBuffer.position() + bytes.length > this.byteBuffer.limit() - 200) {
            this.stoptimer();
            try {
                this.flushToKafka();
            }
            catch (Exception e) {
                this.batchedProducer.setSwallowException();
                throw e;
            }
            if (this.hasValidHeaderOrFooter) {
                bytes = null;
                bytes = this.convertToBytes(data);
            }
            this.initializeTimer();
        }
        this.byteBuffer.put(bytes);
        if (super.recordsInMemory != null && data.position != null) {
            super.recordsInMemory.mergeHigherPositions(data.position);
        }
        ++this.pendingCount;
        this.eventsToBeAcked.add((Event)data.data);
	}
    
    
    
    public double getAvgLatency() {
        if (this.eventCount == 0L) {
            return 0.0;
        }
        return this.totallatency / (this.eventCount * 1.0);
    }
    
    public long getMaxLatency() {
        return this.maxLatency;
    }
    
    public double getAvgTimeBtwSendCalls() {
        if (this.eventCount == 0L) {
            return 0.0;
        }
        return this.totaltimeTakenBtwSendCalls / (this.eventCount * 1.0);
    }
    
    public long getMaxTimeBtwSendCalls() {
        return this.maxtimeTakenBtwSendCalls;
    }
    
    public long getNoOfSends() {
        return this.noOfSends;
    }
    
    public synchronized void flushToKafka() throws Exception {
        //final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        //final ModuleClassLoader mcl = (ModuleClassLoader)this.client.getClass().getClassLoader();
        //Thread.currentThread().setContextClassLoader((ClassLoader)mcl);
        if (this.byteBuffer.position() == 0 || this.pendingCount == 0 || this.stopProducing) {
            return;
        }
        final byte[] footer = this.formatter.addFooter();
        if (footer != null) {
            this.byteBuffer.put(footer);
        }
        final byte[] kafkaBytes = new byte[this.byteBuffer.position()];
        if (KWPositionedBuffer.logger.isDebugEnabled()) {
            KWPositionedBuffer.logger.debug((Object)("Flushing a batched record of length " + this.byteBuffer.position() + " to " + this.topic + "-" + this.partitionId));
        }
        System.arraycopy(this.byteBuffer.array(), 0, kafkaBytes, 0, this.byteBuffer.position());
        try {
            final long timeBeforeSendCall = System.currentTimeMillis();
            int retrySend = this.retries;
            try {
                this.lastSuccessfulWriteOffset = this.client.syncSend(this.topic, this.partitionId, null, kafkaBytes);
            }
            catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                if (this.retries <= 0) {
                    throw e;
                }
                KWPositionedBuffer.logger.warn((Object)("Write to kafka topic - (" + this.topic + "- partition: " + this.partitionId + ") failed due to the following exception : " + e.getMessage() + "\nWill verify if the write was succeessfull (after " + this.retryBackOffMs + " ms), else will retry sending the data to (" + this.topic + "-" + this.partitionId + "). " + retrySend + " retries left."), (Throwable)e);
                while (retrySend > 0 && !Thread.currentThread().isInterrupted() && !this.stopProducing) {
                    try {
                        Thread.sleep(this.retryBackOffMs);
                        final long offsetFromPartition = this.client.verifyAndGetOffset(this.topic, this.partitionId, this.lastSuccessfulWriteOffset, kafkaBytes);
                        if (offsetFromPartition == -1L || offsetFromPartition == this.lastSuccessfulWriteOffset) {
                            this.lastSuccessfulWriteOffset = this.client.syncSend(this.topic, this.partitionId, null, kafkaBytes);
                            break;
                        }
                        this.lastSuccessfulWriteOffset = offsetFromPartition;
                        break;
                    }
                    catch (Exception e2) {
                        if (e2 instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                            throw e2;
                        }
                        if (retrySend - 1 == 0) {
                            throw new KafkaException("Couldn't verify if the last write to kafka topic (" + this.topic + "," + this.partitionId + ") was successful after verifying for " + this.retries + " times.", (Throwable)e2);
                        }
                        KWPositionedBuffer.logger.warn((Object)("Couldn't verify if the last write to kafka topic (" + this.topic + "," + this.partitionId + ") was successful. Will retry to verify again after " + this.retryBackOffMs + "ms. (" + (retrySend - 1) + ") retries left."), (Throwable)e2);
                        --retrySend;
                        continue;
                    }
                }
            }
            if (KWPositionedBuffer.logger.isDebugEnabled()) {
                KWPositionedBuffer.logger.debug((Object)("Last Successfull Write offset is " + this.lastSuccessfulWriteOffset + " in " + this.topic + "-" + this.partitionId));
            }
            final long currentTimeMillis = System.currentTimeMillis();
            final long n = timeBeforeSendCall;
            final long latency = currentTimeMillis - n;
            this.eventCount += this.pendingCount;
            ++this.noOfSends;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.totallatency += latency;
            if (this.prevSendCall > 0L) {
                final long n2 = timeBeforeSendCall;
                final long timeTakenBtwSendCalls = n2 - n2;
                this.maxtimeTakenBtwSendCalls = Math.max(this.maxtimeTakenBtwSendCalls, timeTakenBtwSendCalls);
                this.totaltimeTakenBtwSendCalls += timeTakenBtwSendCalls;
            }
            final long prevSendCall = timeBeforeSendCall;
            this.prevSendCall = prevSendCall;
            this.receiptCallback.bytesWritten((long)kafkaBytes.length);
            if (this.recordsInMemory == null) {
                this.receiptCallback.ack(this.pendingCount);
            }
            else {
                this.receiptCallback.ack(this.pendingCount, this.recordsInMemory.toPosition());
            }
            for (final Event e3 : this.eventsToBeAcked) {
                this.receiptCallback.ack(new UUID(e3.get_da_SimpleEvent_ID()));
            }
            this.eventsToBeAcked.clear();
        }
        catch (Exception e4) {
            throw e4;
        }
        finally {
            this.byteBuffer.clear();
            final byte[] header = this.formatter.addHeader();
            if (header != null) {
                this.byteBuffer.put(header);
            }
            this.pendingCount = 0;
            this.recordsInMemory = new PathManager();
        }
        //Thread.currentThread().setContextClassLoader(cl);
    }
    
    public void closeProducer() {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final ModuleClassLoader mcl = (ModuleClassLoader)this.client.getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader((ClassLoader)mcl);
        final long start = System.currentTimeMillis();
        try {
            this.stopProducing = true;
            if (this.flushservice != null) {
                if (this.flushTask != null) {
                    try {
                        this.flushTask.interrupt();
                    }
                    catch (Exception e) {
                        if (KWPositionedBuffer.logger.isInfoEnabled()) {
                            KWPositionedBuffer.logger.info((Object)("Exception while stopping flush task. But this can be ignored " + e));
                        }
                    }
                }
                this.flushservice.shutdown();
                this.flushservice = null;
            }
            synchronized (this) {
                if (this.client != null) {
                    this.client.closeProducer();
                    this.client = null;
                }
            }
        }
        catch (Exception e) {
            KWPositionedBuffer.logger.warn((Object)("Error while stopping KafkaWriter, it can be ignored " + e));
        }
        if (KWPositionedBuffer.logger.isInfoEnabled()) {
            KWPositionedBuffer.logger.info((Object)("Took " + (System.currentTimeMillis() - start) + " ms to close " + this.topic + "-" + this.partitionId));
        }
        Thread.currentThread().setContextClassLoader(cl);
    }
    
    static {
        KWPositionedBuffer.logger = Logger.getLogger((Class)KWPositionedBuffer.class);
    }
    
    class FlushTask implements Runnable
    {
        @Override
        public void run() {
            try {
                if (!KWPositionedBuffer.this.stopProducing) {
                    KWPositionedBuffer.this.flushToKafka();
                }
            }
            catch (Exception e) {
                if (!KWPositionedBuffer.this.batchedProducer.swallowException()) {
                    KWPositionedBuffer.this.batchedProducer.setSwallowException();
                    KWPositionedBuffer.this.receiptCallback.notifyException((Exception)new SystemException("Problem while flushing data to " + KWPositionedBuffer.this.client.getClientType() + " topic \"" + KWPositionedBuffer.this.topic + "\", partition id \"" + KWPositionedBuffer.this.partitionId + "\"." + e), (Event)null);
                }
                else if (KWPositionedBuffer.logger.isInfoEnabled()) {
                    KWPositionedBuffer.logger.info((Object)("Problem while flushing data to " + KWPositionedBuffer.this.client.getClientType() + " topic \"" + KWPositionedBuffer.this.topic + "\", partition id \"" + KWPositionedBuffer.this.partitionId + "\"." + e));
                }
            }
        }
    }

	
}

