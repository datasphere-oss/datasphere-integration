package com.datasphere.target.kafka;

import org.apache.log4j.*;
import com.datasphere.runtime.components.*;
import com.datasphere.intf.*;
import com.datasphere.event.*;
import com.datasphere.source.lib.constant.*;
import java.nio.*;
import com.datasphere.recovery.*;

public class SyncProducer extends Producer
{
    private static Logger logger;
    private KafkaClient client;
    private long prevSendCall;
    private long totaltimeTakenBtwSendCalls;
    private long maxtimeTakenBtwSendCalls;
    private long eventCount;
    private long totallatency;
    private long maxLatency;
    private long noOfSends;
    
    public SyncProducer() {
        this.prevSendCall = 0L;
        this.totaltimeTakenBtwSendCalls = 0L;
        this.maxtimeTakenBtwSendCalls = 0L;
        this.eventCount = 0L;
        this.totallatency = 0L;
        this.maxLatency = 0L;
        this.noOfSends = 0L;
    }
    
    @Override
    public void initiateProducer(final String topic, final KafkaClient[] client, final ReceiptCallback rc, final Formatter formatter, final boolean isBinaryFormatter) throws Exception {
        super.initiateProducer(topic, client, rc, formatter, isBinaryFormatter);
        this.client = client[0];
    }
    
    @Override
    public Object getAvgLatency() {
        if (this.eventCount == 0L) {
            return String.valueOf(0.0);
        }
        return String.valueOf(this.totallatency / this.eventCount * 1.0);
    }
    
    @Override
    public Object getMaxLatency() {
        return this.maxLatency;
    }
    
    @Override
    public Object getAvgTimeBtwSendCalls() {
        if (this.eventCount == 0L) {
            return String.valueOf(0.0);
        }
        return String.valueOf(this.totaltimeTakenBtwSendCalls / (this.eventCount * 1.0));
    }
    
    @Override
    public Object getMaxTimeBtwSendCalls() {
        return this.maxtimeTakenBtwSendCalls;
    }
    
    @Override
    public long send(final int partition, final Event event, final ImmutableStemma pos) throws Exception {
        try {
            final byte[] headerBytes = this.formatter.addHeader();
            final byte[] footerBytes = this.formatter.addFooter();
            final byte[] hdEventBytes = this.formatter.format((Object)event);
            byte[] value;
            if (super.isBinaryFormatter) {
                final ByteBuffer byteBuffer = ByteBuffer.allocate(hdEventBytes.length + Constant.INTEGER_SIZE);
                byteBuffer.putInt(hdEventBytes.length);
                byteBuffer.put(hdEventBytes);
                value = byteBuffer.array();
            }
            else if (headerBytes != null && footerBytes != null) {
                value = new byte[headerBytes.length + hdEventBytes.length + footerBytes.length];
                System.arraycopy(headerBytes, 0, value, 0, headerBytes.length);
                System.arraycopy(hdEventBytes, 0, value, headerBytes.length, hdEventBytes.length);
                System.arraycopy(footerBytes, 0, value, headerBytes.length + hdEventBytes.length, footerBytes.length);
            }
            else {
                value = hdEventBytes;
            }
            final long start = System.currentTimeMillis();
            this.client.syncSend(this.topic, partition, null, value);
            ++this.noOfSends;
            if (super.receiptCallback != null) {
                super.receiptCallback.ack(1, (Stemma)pos);
                super.receiptCallback.bytesWritten((long)value.length);
            }
            final long latency = System.currentTimeMillis() - start;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.totallatency += latency;
            if (this.prevSendCall > 0L) {
                final long timeTakenBtwSendCalls = this.prevSendCall - start;
                this.maxtimeTakenBtwSendCalls = Math.max(this.maxtimeTakenBtwSendCalls, timeTakenBtwSendCalls);
                this.totaltimeTakenBtwSendCalls += timeTakenBtwSendCalls;
            }
            this.prevSendCall = start;
            ++this.eventCount;
        }
        catch (Exception e) {
            this.client.closeProducer();
            this.client = null;
            throw e;
        }
        return 0L;
    }
    
    @Override
    public void close() {
        if (this.client != null) {
            this.client.closeProducer();
            this.client = null;
        }
    }
    
    @Override
    public long getNoOfSendCalls() {
        return this.noOfSends;
    }
    
    static {
        SyncProducer.logger = Logger.getLogger((Class)SyncProducer.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}

