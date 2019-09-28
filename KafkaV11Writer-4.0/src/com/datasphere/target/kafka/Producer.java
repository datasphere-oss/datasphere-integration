package com.datasphere.target.kafka;

import java.util.Properties;

import com.datasphere.event.Event;
import com.datasphere.intf.Formatter;
import com.datasphere.kafka.schemaregistry.SchemaRegistry;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.runtime.components.ReceiptCallback;
import com.datasphere.source.lib.constant.Constant;

public abstract class Producer
{
    public ReceiptCallback receiptCallback;
    private boolean swallowException;
    protected Formatter formatter;
    protected boolean isBinaryFormatter;
    protected Constant.FormatOptions formatOptions;
    protected String schemaRegistryURL;
    protected SchemaRegistry schemaRegistryClient;
    protected String topic;
    
    public Producer() {
        this.swallowException = false;
        this.formatter = null;
        this.isBinaryFormatter = false;
        this.formatOptions = Constant.FormatOptions.Default;
    }
    
    public static Producer getProducer(final Properties props, final String mode) {
        if (mode.equalsIgnoreCase("Async")) {
            return new AsyncProducer();
        }
        if (mode.equalsIgnoreCase("BatchedSync")) {
            return new BatchedSyncProducer(props);
        }
        return new SyncProducer();
    }
    
    public void setReceiptCallBack(final ReceiptCallback rc) {
        this.receiptCallback = rc;
    }
    
    public abstract long send(final int p0, final Event p1, final ImmutableStemma p2) throws Exception;
    
    public void initiateProducer(final String topic, final KafkaClient[] client, final ReceiptCallback rc, final Formatter formatter, final boolean isBinaryFormatter) throws Exception {
        this.topic = topic;
        this.formatter = formatter;
        this.isBinaryFormatter = isBinaryFormatter;
        this.receiptCallback = rc;
    }
    
    public void close() throws Exception {
        if (this.schemaRegistryClient != null) {
            this.schemaRegistryClient.close();
            this.schemaRegistryClient = null;
        }
    }
    
    public synchronized boolean swallowException() {
        return this.swallowException;
    }
    
    public synchronized void setSwallowException() {
        this.swallowException = true;
    }
    
    public abstract Object getAvgTimeBtwSendCalls();
    
    public abstract Object getMaxTimeBtwSendCalls();
    
    public Object getAvgLatency() {
        return 0;
    }
    
    public Object getMaxLatency() {
        return 0;
    }
    
    public abstract long getNoOfSendCalls();
}

