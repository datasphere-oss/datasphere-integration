package com.datasphere.proc;

import com.datasphere.target.kafka.*;
import org.apache.kafka.clients.producer.*;
import com.datasphere.event.*;
import com.datasphere.recovery.*;

class RecordCallback implements Callback
{
    private ImmutableStemma eventPos;
    private AsyncProducer producer;
    private String topic;
    private int partitionID;
    private long messageValueSize;
    private long startTime;
    private long latency;
    
    public RecordCallback(final ImmutableStemma pos, final AsyncProducer p, final String t, final int partition, final long size) {
        this.messageValueSize = 0L;
        this.eventPos = pos;
        this.producer = p;
        this.topic = t;
        this.partitionID = partition;
        this.messageValueSize = size;
        this.startTime = System.currentTimeMillis();
    }
    
    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            if (!this.producer.swallowException()) {
                this.producer.setSwallowException();
                this.producer.receiptCallback.notifyException((Exception)new RuntimeException("Problem while flushing data to kafka topic (" + this.topic + "-" + this.partitionID + ")." + exception), (Event)null);
            }
            return;
        }
        if (this.producer.receiptCallback != null) {
            this.latency = System.currentTimeMillis() - this.startTime;
            this.producer.receiptCallback.ack(1, (Stemma)this.eventPos);
            this.producer.receiptCallback.latency(this.latency);
            this.producer.receiptCallback.bytesWritten(this.messageValueSize);
        }
    }
}
