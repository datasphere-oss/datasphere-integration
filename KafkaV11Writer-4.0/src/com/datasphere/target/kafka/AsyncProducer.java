package com.datasphere.target.kafka;

import com.datasphere.source.lib.constant.*;
import com.datasphere.kafka.schemaregistry.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.components.*;
import com.datasphere.intf.*;
import com.datasphere.target.kafka.serializer.*;
import com.datasphere.event.*;
import com.datasphere.recovery.*;

public class AsyncProducer extends Producer
{
    private KafkaClient client;
    private KafkaMessageSerilaizer serializer;
    private long prevSendCall;
    private long totaltimeTakenBtwSendCalls;
    private long maxtimeTakenBtwSendCalls;
    private long eventCount;
    private long noOfSendCalls;
    
    public AsyncProducer() {
        this.serializer = null;
        this.prevSendCall = 0L;
        this.totaltimeTakenBtwSendCalls = 0L;
        this.maxtimeTakenBtwSendCalls = 0L;
        this.eventCount = 0L;
        this.noOfSendCalls = 0L;
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
    
    public void setBaseUri(final String schemaRegistryUrl, final Constant.FormatOptions formatOptions) throws MetaDataRepositoryException {
        this.schemaRegistryClient = new SchemaRegistry(schemaRegistryUrl);
        super.formatOptions = formatOptions;
    }
    
    @Override
    public void initiateProducer(final String topic, final KafkaClient[] client, final ReceiptCallback rc, final Formatter formatter, final boolean isBinaryFormatter) throws Exception {
        super.initiateProducer(topic, client, rc, formatter, isBinaryFormatter);
        this.client = client[0];
        if (this.formatOptions.equals((Object)Constant.FormatOptions.Native) || this.formatOptions.equals((Object)Constant.FormatOptions.Table)) {
            this.serializer = new NativeAvroSerializer(formatter, this.schemaRegistryClient);
        }
        else if (this.formatOptions.equals((Object)Constant.FormatOptions.Default) && this.schemaRegistryClient != null) {
            this.serializer = new DefaultByteWithSchemaRegistrySerializer(formatter, this.schemaRegistryClient);
        }
        else {
            this.serializer = new DefaultByteArraySerializer(formatter, isBinaryFormatter);
        }
    }
    
    @Override
    public long send(final int partition, final Event event, final ImmutableStemma pos) throws Exception {
        final byte[] value = this.serializer.convertToBytes(event);
        if (value == null) {
            this.receiptCallback.ack(1, (Stemma)pos);
            return 0L;
        }
        this.client.initProducerRecord(this.topic, partition, null, value);
        final long start = System.currentTimeMillis();
        this.client.asyncSend(pos, this);
        ++this.noOfSendCalls;
        if (this.prevSendCall > 0L) {
            final long timeTakenBtwSendCalls = start - this.prevSendCall;
            this.maxtimeTakenBtwSendCalls = Math.max(this.maxtimeTakenBtwSendCalls, timeTakenBtwSendCalls);
            this.totaltimeTakenBtwSendCalls += timeTakenBtwSendCalls;
        }
        this.prevSendCall = start;
        ++this.eventCount;
        return value.length;
    }
    
    @Override
    public void close() throws Exception {
        if (this.client != null) {
            this.client.closeProducer();
            this.client = null;
        }
        super.close();
    }
    
    @Override
    public long getNoOfSendCalls() {
        return this.noOfSendCalls;
    }
}

