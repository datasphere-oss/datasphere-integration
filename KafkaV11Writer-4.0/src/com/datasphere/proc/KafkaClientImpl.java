package com.datasphere.proc;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.datasphere.intf.Formatter;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.target.kafka.AsyncProducer;
import com.datasphere.target.kafka.KafkaClient;
import com.datasphere.uuid.UUID;

public class KafkaClientImpl implements KafkaClient
{
    protected KafkaProducer<byte[], byte[]> kafkaProducer;
    protected int maxRequestSize;
    protected long recordMetadataWaitMs;
    private ProducerRecord<byte[], byte[]> record;
    
    public KafkaClientImpl(final Properties props) {
        this.maxRequestSize = 1048576;
        this.recordMetadataWaitMs = 20000L;
        try {
            this.kafkaProducer = new KafkaProducer<byte[], byte[]>(props);
            if (props.containsKey("request.timeout.ms")) {
                try {
                    if (props.get("request.timeout.ms") != null) {
                        this.recordMetadataWaitMs = Long.parseLong(props.get("request.timeout.ms").toString());
                    }
                }
                catch (Exception ex) {}
            }
            if (props.containsKey("max.request.size")) {
                try {
                    if (props.get("max.request.size") != null) {
                        this.maxRequestSize = Integer.parseInt(props.get("max.request.size").toString());
                    }
                }
                catch (Exception ex2) {}
            }
        }
        catch (Exception e) {
            throw e;
        }
    }
    
    @Override
    public void initProducerRecord(final String topic, final int partition, final byte[] key, final byte[] value) {
        this.record = new ProducerRecord<byte[], byte[]>(topic, partition, key, value);
    }
    
    @Override
    public void closeProducer() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
    }
    
    @Override
    public long syncSend(final String topic, final int partition, final byte[] key, final byte[] value) throws InterruptedException, ExecutionException, TimeoutException {
        this.record = new ProducerRecord<byte[], byte[]>(topic, partition, key, value);
        final RecordMetadata rm = this.kafkaProducer.send(this.record).get(this.recordMetadataWaitMs, TimeUnit.MILLISECONDS);
        this.record = null;
        return rm.offset();
    }
    
    @Override
    public void asyncSend(final ImmutableStemma pos, final AsyncProducer p) {
//	        this.kafkaProducer.send(this.record, new RecordCallback(pos, p, this.record.topic(), this.record.partition(), this.record.value().length));
//	        this.record = null;
	        try {
				this.kafkaProducer.send(this.record);
			} catch (Exception e) {
				e.printStackTrace();
			}
	        this.record = null;
    }
    
    @Override
    public int getTotalNoofPartitions(final String topic) {
        try {
            final int noOfParitions = this.kafkaProducer.partitionsFor(topic).size();
            return noOfParitions;
        }
        catch (Exception e) {
            throw new RuntimeException("Problem while fetching the number of partitons " + e);
        }
    }
    
    @Override
    public String getClientType() {
        return "kafka";
    }
    
    @Override
    public OffsetPosition updatePartitionWaitPosition(final String topic, final int partitionId, final OffsetPosition checkPoint, final Offset startOffset, final Offset endOffset, final Formatter formatter, final UUID targetUUID) throws Exception {
        return null;
    }
    
    @Override
    public long verifyAndGetOffset(final String topic, final int partitionId, final long lastSuccessfullOffset, final byte[] data) {
        return 0L;
    }
    
    @Override
    public Offset getNewOffset() {
        return (Offset)new KafkaLongOffset(-1L);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
