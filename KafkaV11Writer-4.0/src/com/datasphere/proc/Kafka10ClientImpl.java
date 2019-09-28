package com.datasphere.proc;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;

import com.datasphere.common.constants.Constant;
import com.datasphere.common.exc.RecordException;
import com.datasphere.intf.Formatter;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.uuid.UUID;

public class Kafka10ClientImpl extends KafkaClientImpl
{
    private Properties consumerProps;
    private static Logger logger;
    private long totalReadTime;
    private long totalParseTime;
    
    public Kafka10ClientImpl(final Properties props) {
        super(props);
        this.totalReadTime = 0L;
        this.totalParseTime = 0L;
        (this.consumerProps = new Properties()).putAll(props);
        this.consumerProps.remove("key.serializer");
        this.consumerProps.remove("value.serializer");
        this.consumerProps.remove("acks");
        this.consumerProps.remove("max.request.size");
        this.consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        final int maxFetchSize = (this.maxRequestSize > 10485760) ? this.maxRequestSize : 10485760;
        this.consumerProps.put("max.partition.fetch.bytes", maxFetchSize);
        this.consumerProps.put("fetch.min.bytes", 1048576);
        this.consumerProps.put("fetch.max.wait.ms", 1000);
        this.consumerProps.put("receive.buffer.bytes", 2000000);
    }
    
    @Override
    public OffsetPosition updatePartitionWaitPosition(final String topic, final int partitionId, final OffsetPosition partitionCheckpoint, Offset startOffset, final Offset endOffset, final Formatter formatter, final UUID targetUUID) throws Exception {
        final PathManager updateCheckpoint = new PathManager((Position)partitionCheckpoint);
        KafkaConsumer<byte[], byte[]> consumer = null;
        String positionStatus = "Data has valid wait position.";
        try {
            final Properties consumerProperties = new Properties();
            consumerProperties.putAll(this.consumerProps);
            consumerProperties.put("client.id", "Consumer_Update_Waitposition_" + new UUID(System.currentTimeMillis()) + topic + partitionId);
            consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
            final TopicPartition tp = new TopicPartition(topic, partitionId);
            consumer.assign(Collections.singletonList(tp));
            if (!startOffset.isValid()) {
                startOffset = (Offset)new KafkaLongOffset(0L);
            }
            consumer.seek(tp, (long)startOffset.getOffset());
            final MetaInfo.MetaObject targetInfo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(targetUUID, HDSecurityManager.TOKEN);
            final long startTime = System.currentTimeMillis();
            while (startOffset.compareTo(endOffset) <= 0) {
                if (startOffset == endOffset) {
                    if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                        Kafka10ClientImpl.logger.info((Object)("Retriving merged position of data with kafka offset " + startOffset + " from topic (" + topic + "-" + partitionId + ")."));
                    }
                    else if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                        Kafka10ClientImpl.logger.info((Object)("Retriving merged position of data with kafka offset from " + startOffset + " to " + endOffset + " from topic (" + topic + "-" + partitionId + ")."));
                    }
                }
                final long startRead = System.currentTimeMillis();
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(1000L);
                final long stopRead = System.currentTimeMillis();
                this.totalReadTime += stopRead - startRead;
                if (records != null && records.count() > 0) {
                    for (final ConsumerRecord<byte[], byte[]> record : records) {
                        startOffset = (Offset)new KafkaLongOffset(record.offset() + 1L);
                        final long startParse = System.currentTimeMillis();
                        try {
                            final Position position = formatter.convertBytesToPosition((byte[])record.value(), targetUUID);
                            final long stopParse = System.currentTimeMillis();
                            this.totalParseTime += stopParse - startParse;
                            if (position != null) {
                                updateCheckpoint.mergeHigherPositions(position);
                            }
                            else {
                                Kafka10ClientImpl.logger.warn((Object)("First message at offset " + startOffset + " of topic " + topic + "-" + partitionId + " has NULL position (or data produced from different KafkaWriter)."));
                                if (this.checkIfLastOffsetHasValidPosition(topic, partitionId, endOffset, formatter, targetUUID)) {
                                    Kafka10ClientImpl.logger.warn((Object)("Last message at offset " + endOffset + " of topic " + topic + "-" + partitionId + " also has NULL position (or data produced from different KafkaWriter), hence skipping wait position computation."));
                                    startOffset = (Offset)new KafkaLongOffset((long)endOffset.getOffset() + 1L);
                                    positionStatus = "Data has in-valid wait position";
                                    break;
                                }
                                continue;
                            }
                        }
                        catch (RecordException re) {
                            if (re.returnStatus() != Constant.recordstatus.INVALID_RECORD) {
                                continue;
                            }
                            if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                                Kafka10ClientImpl.logger.info((Object)("First message at offset " + startOffset + " of topic " + topic + "-" + partitionId + " was not produced by " + targetInfo.name));
                            }
                            if (this.checkIfLastOffsetHasValidPosition(topic, partitionId, endOffset, formatter, targetUUID)) {
                                Kafka10ClientImpl.logger.warn((Object)(targetInfo.name + " is configured to write data to topic " + topic + " which has data from some other KafkaWriter component. Hence skipping the wait position computation and will allow any data that comes in from the Source. Recommended approach is to start with a fresh topic in case of Sync mode with recovery ON."));
                                startOffset = (Offset)new KafkaLongOffset((long)endOffset.getOffset() + 1L);
                                positionStatus = "Data has in-valid wait position.";
                                break;
                            }
                            continue;
                        }
                    }
                }
            }
            if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                Kafka10ClientImpl.logger.info((Object)("Computing wait position for the topic-partition : " + topic + "-" + partitionId + "\nTotal time to process the waitposition : " + (System.currentTimeMillis() - startTime) + "\nTotal time to read the data : " + this.totalReadTime + "\nTotal time to parse the data : " + this.totalParseTime + "\n" + positionStatus));
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Problem while fetching latest position of (" + topic + "," + partitionId + "). This usually happens if the partition log file has been corrupted or the existing data in the partition was generated through a different application." + e);
        }
        finally {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
            this.consumerProps.remove("client.id");
            this.totalReadTime = 0L;
            this.totalParseTime = 0L;
        }
        final Position p = updateCheckpoint.toPosition();
        return new OffsetPosition(p, endOffset, System.currentTimeMillis());
    }
    
    private boolean checkIfLastOffsetHasValidPosition(final String topic, final int partitionId, final Offset endOffset, final Formatter formatter, final UUID targetUUID) throws Exception {
        KafkaConsumer<byte[], byte[]> consumer = null;
        try {
            final Properties consumerProperties = new Properties();
            long readOffset = (long)endOffset.getOffset();
            consumerProperties.putAll(this.consumerProps);
            consumerProperties.put("client.id", "Consumer_Check_If_Last_Offset_Has_Valid_Position" + new UUID(System.currentTimeMillis()) + topic + partitionId);
            consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
            final TopicPartition tp = new TopicPartition(topic, partitionId);
            consumer.assign(Collections.singletonList(tp));
            consumer.seek(tp, (long)endOffset.getOffset());
            while (readOffset == (long)endOffset.getOffset()) {
                final long startRead = System.currentTimeMillis();
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(1000L);
                final long stopRead = System.currentTimeMillis();
                this.totalReadTime += stopRead - startRead;
                if (records != null && records.count() > 0) {
                    for (final ConsumerRecord<byte[], byte[]> record : records) {
                        try {
                            final long startParse = System.currentTimeMillis();
                            final Position position = formatter.convertBytesToPosition((byte[])record.value(), targetUUID);
                            final long stopParse = System.currentTimeMillis();
                            this.totalParseTime += stopParse - startParse;
                            if (position == null) {
                                return true;
                            }
                            readOffset = record.offset() + 1L;
                        }
                        catch (RecordException re) {
                            return true;
                        }
                    }
                }
            }
        }
        finally {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        }
        return false;
    }
    
    @Override
    public long verifyAndGetOffset(final String topic, final int partitionId, final long lastSuccessfullWriteOffset, final byte[] subset) {
        KafkaConsumer<byte[], byte[]> consumer = null;
        try {
            final long startTime = System.currentTimeMillis();
            final Properties consumerProperties = new Properties();
            consumerProperties.putAll(this.consumerProps);
            consumerProperties.put("client.id", "Consumer_Verify_Recent_Flush" + new UUID(System.currentTimeMillis()) + "_" + topic + "-" + partitionId);
            consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
            final List<PartitionInfo> pInfos = consumer.partitionsFor(topic);
            if (pInfos == null || pInfos.isEmpty()) {
                throw new RuntimeException("Problem while verifying send failure to (" + topic + "-" + partitionId + "). Unable to refresh the metadata.");
            }
            for (final PartitionInfo pInfo : pInfos) {
                if (pInfo.partition() == partitionId) {
                    Kafka10ClientImpl.logger.info((Object)("Refreshed Partition info of (" + topic + "-" + partitionId + ") is " + pInfo.toString()));
                }
            }
            final TopicPartition tp = new TopicPartition(topic, partitionId);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToEnd(Collections.singletonList(tp));
            final long lastOffsetOfPartition = consumer.position(tp) - 1L;
            if (lastOffsetOfPartition < 0L) {
                if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                    Kafka10ClientImpl.logger.info((Object)("Last offset of (" + topic + ":" + partitionId + ") is " + lastOffsetOfPartition));
                }
                return lastOffsetOfPartition;
            }
            if (lastOffsetOfPartition == lastSuccessfullWriteOffset) {
                if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                    Kafka10ClientImpl.logger.info((Object)("Last offset of (" + topic + ":" + partitionId + ") - " + lastOffsetOfPartition + " is equal to " + lastSuccessfullWriteOffset + "(lastSuccessfullWriteOffset), hence will try sending data again."));
                }
                return lastSuccessfullWriteOffset;
            }
            if (lastOffsetOfPartition < lastSuccessfullWriteOffset) {
                throw new RuntimeException("Last offset - " + lastOffsetOfPartition + " in (" + topic + "-" + partitionId + ") is less than last successfull write offset - " + lastSuccessfullWriteOffset + " locally avaialble. This happens when the new leader was a slow replica. Hence the data won't written until the offset is equal or greater than the current successful offset since it can potentially break E1P.");
            }
            if (lastOffsetOfPartition > lastSuccessfullWriteOffset) {
                long startOffset = lastSuccessfullWriteOffset;
                consumer.seek(tp, startOffset);
                if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                    Kafka10ClientImpl.logger.info((Object)("Will be consuming data from " + startOffset + " to " + lastOffsetOfPartition + " to verify if data has been already written to the topic - " + topic + ":" + partitionId));
                }
                while (startOffset <= lastOffsetOfPartition) {
                    final ConsumerRecords<byte[], byte[]> records = consumer.poll(1000L);
                    if (records != null && records.count() > 0) {
                        for (final ConsumerRecord<byte[], byte[]> record : records) {
                            startOffset = record.offset() + 1L;
                            final byte[] superset = record.value();
                            int subsetIndex = 0;
                            for (int supersetIndex = 0; supersetIndex < superset.length; ++supersetIndex) {
                                if (superset[supersetIndex] == subset[subsetIndex]) {
                                    if (++subsetIndex >= subset.length) {
                                        if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                                            Kafka10ClientImpl.logger.info((Object)("Data has already been sent successfully to (" + topic + ":" + partitionId + ") and its offset is " + record.offset() + "."));
                                        }
                                        return record.offset();
                                    }
                                }
                                else {
                                    if (superset.length - supersetIndex <= subset.length) {
                                        break;
                                    }
                                    subsetIndex = 0;
                                }
                            }
                        }
                    }
                }
            }
            if (Kafka10ClientImpl.logger.isInfoEnabled()) {
                Kafka10ClientImpl.logger.info((Object)("Took " + (System.currentTimeMillis() - startTime) + "ms to verify if the messages was already written in " + topic + "-" + partitionId));
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Problem while verifying send failure to (" + topic + "-" + partitionId + ")." + e);
        }
        finally {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        }
        return lastSuccessfullWriteOffset;
    }
    
    @Override
    public long syncSend(final String topic, final int partition, final byte[] key, final byte[] value) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return super.syncSend(topic, partition, key, value);
        }
        catch (Exception e) {
            if (e instanceof InterruptException) {
                Thread.currentThread().interrupt();
                throw new InterruptedException(e.getMessage());
            }
            try {
				throw e;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
        return 0L;
    }
    
    @Override
    public void closeProducer() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close(this.recordMetadataWaitMs, TimeUnit.MILLISECONDS);
        }
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        Kafka10ClientImpl.logger = Logger.getLogger((Class)Kafka10ClientImpl.class);
    }
    
    class SecurityAccess {
		public void disopen() {
			
		}
    }
}
