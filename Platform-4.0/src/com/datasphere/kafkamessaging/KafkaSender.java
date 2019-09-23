package com.datasphere.kafkamessaging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.classloading.ModuleClassLoader;
import com.datasphere.intf.Formatter;
import com.datasphere.kafka.ConsumerIntf;
import com.datasphere.kafka.KafkaConstants;
import com.datasphere.kafka.KafkaException;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.KafkaMessageAndOffset;
import com.datasphere.kafka.KafkaNode;
import com.datasphere.kafka.KafkaUtilsIntf;
import com.datasphere.kafka.Offset;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.kafka.PartitionState;
import com.datasphere.kafka.PositionedBuffer;
import com.datasphere.kafka.ProducerIntf;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.components.Stream;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;

public class KafkaSender implements KafkaSenderIntf
{
    private static final Logger logger;
    private final MDRepository mdRepository;
    private final String kafkaTopicName;
    private final KeyFactory key_factory;
    final MetaInfo.Stream streamInfo;
    protected final boolean isEncrypted;
    final PositionedBuffer[] dataBuffers;
    private final boolean isRecoveryEnabled;
    private final Stream streamRuntime;
    private ScheduledThreadPoolExecutor flushProcess;
    private volatile boolean isStarted;
    private final int num_partitions;
    private final Properties producerProperties;
    KafkaFlusher[] flushers;
    private String format;
    private Formatter formatter;
    boolean isAvro;
    KafkaUtilsIntf kafkaUtils;
    List<String> kafkaBrokers;
    Map<String, String> securityProperties;
    private final int flushTimeout;
    
    public KafkaSender(final boolean isEncrypted, final Stream streamRuntime, final KeyFactory keyFactory, final boolean recoveryEnabled) throws Exception {
        this.mdRepository = MetadataRepository.getINSTANCE();
        this.isStarted = false;
        this.kafkaUtils = null;
        this.kafkaBrokers = null;
        this.isEncrypted = isEncrypted;
        this.streamInfo = streamRuntime.getMetaInfo();
        this.streamRuntime = streamRuntime;
        this.key_factory = keyFactory;
        this.isRecoveryEnabled = recoveryEnabled;
        this.producerProperties = new Properties();
        final List<String> bootstrapBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
        this.producerProperties.put("bootstrap.servers", bootstrapBrokers);
        this.producerProperties.put("acks", KafkaConstants.producer_acks);
        this.producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        this.producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producerProperties.put("buffer.memory", KafkaConstants.producer_buffer_memory);
        this.producerProperties.put("batch.size", 0);
        this.producerProperties.put("max.request.size", KafkaConstants.producer_max_request_size);
        this.producerProperties.put("retries", 0);
        this.producerProperties.put("linger.ms", 0);
        this.producerProperties.put("reconnect.backoff.ms", 10000);
        this.producerProperties.put("retry.backoff.ms", 10000);
        this.kafkaTopicName = KafkaStreamUtils.createTopicName(this.streamInfo);
        final MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(this.streamInfo);
        this.num_partitions = KafkaStreamUtils.getPartitionsCount(this.streamInfo, streamPropset);
        this.securityProperties = KafkaStreamUtils.getSecurityProperties(streamPropset);
        if (this.securityProperties != null) {
            this.producerProperties.put("securityconfig", this.securityProperties);
        }
        this.kafkaBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
        this.kafkaUtils = KafkaStreamUtils.loadKafkaUtilsClass(this.streamInfo);
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final ModuleClassLoader mcl = (ModuleClassLoader)this.kafkaUtils.getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader(mcl);
        this.dataBuffers = new PSPositionedBuffer[this.num_partitions];
        final int batchSize = KafkaStreamUtils.getBatchSize(this.streamInfo);
        for (int ii = 0; ii < this.num_partitions; ++ii) {
            (this.dataBuffers[ii] = new PSPositionedBuffer(ii, batchSize, this.producerProperties)).setRecoveryEnabled(this.isRecoveryEnabled);
        }
        this.flushTimeout = KafkaStreamUtils.getLingerTime(this.streamInfo);
        if (streamPropset.properties != null) {
            this.format = (String)streamPropset.properties.get("dataformat");
            if (this.format != null && this.format.equalsIgnoreCase("avro")) {
                this.formatter = this.getFormatterInstance(this.format);
                if (this.formatter == null) {
                    throw new Exception("Allowed dataformat is only avro or leave it blank to default to Striim format. Format passed in : " + this.format);
                }
                this.isAvro = true;
            }
        }
        this.setupKafkaFlushers();
        Thread.currentThread().setContextClassLoader(cl);
    }
    
    private Formatter getFormatterInstance(final String format) throws Exception {
        if (format.equalsIgnoreCase("avro")) {
            final Map<String, Object> avroFormatter_Properties = new HashMap<String, Object>();
            final String schemFileName = this.streamInfo.getFullName().concat("_schema.avsc");
            avroFormatter_Properties.put("schemaFileName", schemFileName);
            final UUID type_uuid = this.streamInfo.getDataType();
            if (type_uuid != null) {
                final MetaInfo.Type type = (MetaInfo.Type)this.mdRepository.getMetaObjectByUUID(type_uuid, HSecurityManager.TOKEN);
                avroFormatter_Properties.put("TypeName", type.getName());
                final Class<?> typeClass = ClassLoader.getSystemClassLoader().loadClass(type.className);
                avroFormatter_Properties.put("EventType", "ContainerEvent");
                Field[] fields = typeClass.getDeclaredFields();
                final Field[] typedEventFields = new Field[fields.length - 1];
                int i = 0;
                for (final Field field : fields) {
                    if (Modifier.isPublic(field.getModifiers())) {
                        if (!"mapper".equals(field.getName())) {
                            typedEventFields[i] = field;
                            ++i;
                        }
                    }
                }
                fields = typedEventFields;
                final String formatterClassName = "com.datasphere.proc.AvroFormatter";
                final Class<?> formatterClass = Class.forName(formatterClassName, false, ClassLoader.getSystemClassLoader());
                final Formatter avroFormatter = (Formatter)formatterClass.getConstructor(Map.class, Field[].class).newInstance(avroFormatter_Properties, fields);
                return avroFormatter;
            }
        }
        return null;
    }
    
    private void setupKafkaFlushers() throws Exception {
        assert !this.isStarted;
        this.flushers = new KafkaFlusher[KafkaConstants.flush_threads];
        for (int ij = 0; ij < this.num_partitions; ++ij) {
            final int partition = ij % KafkaConstants.flush_threads;
            if (this.flushers[partition] == null) {
                this.flushers[partition] = new KafkaFlusher(this.streamRuntime);
            }
            this.flushers[partition].add(this.dataBuffers[ij]);
        }
        this.flushProcess = new ScheduledThreadPoolExecutor(1);
        for (final KafkaFlusher flusher : this.flushers) {
            this.flushProcess.scheduleAtFixedRate(flusher, this.flushTimeout, this.flushTimeout, TimeUnit.MILLISECONDS);
        }
        if (this.isRecoveryEnabled) {
            try {
                this.setPartitionWaitPositions();
            }
            catch (Exception e) {
                KafkaSender.logger.warn((Object)("Couldn't set wait positions per partition for Kafka Stream: " + this.streamInfo.getFullName() + ". Will crash the parent application! Reason -> " + e.getMessage()), (Throwable)e);
                throw e;
            }
        }
        this.isStarted = true;
        if (KafkaSender.logger.isInfoEnabled()) {
            KafkaSender.logger.info((Object)("Created KafkaSender to write to topic: " + this.kafkaTopicName + " for stream : " + this.streamInfo.getFullName()));
        }
    }
    
    @Override
    public synchronized void stop() {
        this.isStarted = false;
        if (this.flushProcess != null) {
            try {
                this.flushProcess.shutdown();
                if (!this.flushProcess.awaitTermination(100L, TimeUnit.MILLISECONDS)) {
                    KafkaSender.logger.warn((Object)("Unable to terminate Kafka flusher thread in 1 second for topic: " + this.kafkaTopicName + ". Will force terminate!"));
                    this.flushProcess.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                KafkaSender.logger.warn((Object)("Kafka flusher thread interrupted to force terminate for topic: " + this.kafkaTopicName), (Throwable)e);
            }
        }
        for (final PositionedBuffer positionedBuffer : this.dataBuffers) {
            try {
                positionedBuffer.closeProducer();
            }
            catch (Exception e2) {
                KafkaSender.logger.warn((Object)"Error while trying to stopping Kafka sender, it can be ignored", (Throwable)e2);
                KafkaSender.logger.error((Object)e2.getMessage(), (Throwable)e2);
            }
        }
        if (KafkaSender.logger.isInfoEnabled()) {
            KafkaSender.logger.info((Object)("Kafka sender for topic: " + this.kafkaTopicName + " stopped"));
        }
    }
    
    @Override
    public synchronized boolean send(final ITaskEvent data) throws Exception {
        if (!this.isStarted) {
            return false;
        }
        if (data instanceof CommandEvent) {
            for (final PositionedBuffer dataBuffer : this.dataBuffers) {
                dataBuffer.put(data);
            }
        }
        else {
            for (final DARecord event : data.batch()) {
                final int partitionId = this.getPartitionId(event);
                event.setLagMarker(((TaskEvent)data).getLagMarker());
                this.dataBuffers[partitionId].put(event);
            }
        }
        return true;
    }
    
    @Override
    public Position getComponentCheckpoint() {
        final PathManager result = new PathManager();
        for (final PositionedBuffer b : this.dataBuffers) {
            result.mergeHigherPositions(b.recordsInMemory);
        }
        return result.toPosition();
    }
    
    private void setPartitionWaitPositions() throws Exception {
        final String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
        List<String> kafkaBrokers = (List<String>)KafkaConstants.broker_address_list;
        if (kafkaBrokers == null || kafkaBrokers.isEmpty()) {
            kafkaBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
        }
        final String[] hostPort = kafkaBrokers.get(0).split(":");
        assert hostPort.length == 2;
        final String clientId = "Sender_" + this.kafkaTopicName + "_" + KafkaPuller.getServerId();
        final MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(this.streamInfo);
        final int numPartitions = KafkaStreamUtils.getPartitionsCount(this.streamInfo, streamPropset);
        final List<Integer> kafkaTopicPartitions = new ArrayList<Integer>();
        while (kafkaTopicPartitions.size() < numPartitions) {
            kafkaTopicPartitions.add(kafkaTopicPartitions.size());
        }
        final Map<KafkaNode, List<Integer>> kafkaTopicLeaders = (Map<KafkaNode, List<Integer>>)this.kafkaUtils.getBrokerPartitions((List)kafkaBrokers, this.kafkaTopicName, (Map)this.securityProperties, clientId);
        final Map<Integer, KafkaNode> dataTopicPartitionToServer = new HashMap<Integer, KafkaNode>();
        for (final Map.Entry<KafkaNode, List<Integer>> entry : kafkaTopicLeaders.entrySet()) {
            for (final Integer i : entry.getValue()) {
                dataTopicPartitionToServer.put(i, entry.getKey());
            }
        }
        if (KafkaSender.logger.isInfoEnabled()) {
            KafkaSender.logger.info((Object)"Data topic metadata:");
            KafkaSender.logger.info((Object)dataTopicPartitionToServer);
        }
        final Map<KafkaNode, List<Integer>> checkpointTopicLeaders = (Map<KafkaNode, List<Integer>>)this.kafkaUtils.getBrokerPartitions((List)kafkaBrokers, checkpointTopicName, (Map)this.securityProperties, clientId);
        final Map<Integer, KafkaNode> checkpointTopicPartitionToServer = new HashMap<Integer, KafkaNode>();
        for (final Map.Entry<KafkaNode, List<Integer>> entry2 : checkpointTopicLeaders.entrySet()) {
            for (final Integer j : entry2.getValue()) {
                checkpointTopicPartitionToServer.put(j, entry2.getKey());
            }
        }
        if (KafkaSender.logger.isInfoEnabled()) {
            KafkaSender.logger.info((Object)"Checkpoint topic metadata:");
            KafkaSender.logger.info((Object)dataTopicPartitionToServer);
        }
        for (final PositionedBuffer positionedBuffer : this.dataBuffers) {
            final String clientIdByPartition = clientId.concat("_").concat(String.valueOf(positionedBuffer.partitionId));
            if (KafkaSender.logger.isInfoEnabled()) {
                KafkaSender.logger.info((Object)("KafkaSender -> Getting last record for " + checkpointTopicName + ":" + positionedBuffer.partitionId + " from " + checkpointTopicPartitionToServer.get(positionedBuffer.partitionId)));
            }
            final KafkaMessageAndOffset rawObject = this.kafkaUtils.getLastRecordFromTopic(checkpointTopicName, positionedBuffer.partitionId, (KafkaNode)checkpointTopicPartitionToServer.get(positionedBuffer.partitionId), clientIdByPartition, (Map)this.securityProperties);
            OffsetPosition partitionCheckpoint = null;
            if (rawObject == null) {
                if (KafkaSender.logger.isInfoEnabled()) {
                    KafkaSender.logger.info((Object)("KafkaSender ->  Last record for " + checkpointTopicName + ":" + positionedBuffer.partitionId + " is null"));
                }
            }
            else {
                partitionCheckpoint = (OffsetPosition)KryoSingleton.read(rawObject.data, false);
                if (KafkaSender.logger.isInfoEnabled()) {
                    KafkaSender.logger.info((Object)("KafkaSender ->  Last record for " + checkpointTopicName + ":" + positionedBuffer.partitionId + " : " + partitionCheckpoint));
                }
            }
            if (KafkaSender.logger.isInfoEnabled()) {
                KafkaSender.logger.info((Object)("KafkaSender -> Getting last record for " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + " from " + dataTopicPartitionToServer.get(positionedBuffer.partitionId)));
            }
            final KafkaMessageAndOffset messageAndOffset = this.kafkaUtils.getLastRecordFromTopic(this.kafkaTopicName, positionedBuffer.partitionId, (KafkaNode)dataTopicPartitionToServer.get(positionedBuffer.partitionId), clientIdByPartition, (Map)this.securityProperties);
            if (messageAndOffset != null) {
                if (KafkaSender.logger.isInfoEnabled()) {
                    KafkaSender.logger.info((Object)("KafkaSender ->  Last record for " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + " at offset :" + messageAndOffset.offset));
                }
                if (partitionCheckpoint == null) {
                    partitionCheckpoint = new OffsetPosition((Offset)new KafkaLongOffset(-1L), System.currentTimeMillis());
                }
                if (partitionCheckpoint.getOffset().compareTo((Offset)new KafkaLongOffset(messageAndOffset.offset)) <= 0) {
                    if (KafkaSender.logger.isInfoEnabled()) {
                        KafkaSender.logger.info((Object)("KafkaSender -> Updating checkpoint for : " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + " : checkpoint offset -> " + partitionCheckpoint.getOffset() + " : data offset -> " + messageAndOffset.offset));
                    }
                    final Properties properties = new Properties();
                    properties.put("bootstrap.brokers", kafkaBrokers);
                    properties.put("topic", this.kafkaTopicName);
                    final PartitionState ps_1 = new PartitionState(positionedBuffer.partitionId);
                    properties.put("ps", ps_1);
                    properties.put("clientId", clientIdByPartition);
                    if (this.securityProperties != null) {
                        properties.put("securityconfig", this.securityProperties);
                    }
                    final ConsumerIntf consumer = this.kafkaUtils.getKafkaConsumer(properties);
                    try {
                        while (partitionCheckpoint.getOffset().compareTo((Offset)new KafkaLongOffset(messageAndOffset.offset)) <= 0) {
                            final List<KafkaMessageAndOffset> msgs = (List<KafkaMessageAndOffset>)this.kafkaUtils.fetch(consumer, (long)partitionCheckpoint.getOffset().getOffset(), (List)kafkaBrokers, this.kafkaTopicName, positionedBuffer.partitionId, (KafkaNode)dataTopicPartitionToServer.get(positionedBuffer.partitionId), clientIdByPartition, (Map)this.securityProperties);
                            if (msgs != null) {
                                if (msgs.size() == 0) {
                                    KafkaSender.logger.warn((Object)("Data between offsets " + partitionCheckpoint.getOffset() + " and " + messageAndOffset.offset + " is not present, which means checkpoint won't reflect upto date information and can cause duplicate results.Checkpoint will advanced to offset " + (messageAndOffset.offset + 1L) + ", Event processing will continue."));
                                    partitionCheckpoint.setOffset((Offset)new KafkaLongOffset(messageAndOffset.nextOffset));
                                    break;
                                }
                                partitionCheckpoint = this.updateCheckpoint(partitionCheckpoint, msgs, positionedBuffer.partitionId);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw new KafkaException("Could not set partition wait positions", (Throwable)e);
                    }
                    finally {
                        consumer.close();
                    }
                    final byte[] bytes = KryoSingleton.write(partitionCheckpoint, false);
                    ((PSPositionedBuffer)positionedBuffer).producer.write(checkpointTopicName, positionedBuffer.partitionId, bytes, clientIdByPartition, (List)kafkaBrokers, 0L, 3);
                    if (KafkaSender.logger.isInfoEnabled()) {
                        KafkaSender.logger.info((Object)("KafkaSender -> Updated checkpoint for : " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + " : checkpoint offset -> " + partitionCheckpoint.getOffset() + " : data offset -> " + messageAndOffset.offset));
                    }
                }
                else if (KafkaSender.logger.isInfoEnabled()) {
                    KafkaSender.logger.info((Object)("KafkaSender -> Checkpoint upto date: " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + " : " + partitionCheckpoint.getOffset() + " : " + messageAndOffset.offset));
                }
            }
            else if (partitionCheckpoint == null || partitionCheckpoint.isEmpty()) {
                if (KafkaSender.logger.isInfoEnabled()) {
                    KafkaSender.logger.info((Object)("KafkaSender ->  Last record for " + this.kafkaTopicName + " on partition : " + positionedBuffer.partitionId + " is NULL"));
                }
                partitionCheckpoint = new OffsetPosition((Offset)new KafkaLongOffset(-1L), System.currentTimeMillis());
            }
            if (!partitionCheckpoint.isEmpty()) {
                positionedBuffer.waitPosition = new PathManager((Position)partitionCheckpoint);
                if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                    Logger.getLogger("KafkaStreams").debug((Object)("Kafka partition checkpoint for " + this.kafkaTopicName + ":" + positionedBuffer.partitionId + "..."));
                    Utility.prettyPrint(positionedBuffer.waitPosition);
                }
            }
        }
    }
    
    private OffsetPosition updateCheckpoint(final OffsetPosition partitionCheckpoint, final List<KafkaMessageAndOffset> msgs, final int partitionId) throws KafkaException {
        final PathManager updatedCheckpoint = new PathManager((Position)partitionCheckpoint);
        long offset = (long)partitionCheckpoint.getOffset().getOffset();
        for (final KafkaMessageAndOffset km : msgs) {
            final ByteBuffer payload = ByteBuffer.wrap(km.data);
            while (payload.hasRemaining()) {
                try {
                    final int size = payload.getInt();
                    if (payload.position() + size <= payload.limit() && size != 0) {
                        final byte[] current_bytes = new byte[size];
                        payload.get(current_bytes, 0, size);
                        final Object rawObject = this.deserialize(current_bytes);
                        if (rawObject instanceof DARecord) {
                            final DARecord e = (DARecord)rawObject;
                            if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                                Logger.getLogger("KafkaStreams").debug((Object)("Scanning for checkpoint " + this.kafkaTopicName + ":" + partitionId + "@" + km.offset + " ~ " + e.position));
                            }
                            updatedCheckpoint.mergeHigherPositions(e.position);
                        }
                        else {
                            if (rawObject instanceof CommandEvent) {
                                continue;
                            }
                            KafkaSender.logger.error((Object)("Unexpected object type found in kafka stream while updating checkpoint: " + rawObject.getClass() + "... " + rawObject));
                        }
                        continue;
                    }
                }
                catch (Exception e2) {
                    throw new KafkaException("Could not update checkpoit for " + partitionId, (Throwable)e2);
                }
                break;
            }
            offset = km.nextOffset;
        }
        final Position p = updatedCheckpoint.toPosition();
        final OffsetPosition result = new OffsetPosition(p, (Offset)new KafkaLongOffset(offset), partitionCheckpoint.getTimestamp());
        return result;
    }
    
    private Object deserialize(final byte[] current_bytes) throws Exception {
        return KryoSingleton.read(current_bytes, false);
    }
    
    private int getPartitionId(final DARecord event) {
        if (this.key_factory != null) {
            final RecordKey record_key = this.key_factory.makeKey(event.data);
            final int result = Math.abs(record_key.hashCode() % this.dataBuffers.length);
            return result;
        }
        return 0;
    }
    
    @Override
    public void close() {
        this.formatter = null;
    }
    
    public static void writeToFile(final String event) {
        try {
            final File file = new File("/Users/Vijay/Documents/HD/workspace/workspace.txt");
            if (!file.exists()) {
                file.createNewFile();
            }
            final FileWriter fw = new FileWriter(file.getAbsoluteFile());
            final BufferedWriter bw = new BufferedWriter(fw);
            bw.append((CharSequence)event);
            bw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public Map getStats() {
        long total_event_count = 0L;
        long total_bytes_count = 0L;
        long total_time = 0L;
        long max_latency = 0L;
        long total_iterations = 0L;
        long total_wait_count = 0L;
        final Map sMap = new HashMap();
        for (final PositionedBuffer buffer : this.dataBuffers) {
            final PSPositionedBuffer positionedBuffer = (PSPositionedBuffer)buffer;
            total_event_count += positionedBuffer.stats.sentCount;
            total_bytes_count += positionedBuffer.stats.bytes;
            total_time += positionedBuffer.stats.time;
            max_latency = Math.max(max_latency, positionedBuffer.stats.maxLatency);
            total_iterations += positionedBuffer.stats.iterations;
            total_wait_count += positionedBuffer.stats.waitCount;
        }
        if (total_time != 0L) {
            final long total_time_seconds = total_time / 1000L;
            if (total_time_seconds != 0L) {
                final long events_per_sec = total_event_count / total_time_seconds;
                final long megs_per_sec = total_bytes_count / 1000000L / total_time_seconds;
                sMap.put("events_per_sec", events_per_sec);
                sMap.put("megs_per_sec", megs_per_sec);
            }
            else {
                sMap.put("events_per_sec", 0);
                sMap.put("megs_per_sec", 0);
            }
        }
        else {
            sMap.put("events_per_sec", 0);
            sMap.put("megs_per_sec", 0);
        }
        if (total_iterations != 0L) {
            final long avg_latency = total_time / total_iterations;
            sMap.put("avg_latency", avg_latency);
        }
        else {
            sMap.put("avg_latency", 0);
        }
        sMap.put("total_event_count", total_event_count);
        sMap.put("total_bytes_count", total_bytes_count);
        sMap.put("max_latency", max_latency);
        sMap.put("total_wait_count", total_wait_count);
        return sMap;
    }
    
    static {
        logger = Logger.getLogger("KafkaStreams");
    }
    
    class PSPositionedBuffer extends PositionedBuffer
    {
        public ProducerIntf producer;
        public KafkaPartitionSendStats stats;
        
        public PSPositionedBuffer(final int partition_id, final int sendBufferSize, final Properties producerProperties) {
            super(partition_id, sendBufferSize);
            this.stats = new KafkaPartitionSendStats();
            this.producer = KafkaSender.this.kafkaUtils.getKafkaProducer(producerProperties);
        }
        
        protected byte[] convertToBytes(final Object data) throws Exception {
            if (KafkaSender.this.isAvro) {
                return KafkaSender.this.formatter.format(data);
            }
            return KryoSingleton.write(data, KafkaSender.this.isEncrypted);
        }
        
        public synchronized void put(final DARecord data) throws Exception {
            if (super.isRecoveryEnabled && data.position != null) {
                data.position = data.position.createAugmentedPosition(KafkaSender.this.streamInfo.uuid, String.valueOf(this.partitionId));
                if (this.isBeforeWaitPosition(data.position)) {
                    if (Logger.getLogger("Recovery").isDebugEnabled()) {
                        Logger.getLogger("Recovery").debug((Object)(KafkaSender.this.streamInfo.name + " dropping input which precedes the checkpoint: " + data.position));
                    }
                    return;
                }
            }
            final byte[] bytes = this.convertToBytes(data);
            if (this.byteBuffer.position() + 4 + bytes.length > this.byteBuffer.limit()) {
                this.isFull = true;
                while (this.isFull) {
                    this.wait(KafkaConstants.flush_period);
                }
                ++this.waitStats;
            }
            this.byteBuffer.putInt(bytes.length);
            this.byteBuffer.put(bytes);
            ++this.pendingCount;
            if (this.isRecoveryEnabled) {
                this.recordsInMemory.mergeHigherPositions(data.position);
            }
        }
        
        public synchronized void put(final ITaskEvent data) throws Exception {
            final byte[] bytes = this.convertToBytes(data);
            if (this.byteBuffer.position() + 4 + bytes.length > this.byteBuffer.limit()) {
                this.isFull = true;
                while (this.isFull) {
                    this.wait(KafkaConstants.flush_period);
                }
                ++this.waitStats;
            }
            this.byteBuffer.putInt(bytes.length);
            this.byteBuffer.put(bytes);
            ++this.pendingCount;
        }
        
        public synchronized void flushToKafka() throws Exception {
            if (!KafkaSender.this.isStarted) {
                return;
            }
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try {
                final ModuleClassLoader mcl = (ModuleClassLoader)KafkaSender.this.kafkaUtils.getClass().getClassLoader();
                Thread.currentThread().setContextClassLoader(mcl);
                if (this.byteBuffer.position() == 0) {
                    return;
                }
                if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                    Logger.getLogger("KafkaStreams").debug((Object)("Flushing to kafka buffer partition " + KafkaSender.this.kafkaTopicName + ":" + this.partitionId));
                    Utility.prettyPrint(this.recordsInMemory);
                }
                final byte[] kafkaBytes = new byte[this.byteBuffer.position()];
                System.arraycopy(this.byteBuffer.array(), 0, kafkaBytes, 0, this.byteBuffer.position());
                final long tmp_pos = this.byteBuffer.position();
                final long tmp_count = this.pendingCount;
                final long currenTime = System.currentTimeMillis();
                final long writeOffset = this.producer.write(KafkaSender.this.kafkaTopicName, this.partitionId, kafkaBytes, KafkaSender.this.streamInfo.getFullName(), (List)KafkaSender.this.kafkaBrokers, this.lastSuccessfulWriteOffset, 3);
                if (writeOffset < 0L) {
                    return;
                }
                final long endTime = System.currentTimeMillis();
                this.stats.record(endTime - currenTime, tmp_pos, tmp_count, this.waitStats);
                this.lastSuccessfulWriteOffset = writeOffset;
            }
            catch (Exception e) {
                throw e;
            }
            finally {
                this.byteBuffer.clear();
                this.pendingCount = 0;
                this.recordsInMemory = new PathManager();
                Thread.currentThread().setContextClassLoader(cl);
                this.isFull = false;
                this.notifyAll();
            }
        }
        
        public void closeProducer() {
            final ClassLoader cl = Thread.currentThread().getContextClassLoader();
            final ModuleClassLoader mcl = (ModuleClassLoader)KafkaSender.this.kafkaUtils.getClass().getClassLoader();
            Thread.currentThread().setContextClassLoader(mcl);
            this.producer.close();
            Thread.currentThread().setContextClassLoader(cl);
        }
    }
}
