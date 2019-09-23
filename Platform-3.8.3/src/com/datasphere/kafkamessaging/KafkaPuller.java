package com.datasphere.kafkamessaging;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.jmqmessaging.KafkaReceiverInfo;
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
import com.datasphere.kafka.ProducerIntf;
import com.datasphere.messaging.Handler;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.KafkaSourcePosition;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.KafkaDistributedRcvr;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.UUID;

public class KafkaPuller implements Runnable
{
    private static Logger logger;
    private static final long STATS_PERIOD_MILLIS = 10000L;
    final String kafkaTopicName;
    final UUID streamUuid;
    final Handler rcvr;
    final String clientId;
    private final boolean isSingleThreaded = true;
    final boolean isRecoveryEnabled;
    final MetaInfo.Stream streamMetaObject;
    final KafkaReceiverInfo receiverInfo;
    final Set<PartitionState> partitions;
    private Pair<KafkaPullerTask, Future>[] kafkaPullerTasks;
    private ExecutorService dataExecutorService;
    Pair<StreamCheckpointWriterTask, Future> kafkaCheckpointTasks;
    private ScheduledThreadPoolExecutor checkpointExecutorService;
    Pair<KafkaPullerStatsTask, Future> kafkaStatsTasks;
    private ScheduledThreadPoolExecutor statsExecutorService;
    private List<String> boostrapKafkaBrokers;
    private List<Integer> boostrapKafkaPartitions;
    private volatile boolean dontStop;
    KafkaUtilsIntf kafkaUtils;
    private Map<KafkaNode, Set<PartitionState>> node2Partitions;
    List<String> bootstrapBrokers;
    ProducerIntf producerIntf;
    Map<String, String> securityProperties;
    KafkaPullerTask dataTask;
    Thread dataTaskThread;
    
    public KafkaPuller(final MetaInfo.Stream streamMetaObject, final KafkaReceiverInfo receiverInfo, final Handler rcvr) {
        this.partitions = new HashSet<PartitionState>();
        this.dontStop = true;
        this.kafkaUtils = null;
        this.node2Partitions = new HashMap<KafkaNode, Set<PartitionState>>();
        this.receiverInfo = receiverInfo;
        this.streamMetaObject = streamMetaObject;
        this.kafkaTopicName = receiverInfo.getTopic_name();
        this.streamUuid = streamMetaObject.getUuid();
        this.rcvr = rcvr;
        this.clientId = "Puller_" + this.kafkaTopicName + "_" + getServerId();
        this.isRecoveryEnabled = ((KafkaDistributedRcvr)rcvr).isRecoveryEnabled();
        if (KafkaPuller.logger.isInfoEnabled()) {
            KafkaPuller.logger.info((Object)("Kafka Puller for " + receiverInfo.getRcvr_name() + " created to read from " + this.kafkaTopicName));
        }
    }
    
    private void createProducer() {
        final Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", this.bootstrapBrokers = KafkaStreamUtils.getBrokerAddress(this.streamMetaObject));
        producerProperties.put("acks", KafkaConstants.producer_acks);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put("buffer.memory", KafkaConstants.producer_buffer_memory);
        producerProperties.put("batch.size", 0);
        producerProperties.put("linger.ms", 0);
        producerProperties.put("retries", 0);
        producerProperties.put("max.request.size", KafkaConstants.producer_max_request_size);
        producerProperties.put("reconnect.backoff.ms", 10000);
        producerProperties.put("retry.backoff.ms", 10000);
        this.securityProperties = KafkaStreamUtils.getSecurityProperties(this.streamMetaObject.propertySet);
        if (this.securityProperties != null) {
            producerProperties.put("securityconfig", this.securityProperties);
        }
        this.producerIntf = this.kafkaUtils.getKafkaProducer(producerProperties);
    }
    
    public static UUID getServerId() {
        if (Server.getServer() != null) {
            return Server.getServer().getServerID();
        }
        return new UUID(System.currentTimeMillis());
    }
    
    public void init(final List<Integer> kafkaPartitions, final List<String> kafkaBrokers) throws Exception {
        this.boostrapKafkaPartitions = kafkaPartitions;
        this.boostrapKafkaBrokers = kafkaBrokers;
        this.kafkaUtils = KafkaStreamUtils.loadKafkaUtilsClass(this.streamMetaObject);
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.kafkaUtils.getClass().getClassLoader());
        this.createProducer();
        final Map<Integer, PartitionState> kpir = this.initConsumersForKafka(kafkaPartitions, kafkaBrokers);
        if (this.isRecoveryEnabled) {
            this.initRecoveryRelatedMetadata(kafkaBrokers, kafkaPartitions, kpir);
        }
        Thread.currentThread().setContextClassLoader(cl);
    }
    
    void reInitKafka() throws Exception {
        this.createProducer();
        this.init(this.boostrapKafkaPartitions, this.boostrapKafkaBrokers);
    }
    
    Map<Integer, PartitionState> initConsumersForKafka(final List<Integer> kafkaPartitions, final List<String> kafkaBrokers) throws KafkaException {
        final Map<Integer, PartitionState> result = new HashMap<Integer, PartitionState>();
        final Map<KafkaNode, List<Integer>> kafkaPartitionLeaders = (Map<KafkaNode, List<Integer>>)this.kafkaUtils.getBrokerPartitions((List)kafkaBrokers, this.kafkaTopicName, (Map)this.securityProperties, this.clientId);
        if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
            KafkaPuller.logger.info((Object)kafkaPartitionLeaders);
        }
        if (kafkaPartitionLeaders == null || kafkaPartitionLeaders.isEmpty()) {
            throw new KafkaException("Failed to communicate with Kafka Brokers [" + kafkaBrokers + "] to find Leader for [" + this.kafkaTopicName + ", with partitions " + kafkaPartitions + "]");
        }
        for (final Map.Entry<KafkaNode, List<Integer>> entry : kafkaPartitionLeaders.entrySet()) {
            final KafkaNode broker = entry.getKey();
            final List<Integer> partitionIds = entry.getValue();
            final Set<PartitionState> localSet = new HashSet<PartitionState>();
            for (final Integer partitionId : partitionIds) {
                final PartitionState ps = new PartitionState((int)partitionId);
                ps.dataTopicBroker = broker;
                this.partitions.add(ps);
                localSet.add(ps);
                result.put(partitionId, ps);
            }
            this.node2Partitions.put(broker, localSet);
        }
        return result;
    }
    
    private void initRecoveryRelatedMetadata(final List<String> kafkaBrokers, final List<Integer> kafkaPartitions, final Map<Integer, PartitionState> partitions) throws KafkaException {
        final String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
        final Map<KafkaNode, List<Integer>> checkpointTopicLeaders = (Map<KafkaNode, List<Integer>>)this.kafkaUtils.getBrokerPartitions((List)kafkaBrokers, checkpointTopicName, (Map)this.securityProperties, this.clientId);
        if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
            KafkaPuller.logger.info((Object)checkpointTopicLeaders);
        }
        if (checkpointTopicLeaders == null || checkpointTopicLeaders.isEmpty()) {
            throw new KafkaException("Failed to communicate with Kafka Brokers [" + kafkaBrokers + "] to find Leader for [" + checkpointTopicName + ", with partitions " + kafkaPartitions + "]");
        }
        for (final Map.Entry<KafkaNode, List<Integer>> entry : checkpointTopicLeaders.entrySet()) {
            final KafkaNode broker = entry.getKey();
            final List<Integer> partitionIds = entry.getValue();
            for (final Integer partitionId : partitionIds) {
                final String clientIdByPartition = this.clientId.concat("_").concat("meta").concat("_").concat(String.valueOf(partitionId));
                final PartitionState ps = partitions.get(partitionId);
                if (ps == null) {
                    KafkaPuller.logger.error((Object)("Kafka partition information not found when trying to add checkpoint information: " + this.kafkaTopicName + ":" + partitionId));
                }
                else {
                    ps.checkpointTopicBroker = broker;
                    final KafkaMessageAndOffset rawObject = this.kafkaUtils.getLastRecordFromTopic(checkpointTopicName, (int)partitionId, ps.checkpointTopicBroker, clientIdByPartition, (Map)this.securityProperties);
                    OffsetPosition partitionCheckpoint = null;
                    if (rawObject == null) {
                        if (KafkaPuller.logger.isInfoEnabled()) {
                            KafkaPuller.logger.info((Object)("Kafka Puller -> Last record from checkpoint topic " + checkpointTopicName + " is NULL"));
                        }
                    }
                    else {
                        partitionCheckpoint = (OffsetPosition)KryoSingleton.read(rawObject.data, false);
                        if (KafkaPuller.logger.isInfoEnabled()) {
                            KafkaPuller.logger.info((Object)("Kafka Puller -> Last record from checkpoint topic " + checkpointTopicName + " is : " + partitionCheckpoint + " written at offset : " + rawObject.offset));
                        }
                    }
                    final KafkaMessageAndOffset messageAndOffset = this.kafkaUtils.getLastRecordFromTopic(this.kafkaTopicName, (int)partitionId, ps.dataTopicBroker, clientIdByPartition, (Map)this.securityProperties);
                    if (messageAndOffset != null) {
                        if (KafkaPuller.logger.isInfoEnabled()) {
                            KafkaPuller.logger.info((Object)("Kafka Puller -> Last record from data topic " + this.kafkaTopicName + " is at offset : " + messageAndOffset.offset));
                        }
                        if (partitionCheckpoint == null) {
                            partitionCheckpoint = new OffsetPosition((Offset)new KafkaLongOffset(-1L), System.currentTimeMillis());
                        }
                        if (partitionCheckpoint.getOffset().compareTo((Offset)new KafkaLongOffset(messageAndOffset.offset)) <= 0) {
                            if (KafkaPuller.logger.isInfoEnabled()) {
                                KafkaPuller.logger.info((Object)("Kafka Puller -> Updating checkpoint for : " + partitionId + " : checkpoint offset -> " + partitionCheckpoint.getOffset() + " : data offset -> " + messageAndOffset.offset));
                            }
                            final Properties properties = new Properties();
                            properties.put("bootstrap.brokers", kafkaBrokers);
                            properties.put("topic", this.kafkaTopicName);
                            final PartitionState ps_1 = new PartitionState((int)partitionId);
                            properties.put("ps", ps_1);
                            properties.put("clientId", clientIdByPartition);
                            if (this.securityProperties != null) {
                                properties.put("securityconfig", this.securityProperties);
                            }
                            final ConsumerIntf consumer = this.kafkaUtils.getKafkaConsumer(properties);
                            try {
                                while (partitionCheckpoint.getOffset().compareTo((Offset)new KafkaLongOffset(messageAndOffset.offset)) <= 0) {
                                    final List<KafkaMessageAndOffset> msgs = (List<KafkaMessageAndOffset>)this.kafkaUtils.fetch(consumer, (long)partitionCheckpoint.getOffset().getOffset(), (List)kafkaBrokers, this.kafkaTopicName, (int)partitionId, broker, clientIdByPartition, (Map)this.securityProperties);
                                    if (msgs != null) {
                                        if (msgs.size() == 0) {
                                            KafkaPuller.logger.warn((Object)("Data between offsets " + partitionCheckpoint.getOffset() + " and " + messageAndOffset.offset + " is not present, which means checkpoint won't reflect upto date information and can cause duplicate results.Checkpoint will advanced to offset " + messageAndOffset.nextOffset + ", Event processing will continue."));
                                            partitionCheckpoint.setOffset((Offset)new KafkaLongOffset(messageAndOffset.nextOffset));
                                            break;
                                        }
                                        partitionCheckpoint = this.updateCheckpoint(partitionCheckpoint, msgs, partitionId);
                                    }
                                }
                            }
                            catch (Exception e) {
                                throw new KafkaException("Could not get Kafka topic leaders", (Throwable)e);
                            }
                            finally {
                                if (consumer != null) {
                                    consumer.close();
                                }
                            }
                            final byte[] bytes = KryoSingleton.write(partitionCheckpoint, false);
                            this.producerIntf.write(checkpointTopicName, (int)partitionId, bytes, clientIdByPartition, (List)kafkaBrokers, 0L, 3);
                            if (KafkaPuller.logger.isInfoEnabled()) {
                                KafkaPuller.logger.info((Object)("Kafka Puller -> Updated checkpoint for : " + partitionId + " : checkpoint offset -> " + partitionCheckpoint.getOffset() + " : data offset -> " + messageAndOffset.offset));
                            }
                        }
                        else if (KafkaPuller.logger.isInfoEnabled()) {
                            KafkaPuller.logger.info((Object)("Kafka Puller -> Checkpoint upto date for partition: " + partitionId + " : " + partitionCheckpoint.getOffset() + " : " + messageAndOffset.offset));
                        }
                    }
                    else if (partitionCheckpoint == null || partitionCheckpoint.isEmpty()) {
                        if (KafkaPuller.logger.isInfoEnabled()) {
                            KafkaPuller.logger.info((Object)("KafkaPuller ->  Last record for " + this.kafkaTopicName + " on partition : " + partitionId + " is NULL"));
                        }
                        partitionCheckpoint = new OffsetPosition((Offset)new KafkaLongOffset(-1L), System.currentTimeMillis());
                    }
                    ps.partitionCheckpoint = new PathManager((Position)partitionCheckpoint);
                    ps.checkpointOffset = (long)partitionCheckpoint.getOffset().getOffset();
                    ps.checkpointTimestamp = partitionCheckpoint.getTimestamp();
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
                            KafkaPuller.logger.error((Object)("Unexpected object type found in kafka stream while updating checkpoint: " + rawObject.getClass() + "... " + rawObject));
                        }
                        continue;
                    }
                }
                catch (Exception e2) {
                    throw new KafkaException("Could not update Kafka checkpoint", (Throwable)e2);
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
    
    public synchronized void stop() throws InterruptedException {
        this.dontStop = false;
        if (this.isRecoveryEnabled) {
            this.kafkaCheckpointTasks.first.stop();
        }
        if (KafkaPuller.logger.isInfoEnabled()) {
            KafkaPuller.logger.info((Object)("Kafka Puller for " + this.receiverInfo.getRcvr_name() + " stopped"));
        }
    }
    
    public void startReadingFromTopic() throws Exception {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.kafkaUtils.getClass().getClassLoader());
        if (Logger.getLogger("KafkaStreams").isInfoEnabled()) {
            KafkaPuller.logger.info((Object)"Creating puller task for all consumers");
        }
        this.dataTask = new KafkaPullerTask(this, new HashSet<PartitionState>(this.partitions));
        (this.dataTaskThread = new Thread(this.dataTask)).start();
        if (KafkaPuller.logger.isDebugEnabled()) {
            final KafkaPullerStatsTask kafkaPullerStats = new KafkaPullerStatsTask(this.kafkaPullerTasks, this.receiverInfo.getRcvr_name());
            this.statsExecutorService = new ScheduledThreadPoolExecutor(1);
            final Future statsFuture = this.statsExecutorService.scheduleAtFixedRate(kafkaPullerStats, 10000L, 10000L, TimeUnit.MILLISECONDS);
            this.kafkaStatsTasks = new Pair<KafkaPullerStatsTask, Future>(kafkaPullerStats, statsFuture);
        }
        if (this.isRecoveryEnabled) {
            final StreamCheckpointWriterTask writerTask = new StreamCheckpointWriterTask(this);
            this.checkpointExecutorService = new ScheduledThreadPoolExecutor(1);
            final Future checkpointFuture = this.checkpointExecutorService.scheduleAtFixedRate(writerTask, 10000L, 10000L, TimeUnit.MILLISECONDS);
            this.kafkaCheckpointTasks = new Pair<StreamCheckpointWriterTask, Future>(writerTask, checkpointFuture);
        }
        Thread.currentThread().setContextClassLoader(cl);
        if (KafkaPuller.logger.isInfoEnabled()) {
            KafkaPuller.logger.info((Object)("Kafka Puller started reading from topic " + this.receiverInfo.getRcvr_name()));
        }
    }
    
    public void setStartPosition(final PartitionedSourcePosition position) {
        if (position == null) {
            return;
        }
        if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
            Logger.getLogger("KafkaStreams").debug((Object)("Set Kafka puller position " + position));
        }
        for (final PartitionState ps : this.partitions) {
            final String partitionIdString = String.valueOf(ps.partitionId);
            final SourcePosition sp = position.get(partitionIdString);
            final Boolean atOrAfter = position.getAtOrAfter(partitionIdString);
            if (sp instanceof KafkaSourcePosition) {
                final KafkaSourcePosition ksp = (KafkaSourcePosition)sp;
                ps.kafkaReadOffset = ksp.getKafkaReadOffset();
                if (!atOrAfter) {
                    continue;
                }
                final PartitionState partitionState = ps;
                ++partitionState.kafkaReadOffset;
            }
        }
    }
    
    public Position getComponentCheckpoint() {
        final PathManager result = new PathManager();
        for (final PartitionState ps : this.partitions) {
            result.mergeHigherPositions(ps.lastEmittedPosition);
        }
        if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
            Logger.getLogger("KafkaStreams").debug((Object)("Component checkpoint for " + this.kafkaTopicName + ":"));
            Utility.prettyPrint(result);
        }
        return result.toPosition();
    }
    
    @Override
    public void run() {
        try {
            this.startReadingFromTopic();
            while (this.dontStop) {
                this.checkTasks();
                Thread.sleep(5000L);
            }
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                if (!this.dontStop) {
                    Thread.currentThread().interrupt();
                }
                this.dontStop = false;
            }
            else {
                KafkaPuller.logger.error((Object)e.getMessage(), (Throwable)e);
                final FlowComponent flowComponent = this.rcvr.getOwner();
                flowComponent.notifyAppMgr(EntityType.STREAM, this.clientId, this.streamUuid, e, null, new Object[0]);
            }
        }
    }
    
    private synchronized void checkTasks() throws Exception {
        this.checkDataConsumptionTask();
        this.checkCheckpointTask();
        this.checkStatsTask();
    }
    
    private void checkDataConsumptionTask() throws Exception {
        if (this.dataTask != null && !this.dataTask.dontStop && !this.dataTask.isUserTriggered) {
            try {
                KafkaPuller.logger.warn((Object)("Error in data consumption task of " + this.getClass().getCanonicalName() + " for: " + this.receiverInfo.getRcvr_name() + ", will try to re-initialize"));
                KafkaStreamUtils.retryBackOffMillis(1000L);
                this.doCleanUp();
                this.reInitKafka();
                this.startReadingFromTopic();
            }
            catch (Exception e) {
                KafkaPuller.logger.warn((Object)("Couldn't re-initialize " + this.getClass().getCanonicalName() + "'s data consumption task. Will crash the parent application!"));
                throw e;
            }
        }
    }
    
    private void checkCheckpointTask() throws Exception {
        if (this.kafkaCheckpointTasks != null && (this.kafkaCheckpointTasks.second.isDone() || this.kafkaCheckpointTasks.second.isCancelled()) && !this.kafkaCheckpointTasks.first.isUserTriggered) {
            try {
                KafkaPuller.logger.warn((Object)("Error in checkpoint task of " + this.getClass().getCanonicalName() + " for: " + this.receiverInfo.getRcvr_name() + ", will try to re-initialize"));
                this.doCleanUp();
                this.reInitKafka();
                this.startReadingFromTopic();
            }
            catch (Exception e) {
                KafkaPuller.logger.warn((Object)("Couldn't re-initialize " + this.getClass().getCanonicalName() + "'s checkpoint task. Will crash the parent application!"));
                throw e;
            }
        }
    }
    
    private void checkStatsTask() {
        if (this.kafkaStatsTasks != null && (this.kafkaStatsTasks.second.isDone() || this.kafkaStatsTasks.second.isCancelled()) && !this.kafkaStatsTasks.first.isUserTriggered) {
            final KafkaPullerStatsTask kafkaPullerStats = new KafkaPullerStatsTask(this.kafkaPullerTasks, this.receiverInfo.getRcvr_name());
            this.statsExecutorService = new ScheduledThreadPoolExecutor(1);
            final Future statsFuture = this.statsExecutorService.scheduleAtFixedRate(kafkaPullerStats, 10000L, 10000L, TimeUnit.MILLISECONDS);
            this.kafkaStatsTasks = new Pair<KafkaPullerStatsTask, Future>(kafkaPullerStats, statsFuture);
        }
    }
    
    void doCleanUp() throws InterruptedException {
        this.cleanupLocalTasks();
        this.cleanupProducer();
        if (KafkaPuller.logger.isInfoEnabled()) {
            KafkaPuller.logger.info((Object)("Kafka Puller for " + this.receiverInfo.getRcvr_name() + " stopped"));
        }
    }
    
    void cleanupLocalTasks() throws InterruptedException {
        if (this.dataTask != null) {
            this.dataTask.stop();
        }
        if (this.dataTaskThread != null) {
            this.dataTaskThread.join(2000L);
            if (this.dataTaskThread.isAlive()) {
                this.dataTaskThread.interrupt();
            }
        }
        if (this.kafkaCheckpointTasks != null) {
            this.kafkaCheckpointTasks.first.stop();
            this.kafkaCheckpointTasks.second.cancel(true);
        }
        this.shutdownExceutorService(this.checkpointExecutorService, "checkpointExecutor", 2L, 1);
        this.shutdownExceutorService(this.statsExecutorService, "statsExecutor", 2L, 2);
        this.partitions.clear();
    }
    
    private void shutdownExceutorService(final ExecutorService execService, final String srvcName, final long timeout, final int a) throws InterruptedException {
        if (execService != null) {
            execService.shutdown();
            if (!execService.awaitTermination(timeout, TimeUnit.SECONDS)) {
                KafkaPuller.logger.warn((Object)("Unable to terminate " + srvcName + " in Kafka Puller for topic: " + this.kafkaTopicName + ". Will force terminate!"));
                execService.shutdownNow();
            }
        }
    }
    
    void cleanupProducer() {
        if (this.producerIntf != null) {
            this.producerIntf.close();
        }
    }
    
    static {
        KafkaPuller.logger = Logger.getLogger((Class)KafkaPuller.class);
    }
}
