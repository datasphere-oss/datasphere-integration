package com.datasphere.kafkamessaging;

import org.apache.log4j.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.*;
import com.datasphere.ser.*;
import java.util.*;
import com.hazelcast.core.*;
import com.datasphere.kafka.*;
import com.datasphere.runtime.components.*;

public class StreamCheckpointWriterTask implements Runnable
{
    private static Logger logger;
    private final KafkaPuller kafkaPuller;
    private final Set<PartitionState> partitions;
    private final String kafkaTopicName;
    static final int checkpointPeriodMillis = 10000;
    Exception ex;
    Map<String, String> securityProperties;
    volatile boolean isRunning;
    boolean isUserTriggered;
    
    public StreamCheckpointWriterTask(final KafkaPuller kafkaPuller) {
        this.ex = null;
        this.isRunning = true;
        this.isUserTriggered = false;
        this.kafkaPuller = kafkaPuller;
        this.partitions = kafkaPuller.partitions;
        this.kafkaTopicName = kafkaPuller.kafkaTopicName;
        this.securityProperties = kafkaPuller.securityProperties;
        this.isRunning = true;
        Thread.currentThread().setContextClassLoader(kafkaPuller.kafkaUtils.getClass().getClassLoader());
    }
    
    @Override
    public void run() {
        if (!this.isRunning) {
            return;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        for (final PartitionState partitionState : this.partitions) {
            ILock lock = null;
            try {
                lock = HazelcastSingleton.get().getLock(KafkaStreamUtils.getKafkaStreamLockName(this.kafkaTopicName, partitionState.partitionId));
                if (!lock.tryLock()) {
                    if (!Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                        continue;
                    }
                    Logger.getLogger("KafkaStreams").debug((Object)(this.kafkaTopicName + ":" + partitionState.partitionId + " could not get a lock for writing a checkpoint"));
                }
                else {
                    final String clientId = this.kafkaPuller.clientId.concat("_").concat("checkpointthread").concat("_").concat(String.valueOf(partitionState.partitionId));
                    if (partitionState.partitionCheckpoint != null && (partitionState.partitionCheckpoint.isEmpty() || partitionState.checkpointTimestamp + 10000L > currentTimeMillis)) {
                        continue;
                    }
                    final String checkpointTopicName = KafkaStreamUtils.getCheckpointTopicName(this.kafkaTopicName);
                    if (StreamCheckpointWriterTask.logger.isDebugEnabled()) {
                        StreamCheckpointWriterTask.logger.debug((Object)("Checkpoint thread -> Getting last record from " + checkpointTopicName + " in partition " + partitionState.partitionId + " in checkpoint thread"));
                    }
                    if (!this.isRunning) {
                        return;
                    }
                    final KafkaMessageAndOffset rawObject = this.kafkaPuller.kafkaUtils.getLastRecordFromTopic(checkpointTopicName, partitionState.partitionId, partitionState.checkpointTopicBroker, clientId, (Map)this.securityProperties);
                    if (rawObject == null) {
                        if (StreamCheckpointWriterTask.logger.isDebugEnabled()) {
                            StreamCheckpointWriterTask.logger.debug((Object)("Checkpoint thread ->  Last record for " + checkpointTopicName + " in partition : " + partitionState.partitionId + " is NULL"));
                        }
                    }
                    else {
                        final OffsetPosition mostRecentCheckpoint = (OffsetPosition)KryoSingleton.read(rawObject.data, false);
                        if (StreamCheckpointWriterTask.logger.isDebugEnabled()) {
                            StreamCheckpointWriterTask.logger.debug((Object)("Checkpoint thread ->  Last record for " + checkpointTopicName + " in partition : " + partitionState.partitionId + " is at offset : " + rawObject.offset + ", checkpoint data : " + mostRecentCheckpoint));
                        }
                        if (mostRecentCheckpoint != null && (mostRecentCheckpoint.getTimestamp() + 10000L > currentTimeMillis || new KafkaLongOffset(partitionState.checkpointOffset).compareTo(mostRecentCheckpoint.getOffset()) <= 0)) {
                            continue;
                        }
                    }
                    final OffsetPosition safeLocalCopy;
                    synchronized (partitionState.partitionCheckpoint) {
                        partitionState.checkpointTimestamp = currentTimeMillis;
                        safeLocalCopy = new OffsetPosition(partitionState.partitionCheckpoint.toPosition(), (Offset)new KafkaLongOffset(partitionState.checkpointOffset), partitionState.checkpointTimestamp);
                    }
                    if (!this.isRunning) {
                        return;
                    }
                    final long writeOffset = this.kafkaPuller.producerIntf.write(checkpointTopicName, partitionState.partitionId, KryoSingleton.write(safeLocalCopy, false), clientId, (List)this.kafkaPuller.bootstrapBrokers, partitionState.lastCheckpointWriteOffset, 3);
                    partitionState.lastCheckpointWriteOffset = writeOffset;
                    if (StreamCheckpointWriterTask.logger.isDebugEnabled()) {
                        StreamCheckpointWriterTask.logger.debug((Object)("Checkpoint thread -> Wrote checkpoint for partition " + partitionState.partitionId + " at " + partitionState.lastCheckpointWriteOffset + ", checkpoint data : " + safeLocalCopy));
                    }
                    if (!StreamCheckpointWriterTask.logger.isDebugEnabled()) {
                        continue;
                    }
                    StreamCheckpointWriterTask.logger.debug((Object)("Updated checkpoint for " + this.kafkaTopicName + " at checkpoint buffer offset " + writeOffset));
                }
            }
            catch (Exception e) {
                if (e instanceof KafkaException) {
                    if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
                        this.logException(e);
                    }
                    else {
                        this.crashApp(e);
                    }
                }
                else if (e instanceof InterruptedException) {
                    this.logException(e);
                }
                else {
                    this.crashApp(e);
                }
            }
            finally {
                if (lock != null && lock.isLockedByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
    }
    
    private void logException(final Exception e) {
        Thread.currentThread().interrupt();
        if (this.isUserTriggered && !this.isRunning) {
            StreamCheckpointWriterTask.logger.info((Object)(Thread.currentThread().getName() + " interrupted!Stopping checkpoint thread for " + this.kafkaPuller.streamMetaObject.getFullName() + " for topic " + this.kafkaTopicName));
        }
        else {
            StreamCheckpointWriterTask.logger.warn((Object)(Thread.currentThread().getName() + " interrupted! Is user triggered : " + this.isUserTriggered + " Is running : " + this.isRunning), (Throwable)e);
        }
    }
    
    private void crashApp(final Exception e) {
        StreamCheckpointWriterTask.logger.error((Object)"Error while checkpointing Kafka stream, will crash the owner application", (Throwable)e);
        this.ex = e;
        this.isUserTriggered = false;
        this.kafkaPuller.rcvr.getOwner().notifyAppMgr(EntityType.STREAM, this.kafkaPuller.clientId, this.kafkaPuller.streamUuid, e, null, new Object[0]);
    }
    
    public void stop() {
        this.isUserTriggered = true;
        this.isRunning = false;
    }
    
    static {
        StreamCheckpointWriterTask.logger = Logger.getLogger("KafkaStreams");
    }
}
