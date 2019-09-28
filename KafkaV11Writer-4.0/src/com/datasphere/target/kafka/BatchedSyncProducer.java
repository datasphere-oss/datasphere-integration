package com.datasphere.target.kafka;

import com.datasphere.persistence.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.meta.*;
import org.apache.log4j.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import com.datasphere.source.lib.constant.*;
import com.datasphere.runtime.components.*;
import com.datasphere.intf.*;
import com.datasphere.intf.Formatter;
import com.datasphere.kafka.schemaregistry.*;
import com.datasphere.source.kafka.*;
import com.datasphere.source.lib.intf.*;
import com.datasphere.kafka.*;
import com.datasphere.ser.*;
import com.datasphere.utility.*;
import com.datasphere.event.*;
import com.datasphere.recovery.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

public class BatchedSyncProducer extends Producer
{
    private int batchSize;
    private long batchTimeout;
    private int retries;
    private long retryBackOffMs;
    private PositionedBuffer[] dataBuffer;
    private KWCheckpointPersistenceLayer persistenceLayerImpl;
    private UUID targetUUID;
    private boolean e1p;
    private Map<String, OffsetPosition> partitionWaitPosition;
    private Offset[] latestPositionOfAllPartitions;
    private Offset[] initialPositionOfAllPartitions;
    private MetaInfo.MetaObject target;
    private static Logger logger;
    
    public BatchedSyncProducer(final Properties properties) {
        this.batchSize = 1000000;
        this.batchTimeout = 200L;
        this.retries = 0;
        this.retryBackOffMs = 2000L;
        this.e1p = false;
        this.partitionWaitPosition = null;
        if (properties.containsKey("batch.size")) {
            this.batchSize = Integer.parseInt(properties.remove("batch.size").toString());
        }
        if (properties.containsKey("linger.ms")) {
            this.batchTimeout = Long.parseLong(properties.remove("linger.ms").toString());
        }
        if (properties.containsKey("retries")) {
            this.retries = Integer.parseInt(properties.remove("retries").toString());
        }
        if (properties.containsKey("retry.backoff.ms")) {
            this.retryBackOffMs = Long.parseLong(properties.remove("retry.backoff.ms").toString());
        }
    }
    
    public void setE1P(final boolean e1p, final KWCheckpointPersistenceLayer defaultJPAPersistenceLayerImpl, final UUID uuid, final Offset[] latestPositionOfPartitions, final Offset[] initialPositionOfPartitions) {
        this.e1p = e1p;
        this.persistenceLayerImpl = defaultJPAPersistenceLayerImpl;
        this.targetUUID = uuid;
        if (this.e1p) {
            try {
                this.target = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.targetUUID, HDSecurityManager.TOKEN);
            }
            catch (MetaDataRepositoryException e) {
                throw new RuntimeException("Problem while getting Target Info from metadata repository " + e);
            }
            this.partitionWaitPosition = new HashMap<String, OffsetPosition>();
            this.latestPositionOfAllPartitions = latestPositionOfPartitions;
            this.initialPositionOfAllPartitions = initialPositionOfPartitions;
        }
    }
    
    public void setBaseUri(final String baseUri, final Constant.FormatOptions formatOptions) {
        this.schemaRegistryURL = baseUri;
        super.formatOptions = formatOptions;
    }
    
    @Override
    public void initiateProducer(final String topic, final KafkaClient[] client, final ReceiptCallback rc, final Formatter formatter, final boolean isBinaryFormatter) throws Exception {
        super.initiateProducer(topic, client, rc, formatter, isBinaryFormatter);
        final int noOfPartition = client.length;
        this.dataBuffer = new KWPositionedBuffer[noOfPartition];
        long startTime = System.currentTimeMillis();
        AvroDeserializer avroDeserializer = null;
        if (this.schemaRegistryURL != null) {
            this.schemaRegistryClient = new SchemaRegistry(this.schemaRegistryURL);
            if (this.e1p) {
                avroDeserializer = new AvroDeserializer(new SchemaRegistry(this.schemaRegistryURL));
                ((SchemaCreater)formatter).setAvroDeserializer(avroDeserializer);
            }
        }
        for (int i = 0; i < noOfPartition; ++i) {
            if (System.currentTimeMillis() - startTime > 120000L) {
                BatchedSyncProducer.logger.warn((Object)("Finding correct restart position for topic " + topic + " with " + noOfPartition + " partitions might take a while. Please wait. (Currently processed " + i + " partitions)"));
                startTime = System.currentTimeMillis();
            }
            this.dataBuffer[i] = new KWPositionedBuffer(i, this.batchSize, this.batchTimeout, this.retries, this.retryBackOffMs, client[i], this);
            if (this.e1p) {
                this.setWaitPosition(topic, i, client[i], this.latestPositionOfAllPartitions[i], this.initialPositionOfAllPartitions[i]);
            }
        }
        if (avroDeserializer != null) {
            avroDeserializer.close();
            avroDeserializer = null;
        }
    }
    
    public Map<String, OffsetPosition> getWaitPosition() {
        return this.partitionWaitPosition;
    }
    
    private void setWaitPosition(final String topic, final Integer partition, final KafkaClient client, final Offset lastOffsetOfPartition, final Offset startOffset) throws Exception {
        final String key = this.target.nsName + "_" + this.target.name + "_" + topic + ":" + partition;
        final Object chkPtObj = this.persistenceLayerImpl.get((Class)KWCheckpoint.class, (Object)key);
        OffsetPosition offsetPositionChkPt;
        if (chkPtObj != null) {
            final byte[] chkPtBytes = ((KWCheckpoint)chkPtObj).checkpointdata;
            offsetPositionChkPt = (OffsetPosition)KryoSingleton.read(chkPtBytes, false);
            if (BatchedSyncProducer.logger.isDebugEnabled()) {
                BatchedSyncProducer.logger.debug((Object)("Offset position for " + key + " from KWCheckpoint table " + offsetPositionChkPt));
            }
            if (!offsetPositionChkPt.isEmpty()) {
                if (!offsetPositionChkPt.containsComponent(this.targetUUID)) {
                    final OffsetPosition alteredOffsetPosition = Utility.updateUuids(offsetPositionChkPt);
                    if (!alteredOffsetPosition.containsComponent(this.targetUUID)) {
                        offsetPositionChkPt = new OffsetPosition(client.getNewOffset(), System.currentTimeMillis());
                        final byte[] checkpointbytes = KryoSingleton.write((Object)offsetPositionChkPt, false);
                        final KWCheckpoint updatedChkpt = new KWCheckpoint(key, checkpointbytes);
                        this.persistenceLayerImpl.merge((Object)updatedChkpt);
                    }
                    else if (BatchedSyncProducer.logger.isInfoEnabled()) {
                        BatchedSyncProducer.logger.info((Object)("KafkaWriter component was altered hence honouring the checkpoint (with offset " + offsetPositionChkPt.getOffset() + ")from KWCheckpoint table for topic (" + topic + "," + partition + ")"));
                    }
                }
                if (BatchedSyncProducer.logger.isInfoEnabled()) {
                    BatchedSyncProducer.logger.info((Object)("Offset in KWCheckpoint checkpoint is  " + offsetPositionChkPt.getOffset() + " and last offset in the partitions is " + lastOffsetOfPartition + " for topic (" + topic + "," + partition + ")"));
                }
            }
        }
        else {
            offsetPositionChkPt = new OffsetPosition(client.getNewOffset(), System.currentTimeMillis());
        }
        if (offsetPositionChkPt.getOffset().compareTo(lastOffsetOfPartition) > 0) {
            throw new RuntimeException("Could not find the data with Kafka offset - " + offsetPositionChkPt.getOffset() + " in topic (" + topic + "-" + partition + "). Last offset of data topic is " + lastOffsetOfPartition + ". Data in the topic seems to be lost.");
        }
        if (lastOffsetOfPartition.isValid() && offsetPositionChkPt.getOffset().compareTo(lastOffsetOfPartition) <= 0) {
            if (startOffset != null && startOffset.compareTo(offsetPositionChkPt.getOffset()) > 0) {
                if (startOffset.isValid() || offsetPositionChkPt.getOffset().isValid()) {
                    BatchedSyncProducer.logger.warn((Object)("Start offset of the topic " + topic + "-" + partition + " is different from local checkpoint (Possible reason - Checkpoint in KWCheckpoint table was never updated for this topic in the last session or Retention period of the messages expired or Messages were deleted manually from the topic).Updating the start offset to " + startOffset + "."));
                }
                offsetPositionChkPt.setOffset(startOffset);
            }
            offsetPositionChkPt = client.updatePartitionWaitPosition(topic, partition, offsetPositionChkPt, offsetPositionChkPt.getOffset(), lastOffsetOfPartition, this.formatter, this.targetUUID);
            final byte[] checkpointbytes2 = KryoSingleton.write((Object)offsetPositionChkPt, false);
            final KWCheckpoint updatedChkpt2 = new KWCheckpoint(key, checkpointbytes2);
            if (chkPtObj == null) {
                try {
                    this.persistenceLayerImpl.persist((Object)updatedChkpt2);
                }
                catch (Exception e) {
                    if (BatchedSyncProducer.logger.isInfoEnabled()) {
                        BatchedSyncProducer.logger.info((Object)("Problem while persisting the checkpoint detail for (" + topic + "-" + partition + "). KafkaWriter from some other node has persisted the information already." + e));
                    }
                }
            }
            else {
                this.persistenceLayerImpl.merge((Object)updatedChkpt2);
            }
        }
        if (BatchedSyncProducer.logger.isDebugEnabled()) {
            BatchedSyncProducer.logger.debug((Object)("Position of topic " + topic + "," + partition + " is " + offsetPositionChkPt));
        }
        if (!offsetPositionChkPt.isEmpty()) {
            final Collection<Path> originalPaths = (Collection<Path>)offsetPositionChkPt.values();
            final List<Path> modifiedPaths = new ArrayList<Path>();
            boolean hasKRSource = false;
            for (final Path path : originalPaths) {
                if (path.getLowSourcePosition() instanceof KafkaSourcePosition || path.getHighSourcePosition() instanceof KafkaSourcePosition) {
                    hasKRSource = true;
                }
                if (path.contains(this.targetUUID)) {
                    final Path.ItemList originalItemList = path.getPathItems();
                    final int count = originalItemList.size();
                    final Path.Item[] itemArray = new Path.Item[count];
                    for (int i = 0; i < count; ++i) {
                        Path.Item item = originalItemList.get(i);
                        if (item.getComponentUUID().equals((Object)this.targetUUID)) {
                            item = Path.Item.get(this.targetUUID, partition.toString());
                        }
                        else if (hasKRSource) {
                            final UUID uuid = item.getComponentUUID();
                            final MetaInfo.MetaObject metaObject = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HDSecurityManager.TOKEN);
                            if (metaObject instanceof MetaInfo.Source) {
                                final MetaInfo.Source s = (MetaInfo.Source)metaObject;
                                if (s.adapterClassName.equalsIgnoreCase("com.datasphere.proc.KafkaV8Reader") || s.adapterClassName.equalsIgnoreCase("com.datasphere.proc.KafkaV9Reader")) {
                                    final String did = item.getDistributionID();
                                    if (did.contains(":")) {
                                        final String updatedDID = did.substring(did.indexOf(":") + 1);
                                        item = Path.Item.get(uuid, updatedDID);
                                    }
                                }
                            }
                        }
                        itemArray[i] = item;
                    }
                    final Path.ItemList modifiedItemList = Path.ItemList.get(itemArray);
                    final Path newPath = new Path(modifiedItemList, path.getLowSourcePosition(), path.getHighSourcePosition(), path.getAtOrAfter());
                    modifiedPaths.add(newPath);
                }
                else {
                    modifiedPaths.add(path);
                }
                hasKRSource = false;
            }
            final OffsetPosition newop = new OffsetPosition(new Position((List)modifiedPaths), offsetPositionChkPt.getOffset(), offsetPositionChkPt.getTimestamp());
            if (offsetPositionChkPt.equals((Object)newop)) {
                if (BatchedSyncProducer.logger.isDebugEnabled()) {
                    BatchedSyncProducer.logger.debug((Object)"Modified position and original position looks same.");
                }
            }
            else if (BatchedSyncProducer.logger.isDebugEnabled()) {
                BatchedSyncProducer.logger.debug((Object)("Modified position of " + topic + ":" + partition + " is " + offsetPositionChkPt));
            }
            offsetPositionChkPt = newop;
            if (BatchedSyncProducer.logger.isInfoEnabled()) {
                BatchedSyncProducer.logger.info((Object)("Position of " + topic + "," + partition + " after modification (had " + modifiedPaths.size() + " paths)"));
            }
            final MutableStemma waitpath = MutableStemma.oneMutableStemmaFromPosition((Position)offsetPositionChkPt);
            this.dataBuffer[partition].waitPosition = waitpath.toPathManager();
            this.partitionWaitPosition.put(key, offsetPositionChkPt);
        }
        this.dataBuffer[partition].setRecoveryEnabled(true);
    }
    
    @Override
    public long send(final int partition, final Event event, final ImmutableStemma pos) throws Exception {
        this.dataBuffer[partition].put(new DARecord((Object)event, pos.toPosition()));
        
        return 0L;
    }
    
    @Override
    public void close() throws Exception {
        final long start = System.currentTimeMillis();
        if (this.dataBuffer != null) {
            for (PositionedBuffer buffer : this.dataBuffer) {
                if (buffer != null) {
                    buffer.closeProducer();
                    buffer = null;
                }
            }
            this.dataBuffer = null;
        }
        super.close();
        if (BatchedSyncProducer.logger.isInfoEnabled()) {
            BatchedSyncProducer.logger.info((Object)("Took " + (System.currentTimeMillis() - start) + " ms to close Databuffer of all partitions of " + this.topic));
        }
    }
    
    @Override
    public Object getAvgLatency() {
        double totalLatency = 0.0;
        int totalBuffers = 0;
        if (this.dataBuffer != null) {
            for (final PositionedBuffer buffer : this.dataBuffer) {
                if (buffer != null) {
                    final double avglantency = ((KWPositionedBuffer)buffer).getAvgLatency();
                    totalLatency += avglantency;
                    ++totalBuffers;
                }
            }
        }
        return String.valueOf(totalLatency / (totalBuffers * 1.0));
    }
    
    @Override
    public Object getMaxLatency() {
        long maxLatency = Long.MIN_VALUE;
        if (this.dataBuffer != null) {
            for (final PositionedBuffer buffer : this.dataBuffer) {
                if (buffer != null) {
                    maxLatency = Math.max(maxLatency, ((KWPositionedBuffer)buffer).getMaxLatency());
                }
            }
        }
        return maxLatency;
    }
    
    @Override
    public Object getAvgTimeBtwSendCalls() {
        double avgTime = 0.0;
        long totalBuffers = 0L;
        if (this.dataBuffer != null) {
            for (final PositionedBuffer buffer : this.dataBuffer) {
                if (buffer != null) {
                    avgTime += ((KWPositionedBuffer)buffer).getAvgTimeBtwSendCalls();
                    ++totalBuffers;
                }
            }
        }
        return String.valueOf(avgTime / (totalBuffers * 1.0));
    }
    
    @Override
    public Object getMaxTimeBtwSendCalls() {
        long maxTime = Long.MIN_VALUE;
        if (this.dataBuffer != null) {
            for (final PositionedBuffer buffer : this.dataBuffer) {
                if (buffer != null) {
                    maxTime = Math.max(maxTime, ((KWPositionedBuffer)buffer).getMaxTimeBtwSendCalls());
                }
            }
        }
        return maxTime;
    }
    
    @Override
    public long getNoOfSendCalls() {
        long noOfSendCalls = 0L;
        if (this.dataBuffer != null) {
            for (final PositionedBuffer positionedBuffer : this.dataBuffer) {
                if (positionedBuffer != null) {
                    noOfSendCalls += ((KWPositionedBuffer)positionedBuffer).getNoOfSends();
                }
            }
        }
        return noOfSendCalls;
    }
    
    static {
        BatchedSyncProducer.logger = Logger.getLogger((Class)BatchedSyncProducer.class);
    }
}
