package com.datasphere.kafkamessaging;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.datasphere.classloading.WALoader;
import com.datasphere.event.SimpleEvent;
import com.datasphere.kafka.ConsumerIntf;
import com.datasphere.kafka.KafkaConstants;
import com.datasphere.kafka.KafkaMessageAndOffset;
import com.datasphere.kafka.PartitionState;
import com.datasphere.messaging.Handler;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.commands.CommandEvent;
import com.datasphere.recovery.KafkaSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.KafkaStreamUtils;
import com.datasphere.runtime.LagMarker;
import com.datasphere.runtime.StreamEvent;
import com.datasphere.runtime.StreamEventFactory;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.DynamicEvent;
class KafkaPullerTask implements Runnable
{
    private static Logger logger;
    private final Handler rcvr;
    private final KafkaPuller kafkaPuller;
    protected Integer[] emittedCountPerPartition;
    public volatile boolean dontStop;
    long min_fetch_size;
    long avg_fetch_size;
    long max_fetch_size;
    long total_fetch_reqs;
    Exception ex;
    boolean isUserTriggered;
    private boolean isAvro;
    private SpecificDatumReader avroReader;
    private MDRepository mdr;
    private WALoader wal;
    private MetaInfo.Type streamDataTye;
    private Object streamDataTypeInstance;
    private Class streamDataTypeClass;
    Properties properties;
    Map<Integer, ConsumerIntf> pJobs;
    Map<Integer, PartitionState> psMap;
    
    public KafkaPullerTask(final KafkaPuller kafkaPuller, final Set<PartitionState> partitions) throws Exception {
        this.total_fetch_reqs = 0L;
        this.ex = null;
        this.isUserTriggered = false;
        this.mdr = MetadataRepository.getINSTANCE();
        this.wal = WALoader.get();
        Thread.currentThread().setContextClassLoader(kafkaPuller.kafkaUtils.getClass().getClassLoader());
        this.kafkaPuller = kafkaPuller;
        this.rcvr = kafkaPuller.rcvr;
        this.emittedCountPerPartition = new Integer[kafkaPuller.partitions.size()];
        for (int ii = 0; ii < this.emittedCountPerPartition.length; ++ii) {
            this.emittedCountPerPartition[ii] = 0;
        }
        final MetaInfo.PropertySet streamPropset = KafkaStreamUtils.getPropertySet(kafkaPuller.streamMetaObject);
        if (streamPropset.properties != null) {
            final String format = (String)streamPropset.properties.get("dataformat");
            if (format != null && format.equalsIgnoreCase("avro")) {
                this.streamDataTypeClass = this.getStreamDataTypeClass();
                this.streamDataTypeInstance = this.streamDataTypeClass.newInstance();
                this.avroReader = this.getAvroReader();
                this.isAvro = true;
                this.streamDataTye = this.getStreamDataType();
            }
        }
        ((this.properties = new Properties())).put("fetchSize", KafkaConstants.fetch_size);
        this.properties.put("minBytes", 1048576);
        this.properties.put("maxWait", 1000);
        this.pJobs = new HashMap<Integer, ConsumerIntf>();
        this.psMap = new HashMap<Integer, PartitionState>();
        this.properties.put("bootstrap.brokers", kafkaPuller.bootstrapBrokers);
        this.properties.put("topic", kafkaPuller.kafkaTopicName);
        final Map<String, String> securityProperties = KafkaStreamUtils.getSecurityProperties(streamPropset);
        if (securityProperties != null) {
            this.properties.put("securityconfig", securityProperties);
        }
        for (final PartitionState ps : partitions) {
            final String clientIdByPartition = kafkaPuller.clientId.concat("_").concat(new UUID(System.currentTimeMillis()).toString()).concat("_").concat(String.valueOf(ps.partitionId));
            this.properties.put("ps", ps);
            this.properties.put("clientId", clientIdByPartition);
            final ConsumerIntf cc = kafkaPuller.kafkaUtils.getKafkaConsumer(this.properties);
            this.pJobs.put(ps.partitionId, cc);
            this.psMap.put(ps.partitionId, ps);
        }
        this.dontStop = Boolean.TRUE;
    }
    
    private SpecificDatumReader getAvroReader() throws Exception {
        boolean foundSchema = false;
        MetaInfo.Stream streamMetaObject = null;
        while (!foundSchema) {
            streamMetaObject = (MetaInfo.Stream)this.mdr.getMetaObjectByUUID(this.kafkaPuller.streamUuid, HSecurityManager.TOKEN);
            if (streamMetaObject.avroSchema != null) {
                foundSchema = true;
            }
            Thread.sleep(1000L);
        }
        final Schema schema = new Schema.Parser().parse(streamMetaObject.avroSchema);
        final SpecificDatumReader avroReader = new SpecificDatumReader(schema);
        return avroReader;
    }
    
    private MetaInfo.Type getStreamDataType() throws MetaDataRepositoryException {
        final UUID uuid = this.kafkaPuller.streamMetaObject.getDataType();
        if (uuid != null) {
            final MetaInfo.Type dataType = (MetaInfo.Type)this.mdr.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
            return dataType;
        }
        return null;
    }
    
    private Class<?> getStreamDataTypeClass() throws MetaDataRepositoryException, ClassNotFoundException {
        final UUID uuid = this.kafkaPuller.streamMetaObject.getDataType();
        if (uuid != null) {
            final MetaInfo.Type dataType = (MetaInfo.Type)this.mdr.getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
            return this.wal.loadClass(dataType.className);
        }
        return null;
    }
    
    @Override
    public void run() {
        if (KafkaPullerTask.logger.isInfoEnabled()) {
            KafkaPullerTask.logger.info((Object)("Started Kafka Puller Task for : " + this.kafkaPuller.clientId));
        }
        while (this.dontStop) {
            for (final Map.Entry<Integer, ConsumerIntf> entry : this.pJobs.entrySet()) {
                try {
                    if (!this.dontStop) {
                        break;
                    }
                    final List<KafkaMessageAndOffset> msgs = (List<KafkaMessageAndOffset>)entry.getValue().read((PartitionState)this.psMap.get(entry.getKey()));
                    this.doProcess(msgs, entry.getKey(), this.psMap.get(entry.getKey()));
                }
                catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        if (!this.dontStop) {
                            KafkaPullerTask.logger.warn((Object)(Thread.currentThread().getName() + " interrupted as part of STOP routine."));
                        }
                        else {
                            KafkaPullerTask.logger.error((Object)(Thread.currentThread().getName() + " interrupted."));
                            this.ex = e;
                            this.isUserTriggered = false;
                            this.dontStop = false;
                        }
                        Thread.interrupted();
                    }
                    else {
                        KafkaPullerTask.logger.error((Object)("Error trying to read from Kafka topic: " + this.kafkaPuller.kafkaTopicName + ", partition: " + e));
                        this.ex = e;
                        this.isUserTriggered = false;
                        this.dontStop = false;
                    }
                    break;
                }
            }
        }
        for (final ConsumerIntf consumerIntf : this.pJobs.values()) {
            try {
                consumerIntf.close();
            }
            catch (Exception e) {
                KafkaPullerTask.logger.warn((Object)("Error while stopping Kafka consumer. Reason -> " + e.getMessage()), (Throwable)e);
            }
        }
    }
    
    private void doProcess(final List<KafkaMessageAndOffset> msgs, final Integer key, final PartitionState partitionState) throws Exception {
        try {
            if (!this.rcvr.getOwner().injectedCommandEvents.isEmpty()) {
                final CommandEvent commandEvent = this.rcvr.getOwner().injectedCommandEvents.remove();
                if (commandEvent != null) {
                    if (Logger.getLogger("Commands").isDebugEnabled()) {
                        Logger.getLogger("Commands").debug((Object)("Source " + this.rcvr.getOwner().getMetaFullName() + " is now handling " + commandEvent.getClass() + "..."));
                    }
                    this.rcvr.onMessage(commandEvent);
                }
            }
        }
        catch (Exception e) {
            KafkaPullerTask.logger.error((Object)"Error while processing a Command", (Throwable)e);
        }
        for (final KafkaMessageAndOffset km : msgs) {
            if (km.offset < partitionState.kafkaReadOffset) {
                continue;
            }
            int DARecordIndex = 0;
            final ByteBuffer payload = ByteBuffer.wrap(km.data);
            while (payload.hasRemaining()) {
                try {
                    final int size = payload.getInt();
                    if (payload.position() + size > payload.limit() || size == 0) {
                        break;
                    }
                    final byte[] current_bytes = new byte[size];
                    payload.get(current_bytes, 0, size);
                    final Object rawObject = this.deserialize(current_bytes);
                    if (rawObject instanceof DARecord) {
                        final DARecord e2 = (DARecord)rawObject;
                        if (e2.position != null && partitionState.partitionCheckpoint != null) {
                            partitionState.partitionCheckpoint.mergeHigherPositions(e2.position);
                            partitionState.checkpointOffset = km.nextOffset;
                            if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
                                Logger.getLogger("KafkaStreams").debug((Object)("Pulled  " + this.kafkaPuller.kafkaTopicName + ":" + partitionState.partitionId + "@" + partitionState.kafkaReadOffset + "/" + DARecordIndex + ": " + e2.position));
                            }
                            final KafkaSourcePosition sp = new KafkaSourcePosition(this.kafkaPuller.kafkaTopicName, partitionState.partitionId, (long)DARecordIndex, km.offset);
                            e2.position = Position.from(this.kafkaPuller.streamUuid, String.valueOf(partitionState.partitionId), (SourcePosition)sp);
                            partitionState.lastEmittedPosition = e2.position;
                        }
                        final LagMarker marker = e2.getLagMarker();
                        final TaskEvent taskEvent = (TaskEvent)StreamEventFactory.createStreamEvent(e2.data, e2.position);
                        if (marker != null && !marker.isEmpty()) {
                            taskEvent.setLagRecord(true);
                            taskEvent.setLagMarker(marker);
                        }
                        final StreamEvent streamEvent = new StreamEvent((ITaskEvent)taskEvent, (DistLink)null);
                        this.rcvr.onMessage(streamEvent);
                        final Integer[] emittedCountPerPartition = this.emittedCountPerPartition;
                        final int partitionId = partitionState.partitionId;
                        final Integer n = emittedCountPerPartition[partitionId];
                        ++emittedCountPerPartition[partitionId];
                    }
                    else if (rawObject instanceof ITaskEvent) {
                        final ITaskEvent e3 = (ITaskEvent)rawObject;
                        this.rcvr.onMessage(e3);
                    }
                    else {
                        KafkaPullerTask.logger.error((Object)("Unexpected object type found in kafka stream: " + rawObject.getClass() + "... " + rawObject));
                    }
                }
                finally {
                    ++DARecordIndex;
                }
            }
            partitionState.kafkaReadOffset = km.nextOffset;
        }
    }
    
    private Object deserialize(final byte[] current_bytes) throws Exception {
        if (this.isAvro) {
            final BinaryDecoder recordDecoder = DecoderFactory.get().binaryDecoder(current_bytes, (BinaryDecoder)null);
            final GenericData.Record oo = (GenericData.Record)this.avroReader.read((Object)null, (Decoder)recordDecoder);
            DARecord pDARecord;
            if (this.streamDataTypeInstance instanceof com.datasphere.proc.events.DynamicEvent) {
                final GenericData.Array dataArray = (GenericData.Array)oo.get("data");
                Object[] data = null;
                if (dataArray != null) {
                    data = new Object[dataArray.size()];
                    int ii = 0;
                    for (final Object utf8 : dataArray) {
                        data[ii++] = utf8.toString();
                    }
                }
                final Map metadataMap = (Map)oo.get("metadata");
                final HashMap mmap = new HashMap();
                for (final Object ss : metadataMap.keySet()) {
                    mmap.put(ss, metadataMap.get(ss).toString());
                }
                final GenericData.Array beforeObject = (GenericData.Array)oo.get("before");
                Object[] before = null;
                if (beforeObject != null) {
                    before = new Object[beforeObject.size()];
                    int jj = 0;
                    for (final Object utf9 : beforeObject) {
                        before[jj++] = utf9.toString();
                    }
                }
                final Object dataPBMObject = oo.get("dataPresenceBitMap");
                final byte[] dataPBMBytes = Base64.decodeBase64(dataPBMObject.toString());
                final Object beforePBMObject = oo.get("beforePresenceBitMap");
                final byte[] beforePBMBytes = Base64.decodeBase64(beforePBMObject.toString());
                final Object uuidObject = oo.get("typeUUID");
                UUID typeUUID = null;
                if (uuidObject != null) {
                    typeUUID = new UUID(uuidObject.toString());
                }
                final com.datasphere.proc.records.Record DARecord = (com.datasphere.proc.records.Record)this.streamDataTypeClass.newInstance();
                DARecord.setPayload(data);
                DARecord.before = before;
                DARecord.metadata = mmap;
                DARecord.dataPresenceBitMap = dataPBMBytes;
                DARecord.beforePresenceBitMap = beforePBMBytes;
                DARecord.typeUUID = typeUUID;
                pDARecord = new DARecord((Object)DARecord);
            }
            else {
                final Object[] vals = new Object[this.streamDataTye.fields.size()];
                int ip = 0;
                for (final Map.Entry<String, String> entry : this.streamDataTye.fields.entrySet()) {
                    final Object ood = oo.get((String)entry.getKey());
                    if (entry.getValue().contains("String")) {
                        vals[ip] = ood.toString();
                    }
                    else if (entry.getValue().contains("DateTime")) {
                        DateTime dt = null;
                        if (ood != null) {
                            dt = DateTime.parse(ood.toString());
                        }
                        vals[ip] = dt;
                    }
                    else if (entry.getValue().contains("UUID")) {
                        if (ood != null) {
                            vals[ip] = new UUID(ood.toString());
                        }
                        else {
                            vals[ip] = ood;
                        }
                    }
                    else if (entry.getValue().contains("byte")) {
                        final byte[] bytes = Base64.decodeBase64(ood.toString());
                        final Object result = KryoSingleton.read(bytes, false);
                        vals[ip] = result;
                    }
                    else {
                        vals[ip] = ood;
                    }
                    ++ip;
                }
                final SimpleEvent simpleEvent = (SimpleEvent)this.streamDataTypeClass.newInstance();
                simpleEvent.setPayload(vals);
                pDARecord = new DARecord((Object)simpleEvent);
            }
            final Object posObject = oo.get("position");
            if (posObject != null) {
                final byte[] posBytes = Base64.decodeBase64(posObject.toString());
                final Position position = (Position)KryoSingleton.read(posBytes, false);
                pDARecord.position = position;
            }
            return pDARecord;
        }
        return KryoSingleton.read(current_bytes, false);
    }
    
    public void stop() {
        this.dontStop = false;
        this.isUserTriggered = true;
        for (final ConsumerIntf consumerIntf : this.pJobs.values()) {
            try {
                consumerIntf.wakeup();
            }
            catch (Exception e) {
                KafkaPullerTask.logger.warn((Object)("Error will stopping kafka consumer. Reason -> " + e.getMessage()), (Throwable)e);
            }
        }
    }
    
    static {
        KafkaPullerTask.logger = Logger.getLogger((Class)KafkaPuller.class);
    }
}
