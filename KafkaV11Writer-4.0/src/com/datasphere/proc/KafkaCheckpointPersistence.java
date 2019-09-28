package com.datasphere.proc;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.datasphere.event.Event;
import com.datasphere.intf.EventSink;
import com.datasphere.kafka.KWCheckpoint;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.kafka.OffsetPosition;
import com.datasphere.kinesis.KinesisOffset;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.persistence.DefaultJPAPersistenceLayerImpl;
import com.datasphere.proc.events.AvroEvent;
import com.datasphere.proc.events.JsonNodeEvent;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.ReceiptCallback;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.ser.KryoSingleton;
import com.datasphere.uuid.UUID;

public class KafkaCheckpointPersistence implements Runnable
{
    SourceProcess kafkaReaderAdapter;
    KafkaEventHandler eventHandler;
    DefaultJPAPersistenceLayerImpl persistenceLayerImpl;
    Map<String, KWCheckpoint> checkpointMap;
    Map<String, OffsetPosition> partitionPosition;
    MetaInfo.MetaObject target;
    private ExecutorService threadPool;
    private ScheduledExecutorService scheduledProcess;
    private KafkaConsumerThread consumer;
    private static Logger logger;
    private ReceiptCallback receiptCallback;
    
    public KafkaCheckpointPersistence(final int version, final Map<String, Object> properties, final DefaultJPAPersistenceLayerImpl defaultJPAPersistenceLayerImpl, final Map<String, OffsetPosition> restartPosition, final ReceiptCallback receiptCallback) {
        this.persistenceLayerImpl = defaultJPAPersistenceLayerImpl;
        this.checkpointMap = new ConcurrentHashMap<String, KWCheckpoint>();
        this.partitionPosition = new ConcurrentHashMap<String, OffsetPosition>();
        if (restartPosition != null && !restartPosition.isEmpty()) {
            this.partitionPosition.putAll(restartPosition);
        }
        final UUID targetUUID = (UUID)properties.get("TargetUUID");
        try {
            this.target = MetadataRepository.getINSTANCE().getMetaObjectByUUID(targetUUID, HDSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e1) {
            throw new RuntimeException("Problem in retrieving Meta object for UUID " + targetUUID + "." + e1);
        }
        this.receiptCallback = receiptCallback;
        final HashMap<String, Object> readerProperties = new HashMap<String, Object>();
        readerProperties.put("brokerAddress", properties.get("brokerAddress"));
        readerProperties.put("Topic", properties.get("Topic"));
        readerProperties.put("AutoMapPartition", "false");
        readerProperties.put("KafkaConfig", properties.get("KafkaConfig"));
        String parserName = "";
        final String formatterName = (String)properties.get("formatterName");
        final Map<String, Object> parserProperties = new HashMap<String, Object>();
        if (formatterName.equalsIgnoreCase("AvroFormatter")) {
            parserName = "AvroParser";
            parserProperties.put("schemaFileName", properties.get("schemaFileName"));
            if (properties.containsKey("schemaregistryurl")) {
                parserProperties.put("schemaregistryurl", properties.get("schemaregistryurl"));
            }
            this.eventHandler = new AvroEventHandler();
        }
        else if (formatterName.equalsIgnoreCase("JSONFormatter")) {
            parserName = "JSONParser";
            this.eventHandler = new JSONEventHanlder();
        }
        else if (formatterName.equalsIgnoreCase("DSVFormatter")) {
            parserName = "DSVParser";
            parserProperties.put("columndelimiter", properties.get("columndelimiter"));
            parserProperties.put("charset", properties.get("charset"));
            parserProperties.put("rowdelimiter", properties.get("rowdelimiter"));
            parserProperties.put("quoteset", properties.get("quotecharacter"));
            this.eventHandler = new DSVEventHandler();
        }
        else if (formatterName.equalsIgnoreCase("XMLFormatter")) {
            parserName = "XMLParser";
            parserProperties.put("rootnode", properties.get("rootelement"));
            this.eventHandler = new XMLEventHanlder();
        }
        readerProperties.put("handler", parserName);
        try {
            if (version == 8) {
                this.kafkaReaderAdapter = (SourceProcess)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.KafkaV8Reader").newInstance();
            }
            else if (version == 9) {
                this.kafkaReaderAdapter = (SourceProcess)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.KafkaV9Reader").newInstance();
            }
            else if (version == 10) {
                this.kafkaReaderAdapter = (SourceProcess)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.KafkaV10Reader").newInstance();
            }
            else if (version == 11) {
                this.kafkaReaderAdapter = (SourceProcess)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.KafkaV11Reader").newInstance();
            }
            else if (version == 0) {
                readerProperties.put("streamName", properties.get("streamName"));
                readerProperties.put("Topic", properties.get("streamName"));
                readerProperties.put("partitionKey", properties.get("partitionKey"));
                readerProperties.put("regionName", properties.get("regionName"));
                readerProperties.put("secretaccesskey", properties.get("secretaccesskey"));
                readerProperties.put("accesskeyid", properties.get("accesskeyid"));
                this.kafkaReaderAdapter = (SourceProcess)ClassLoader.getSystemClassLoader().loadClass("com.datasphere.proc.KinesisReader").newInstance();
            }
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex2) {
            throw new RuntimeException("Problem while loading KafkaReader " + ex2);
        }
        try {
            final ClassLoader origialCl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.kafkaReaderAdapter.getClass().getClassLoader());
            if (version == 8 || version == 0) {
                this.kafkaReaderAdapter.init((Map)readerProperties, (Map)parserProperties, targetUUID, (String)null, (SourcePosition)null, false, (Flow)null);
            }
            else {
                this.kafkaReaderAdapter.init((Map)readerProperties, (Map)parserProperties, targetUUID, (String)null, (SourcePosition)null, false, (Flow)null, (List)null);
            }
            this.kafkaReaderAdapter.addEventSink((EventSink)new KafkaEventSink());
            Thread.currentThread().setContextClassLoader(origialCl);
        }
        catch (Exception e3) {
            throw new RuntimeException("Problem while initializing KafkaReader ", e3);
        }
        this.threadPool = Executors.newCachedThreadPool();
        this.consumer = new KafkaConsumerThread();
        this.threadPool.submit(this.consumer);
        (this.scheduledProcess = new ScheduledThreadPoolExecutor(1)).scheduleAtFixedRate(this, 10L, 10L, TimeUnit.SECONDS);
    }
    
    @Override
    public void run() {
        for (final Map.Entry<String, KWCheckpoint> chkpt : this.checkpointMap.entrySet()) {
            try {
                final Object o = this.persistenceLayerImpl.get((Class)KWCheckpoint.class, (Object)chkpt.getKey());
                if (o == null) {
                    if (KafkaCheckpointPersistence.logger.isDebugEnabled()) {
                        KafkaCheckpointPersistence.logger.debug((Object)("Going to persist " + chkpt.getKey() + " info in derby " + chkpt.getValue()));
                    }
                    this.persistenceLayerImpl.persist((Object)chkpt.getValue());
                }
                else {
                    final OffsetPosition positionInDerby = (OffsetPosition)KryoSingleton.read(((KWCheckpoint)o).checkpointdata, false);
                    final OffsetPosition positionToBePersisted = (OffsetPosition)KryoSingleton.read(chkpt.getValue().checkpointdata, false);
                    if (positionInDerby.getOffset().compareTo(positionToBePersisted.getOffset()) > 0) {
                        continue;
                    }
                    if (KafkaCheckpointPersistence.logger.isDebugEnabled()) {
                        KafkaCheckpointPersistence.logger.debug((Object)("Going to persist " + chkpt.getKey() + " info in derby " + chkpt.getValue()));
                    }
                    this.persistenceLayerImpl.merge((Object)chkpt.getValue());
                }
            }
            catch (Exception e) {
                KafkaCheckpointPersistence.logger.warn((Object)("Problem while persisting checkpoint to KWCheckpoint table. " + e));
            }
        }
    }
    
    public void close() {
        try {
            if (this.consumer != null) {
                this.consumer.close(true);
            }
            if (this.kafkaReaderAdapter != null) {
                this.kafkaReaderAdapter.close();
                this.kafkaReaderAdapter = null;
            }
            if (this.threadPool != null) {
                this.threadPool.shutdownNow();
                this.threadPool = null;
            }
            if (this.scheduledProcess != null) {
                this.scheduledProcess.shutdownNow();
                this.scheduledProcess = null;
            }
            if (this.persistenceLayerImpl != null) {
                this.persistenceLayerImpl.close();
                this.persistenceLayerImpl = null;
            }
            this.consumer = null;
        }
        catch (Exception e) {
            if (KafkaCheckpointPersistence.logger.isInfoEnabled()) {
                KafkaCheckpointPersistence.logger.info((Object)("Problem while closing KafkaCheckpoint persistence layer " + e));
            }
        }
    }
    
    static {
        KafkaCheckpointPersistence.logger = Logger.getLogger((Class)KafkaCheckpointPersistence.class);
    }
    
    class KafkaConsumerThread implements Runnable
    {
        private volatile boolean stopConsuming;
        
        KafkaConsumerThread() {
            this.stopConsuming = false;
        }
        
        @Override
        public void run() {
            try {
                while (!this.stopConsuming) {
                    KafkaCheckpointPersistence.this.kafkaReaderAdapter.receiveImpl(0, (Event)null);
                }
            }
            catch (Exception e) {
                if (!this.stopConsuming) {
                    KafkaCheckpointPersistence.logger.error((Object)("Problem while checkpointing data to KWCheckpoint. Exception while consuming data from Kafka " + e));
                    KafkaCheckpointPersistence.this.receiptCallback.notifyException(e, (Event)null);
                }
            }
        }
        
        public void close(final boolean stopConsuming) {
            this.stopConsuming = stopConsuming;
        }
    }
    
    class KafkaEventSink extends AbstractEventSink
    {
        public void receive(final int channel, final Event event) throws Exception {
            final String key = KafkaCheckpointPersistence.this.target.nsName + "_" + KafkaCheckpointPersistence.this.target.name + "_" + KafkaCheckpointPersistence.this.eventHandler.getTopicAndPartition(event);
            final Offset kafkaOffset = KafkaCheckpointPersistence.this.eventHandler.getKafkaOffset(event);
            final byte[] bytes = KafkaCheckpointPersistence.this.eventHandler.getPositionBytesFromEvent(event);
            final ImmutableStemma positionStemma = KryoSingleton.readImmutableStemma(bytes);
            final Position position = positionStemma.toPosition();
            if (!KafkaCheckpointPersistence.this.partitionPosition.containsKey(key)) {
                KafkaCheckpointPersistence.this.partitionPosition.put(key, new OffsetPosition(position, kafkaOffset, System.currentTimeMillis()));
            }
            else {
                final OffsetPosition oldPosition = KafkaCheckpointPersistence.this.partitionPosition.get(key);
                final PathManager oldPath = new PathManager((Position)oldPosition);
                oldPath.mergeHigherPositions(position);
                final OffsetPosition latestOffsetPosition = new OffsetPosition(oldPath.toPosition(), kafkaOffset, System.currentTimeMillis());
                KafkaCheckpointPersistence.this.partitionPosition.put(key, latestOffsetPosition);
            }
            final byte[] positionBytes = KryoSingleton.write((Object)KafkaCheckpointPersistence.this.partitionPosition.get(key), false);
            final KWCheckpoint chkPt = new KWCheckpoint(key, positionBytes);
            KafkaCheckpointPersistence.this.checkpointMap.put(key, chkPt);
        }
    }
    
    abstract class KafkaEventHandler
    {
        public abstract String getTopicAndPartition(final Event p0);
        
        public abstract Offset getKafkaOffset(final Event p0);
        
        public abstract byte[] getPositionBytesFromEvent(final Event p0);
    }
    
    class AvroEventHandler extends KafkaEventHandler
    {
        @Override
        public byte[] getPositionBytesFromEvent(final Event event) {
            final AvroEvent avroEvent = (AvroEvent)event;
            final GenericRecord record = (GenericRecord)avroEvent.data.get("__dataexchangemetadata");
            final String posString = record.get("position").toString();
            return Base64.decodeBase64(posString);
        }
        
        @Override
        public String getTopicAndPartition(final Event event) {
            final String topic = (String)((AvroEvent)event).metadata.get("TopicName");
            final int partitionID = (Integer)((AvroEvent)event).metadata.get("PartitionID");
            return topic + ":" + partitionID;
        }
        
        @Override
        public Offset getKafkaOffset(final Event event) {
            if (((AvroEvent)event).metadata.get("KafkaRecordOffset") != null) {
                final long recordOffset = (Long)((AvroEvent)event).metadata.get("KafkaRecordOffset");
                return (Offset)new KafkaLongOffset(recordOffset);
            }
            return (Offset)new KinesisOffset((BigInteger)((AvroEvent)event).metadata.get("KinesisRecordOffset"));
        }
    }
    
    class JSONEventHanlder extends KafkaEventHandler
    {
        @Override
        public byte[] getPositionBytesFromEvent(final Event event) {
            JsonNodeEvent jsonEvent = (JsonNodeEvent)event;
            String posString = jsonEvent.data.findValue("position").toString();
            return Base64.decodeBase64(posString);
        }
        
        @Override
        public String getTopicAndPartition(final Event event) {
            final String topic = (String)((JsonNodeEvent)event).metadata.get("TopicName");
            final int partitionID = (Integer)((JsonNodeEvent)event).metadata.get("PartitionID");
            return topic + ":" + partitionID;
        }
        
        @Override
        public Offset getKafkaOffset(final Event event) {
            if (((JsonNodeEvent)event).metadata.get("KafkaRecordOffset") != null) {
                final long recordOffset = (Long)((JsonNodeEvent)event).metadata.get("KafkaRecordOffset");
                return (Offset)new KafkaLongOffset(recordOffset);
            }
            return (Offset)new KinesisOffset((BigInteger)((JsonNodeEvent)event).metadata.get("KinesisRecordOffset"));
        }
    }
    
    class DSVEventHandler extends KafkaEventHandler
    {
        @Override
        public byte[] getPositionBytesFromEvent(final Event event) {
            final HDEvent hdEvent = (HDEvent)event;
            final String posString = (String)hdEvent.data[0];
            return Base64.decodeBase64(posString);
        }
        
        @Override
        public String getTopicAndPartition(final Event event) {
            final String topic = (String)((HDEvent)event).metadata.get("TopicName");
            final int partitionID = (Integer)((HDEvent)event).metadata.get("PartitionID");
            return topic + ":" + partitionID;
        }
        
        @Override
        public Offset getKafkaOffset(final Event event) {
            if (((HDEvent)event).metadata.get("KafkaRecordOffset") != null) {
                final long recordOffset = (Long)((HDEvent)event).metadata.get("KafkaRecordOffset");
                return (Offset)new KafkaLongOffset(recordOffset);
            }
            return (Offset)new KinesisOffset((BigInteger)((HDEvent)event).metadata.get("KinesisRecordOffset"));
        }
    }
    
    class XMLEventHanlder extends KafkaEventHandler
    {
        @Override
        public byte[] getPositionBytesFromEvent(final Event event) {
            final HDEvent hdEvent = (HDEvent)event;
            final String posString = (String)hdEvent.data[0];
            return Base64.decodeBase64(posString);
        }
        
        @Override
        public String getTopicAndPartition(final Event event) {
            final String topic = (String)((HDEvent)event).metadata.get("TopicName");
            final int partitionID = (Integer)((HDEvent)event).metadata.get("PartitionID");
            return topic + ":" + partitionID;
        }
        
        @Override
        public Offset getKafkaOffset(final Event event) {
            if (((HDEvent)event).metadata.get("KafkaRecordOffset") != null) {
                final long recordOffset = (Long)((HDEvent)event).metadata.get("KafkaRecordOffset");
                return (Offset)new KafkaLongOffset(recordOffset);
            }
            return (Offset)new KinesisOffset((BigInteger)((HDEvent)event).metadata.get("KinesisRecordOffset"));
        }
    }
}
