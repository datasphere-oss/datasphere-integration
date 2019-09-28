package com.datasphere.target.kafka;

import com.datasphere.proc.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import org.apache.log4j.*;
import com.datasphere.source.lib.utils.lookup.*;
import com.datasphere.source.lib.constant.*;
import com.datasphere.security.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.event.*;
import com.datasphere.persistence.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.recovery.*;
import com.datasphere.kafka.*;
import java.util.*;

public abstract class MessageBusWriter extends BaseWriter implements Acknowledgeable
{
    protected String topic;
    protected int noOfPartitions;
    protected HashSet<String> replicaList;
    protected int maxRequestSize;
    protected Producer producer;
    protected boolean e1p;
    protected String mode;
    protected boolean isBinaryFormatter;
    protected boolean isNativeFormatter;
    protected boolean isBinaryFormatterWithSchemaRegistry;
    protected UUID uuid;
    private static Logger logger;
    protected volatile boolean isRunning;
    protected volatile long bytesSent;
    private static final String PARTITIONKEY = "PartitionKey";
    protected EventLookup eventLookup;
    protected String partitionKey;
    private Long prevBytesSent;
    private Long prevBytesSentRate;
    private double bytesSentRateDouble;
    
    public MessageBusWriter() {
        this.topic = null;
        this.noOfPartitions = 0;
        this.maxRequestSize = 1048576;
        this.e1p = false;
        this.mode = "Sync";
        this.isBinaryFormatter = false;
        this.isNativeFormatter = false;
        this.isBinaryFormatterWithSchemaRegistry = false;
        this.isRunning = false;
        this.bytesSent = 0L;
        this.eventLookup = null;
        this.prevBytesSent = null;
        this.prevBytesSentRate = null;
        this.bytesSentRateDouble = 0.0;
    }
    
    public void init(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties, final UUID inputStream, final String distributionID) throws Exception {
        super.init((Map)writerProperties, (Map)formatterProperties, inputStream, distributionID);
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(writerProperties);
        localPropertyMap.putAll(formatterProperties);
        this.uuid = (UUID)localPropertyMap.get("TargetUUID");
        this.noOfPartitions = 0;
        if (localPropertyMap.containsKey("PartitionKey") && localPropertyMap.get("PartitionKey") != null) {
            this.partitionKey = (String)localPropertyMap.get("PartitionKey");
            if (this.partitionKey.trim().isEmpty()) {
                throw new IllegalArgumentException("Specfied PartitionKey property is empty. Please specify a valid value.");
            }
            final String[] partitionKeys = this.partitionKey.split(",");
            this.eventLookup = EventLookupFactory.getEventLookup(this.fields, partitionKeys);
        }
        else if (localPropertyMap.containsKey("partitionFieldIndex")) {
            final int[] keyFieldIndex = (int[])localPropertyMap.get("partitionFieldIndex");
            if (keyFieldIndex != null && keyFieldIndex.length > 0) {
                final String[] fieldNames = new String[keyFieldIndex.length];
                for (int i = 0; i < keyFieldIndex.length; ++i) {
                    fieldNames[i] = this.fields[keyFieldIndex[i]].getName();
                }
                this.eventLookup = EventLookupFactory.getEventLookup(this.fields, fieldNames);
            }
        }
        this.mode = "BatchedSync";
        final String formatterName = (String)formatterProperties.get("formatterName");
        this.isBinaryFormatter = formatterName.equalsIgnoreCase("AvroFormatter");
        if (this.isBinaryFormatter && localPropertyMap.containsKey("formatAs") && localPropertyMap.get("formatAs") != null) {
            if (!localPropertyMap.get("formatAs").toString().equalsIgnoreCase(Constant.FormatOptions.Default.toString())) {
                Label_0470: {
                    if (localPropertyMap.get("schemaregistryurl") != null) {
                        if (!localPropertyMap.get("schemaregistryurl").toString().isEmpty()) {
                            break Label_0470;
                        }
                    }
                    try {
                        final MetaInfo.MetaObject target = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.uuid, HDSecurityManager.TOKEN);
                        throw new Exception("Problem in initializing " + target.nsName + "." + target.name + ". AvroFormatter requires schemaregistryurl to connect to schema registry in case of " + localPropertyMap.get("formatAs") + " formatting.");
                    }
                    catch (MetaDataRepositoryException e) {
                        throw new RuntimeException("Problem while getting Target Info from metadata repository " + e);
                    }
                }
                this.isNativeFormatter = true;
            }
            else if (localPropertyMap.get("schemaregistryurl") != null && !localPropertyMap.get("schemaregistryurl").toString().isEmpty()) {
                this.isBinaryFormatterWithSchemaRegistry = true;
            }
            else if (localPropertyMap.get("schemaFileName") == null || localPropertyMap.get("schemaFileName").toString().isEmpty()) {
                throw new IllegalArgumentException("Both SchemaFileName or schemaregistryurl property is not set in AvroForamtter. Either one has to be specified for Kafka based Writer.");
            }
        }
    }
    
    public void receiveImpl(final int channel, final Event event) throws Exception {
        this.receive(channel, event, (ImmutableStemma)null);
    }
    
    public void close() throws Exception {
        this.isRunning = false;
        try {
            if (this.producer != null) {
                this.producer.close();
                this.producer = null;
            }
        }
        catch (Exception e) {
            MessageBusWriter.logger.warn((Object)("Error while stopping KafkaWriter, it can be ignored " + e));
        }
    }
    
    public abstract Integer getPartitionId(final Event p0) throws Exception;
    
    public String getDistributionId(final Event event) throws Exception {
        return this.getPartitionId(event).toString();
    }
    
    protected KWCheckpointPersistenceLayer initPersistentLayer() {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("javax.persistence.jdbc.user", MetaDataDBOps.DBUname);
        properties.put("javax.persistence.jdbc.password", MetaDataDBOps.DBPassword);
        properties.put("javax.persistence.jdbc.url", BaseServer.getMetaDataDBProviderDetails().getJDBCURL(MetaDataDBOps.DBLocation, MetaDataDBOps.DBName, MetaDataDBOps.DBUname, MetaDataDBOps.DBPassword));
        properties.put("javax.persistence.jdbc.driver", BaseServer.getMetaDataDBProviderDetails().getJDBCDriver());
        properties.put("eclipselink.weaving", "STATIC");
        properties.put("eclipselink.ddl-generation", "create-tables");
        properties.put("eclipselink.ddl-generation.output-mode", "database");
        final KWCheckpointPersistenceLayer jpaPersistanceLayer = new KWCheckpointPersistenceLayer("KafkaWriterCheckpoint", (Map)properties);
        jpaPersistanceLayer.init();
        return jpaPersistanceLayer;
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        if (this.producer != null) {
            if (this.mode.equalsIgnoreCase("Sync") || this.mode.equalsIgnoreCase("BatchedSync")) {
                events.add(MonitorEvent.Type.AVG_LATENCY, (String)this.producer.getAvgLatency(), MonitorEvent.Operation.AVG);
                events.add(MonitorEvent.Type.MAX_LATENCY, (Long)this.producer.getMaxLatency(), MonitorEvent.Operation.MAX);
            }
            events.add(MonitorEvent.Type.AVG_TIME_BETWEEN_TWO_SEND_CALLS, (String)this.producer.getAvgTimeBtwSendCalls(), MonitorEvent.Operation.AVG);
            events.add(MonitorEvent.Type.MAX_TIME_BETWEEN_TWO_SEND_CALLS, (Long)this.producer.getMaxTimeBtwSendCalls(), MonitorEvent.Operation.MAX);
            events.add(this.getSendCallsMonitorType(), this.producer.getNoOfSendCalls(), MonitorEvent.Operation.SUM);
            Long bytesSentRate = null;
            if (this.bytesSent > 0L) {
                bytesSentRate = events.getRate(this.bytesSent, this.prevBytesSent);
                if (bytesSentRate != null && !bytesSentRate.equals(this.prevBytesSentRate)) {
                    if (bytesSentRate > 0L) {
                        bytesSentRate /= 1048576L;
                    }
                    events.add(MonitorEvent.Type.SENT_BYTES_RATE_MB_per_SEC, bytesSentRate, MonitorEvent.Operation.SUM);
                }
            }
            this.prevBytesSent = this.bytesSent;
            this.prevBytesSentRate = bytesSentRate;
        }
    }
    
    public MetaInfo.MetaObject onUpgrade(final MetaInfo.MetaObject metaObject, final String fromVersion, final String toVersion) throws Exception {
        final MetaInfo.Target targetMetaObject = (MetaInfo.Target)metaObject;
        if (MessageBusWriter.logger.isInfoEnabled()) {
            MessageBusWriter.logger.info((Object)("PropertySet of " + targetMetaObject.adapterClassName + " from previous version " + targetMetaObject.properties));
        }
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(targetMetaObject.properties);
        if (!localPropertyMap.containsKey("Mode")) {
            String writerMode = "Sync";
            if (targetMetaObject.properties.containsKey("KafkaConfig")) {
                final String value = (String)targetMetaObject.properties.get("KafkaConfig");
                if (value != null && !value.isEmpty()) {
                    String newkafkaConfig = null;
                    final String[] split;
                    final String[] splitValues = split = value.split(";");
                    for (final String splitValue : split) {
                        final String[] property = splitValue.split("=");
                        if (property != null && property.length == 2 && property[0].equalsIgnoreCase("producer.type")) {
                            if (property[1].equalsIgnoreCase("Sync") || property[1].equalsIgnoreCase("Async")) {
                                writerMode = property[1];
                            }
                            else {
                                MessageBusWriter.logger.warn((Object)("Invalid value " + property[1] + " found for property \"producer.type\". Mode is set to \"Sync\"."));
                            }
                        }
                        else if (newkafkaConfig == null) {
                            newkafkaConfig = splitValue;
                        }
                        else {
                            newkafkaConfig = newkafkaConfig + ";" + splitValue;
                        }
                    }
                    targetMetaObject.properties.put("KafkaConfig", newkafkaConfig);
                }
            }
            targetMetaObject.properties.put("Mode", writerMode);
            targetMetaObject.properties.put("KafkaMessageFormatVersion", "v1");
            if (MessageBusWriter.logger.isInfoEnabled()) {
                MessageBusWriter.logger.info((Object)("Modified Property Set of " + targetMetaObject.adapterClassName + " is " + targetMetaObject.properties));
            }
        }
        KWCheckpointPersistenceLayer checkpointPersistenceLayer = null;
        try {
            checkpointPersistenceLayer = this.initPersistentLayer();
            final int noOfUpdates = checkpointPersistenceLayer.upgradeKWCheckpointTable((MetaInfo.MetaObject)targetMetaObject);
            if (noOfUpdates > 0 && MessageBusWriter.logger.isInfoEnabled()) {
                MessageBusWriter.logger.info((Object)("Upgraded " + noOfUpdates + " entries respective to " + metaObject.nsName + "." + metaObject.name + " in KWCheckpoint table."));
            }
        }
        catch (Exception e) {
            MessageBusWriter.logger.error((Object)("Upgrade failed due to : " + e));
        }
        finally {
            if (checkpointPersistenceLayer != null) {
                checkpointPersistenceLayer.close();
                checkpointPersistenceLayer = null;
            }
        }
        return (MetaInfo.MetaObject)targetMetaObject;
    }
    
    public Position getWaitPosition() throws Exception {
        final PathManager waitposition = new PathManager();
        if (this.e1p) {
            final Map<String, OffsetPosition> waitPositionOfPartitions = ((BatchedSyncProducer)this.producer).getWaitPosition();
            for (final OffsetPosition op : waitPositionOfPartitions.values()) {
                waitposition.mergeHigherPositions((Position)op);
            }
            if (!waitposition.isEmpty()) {
                if (MessageBusWriter.logger.isDebugEnabled()) {
                    MessageBusWriter.logger.debug((Object)("Merged wait position of " + this.topic + " is " + waitposition.toString()));
                }
                return waitposition.toPosition();
            }
        }
        return null;
    }
    
    public abstract MonitorEvent.Type getSendCallsMonitorType();
    
    static {
        MessageBusWriter.logger = Logger.getLogger((Class)MessageBusWriter.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}

