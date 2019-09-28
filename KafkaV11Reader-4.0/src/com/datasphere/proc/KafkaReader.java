package com.datasphere.proc;

import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.datasphere.common.exc.ConnectionException;
import com.datasphere.common.exc.SystemException;
import com.datasphere.distribution.RoundRobinPartitionManager;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.recovery.KafkaSourcePosition;
import com.datasphere.recovery.PartitionedSourcePosition;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.kafka.KafkaPartitionHandler;
import com.datasphere.source.kafka.KafkaProperty;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.type.positiontype;
import com.datasphere.uuid.UUID;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class KafkaReader extends SourceProcess
{
    public static final String DISTRIBUTION_ID = "distributionId";
    protected String topic;
    protected Map<Integer, KafkaPartitionHandler> partitionMap;
    protected PartitionedSourcePosition kafkaSourcePositions;
    protected KafkaConsumer<byte[], byte[]> oneConsumerForAllPartitions;
    protected positiontype posType;
    protected KafkaProperty prop;
    private static final Logger logger;
    private UUID sourceRef;
    private Map<String, Object> localCopyOfProperty;
    private volatile boolean stopFetching;
    private int fetchSize;
    private boolean sendPositions;
    private int partitionCount;
    private long retryBackoffms;
    private ExecutorService threadPool;
    private long pollTimeout;
    private List<TopicPartition> topicPartitionsAssignedToThisReader;
    private boolean autoMapPartition;
    private int emptyPartitionCount;
    private long totalMessagesRead;
    private long totalBytesRead;
    private Long prevTotalMessagesRead;
    private Long prevTotalBytesRead;
    private Long prevReadRate;
    private Long prevMsgRate;
    
    public KafkaReader() {
        this.stopFetching = false;
        this.sendPositions = false;
        this.partitionCount = 0;
        this.retryBackoffms = 1000L;
        this.pollTimeout = 10000L;
        this.topicPartitionsAssignedToThisReader = null;
        this.autoMapPartition = true;
        this.emptyPartitionCount = 0;
        this.totalMessagesRead = 0L;
        this.totalBytesRead = 0L;
        this.prevTotalMessagesRead = null;
        this.prevTotalBytesRead = null;
        this.prevReadRate = null;
        this.prevMsgRate = null;
    }
    
    public void init(final Map<String, Object> prop1, final Map<String, Object> prop2, final UUID uuid, final String distributionId, final SourcePosition startPosition, final boolean sendPositions, final Flow flow, final List<UUID> servers) throws Exception {
        super.init((Map)prop1, (Map)prop2, uuid, distributionId);
        this.sourceRef = uuid;
        (this.localCopyOfProperty = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER)).putAll(prop1);
        this.localCopyOfProperty.putAll(prop2);
        this.localCopyOfProperty.put(BaseReader.SOURCE_PROCESS, this);
        this.localCopyOfProperty.put(Property.SOURCE_UUID, this.sourceRef);
        this.localCopyOfProperty.put("distributionId", distributionId);
        this.localCopyOfProperty.put("so_timeout", 30010);
        this.kafkaSourcePositions = new PartitionedSourcePosition();
        this.prop = new KafkaProperty((Map)this.localCopyOfProperty);
        this.topic = this.prop.topic;
        this.posType = this.prop.posType;
        final String partitionList = this.prop.partitionList;
        this.autoMapPartition = this.prop.getBoolean("AutoMapPartition", true);
        this.partitionMap = null;
        this.fetchSize = 10485760;
        this.stopFetching = false;
        this.sendPositions = sendPositions;
        this.partitionCount = 0;
        final Properties props = new Properties();
        final String[] kafkaConfig = this.prop.getKafkaBrokerConfigList();
        props.put("max.partition.fetch.bytes", this.fetchSize);
        props.put("fetch.min.bytes", 1048576);
        props.put("fetch.max.wait.ms", 1000);
        props.put("receive.buffer.bytes", 2000000);
        props.put("request.timeout.ms", 60001);
        props.put("session.timeout.ms", 60000);
        if (kafkaConfig != null) {
            for (int i = 0; i < kafkaConfig.length; ++i) {
                final String[] property = kafkaConfig[i].split("=");
                if (property == null || property.length < 2) {
                    KafkaReader.logger.warn((Object)("Kafka Property \"" + property[0] + "\" is invalid."));
                    KafkaReader.logger.warn((Object)("Invalid \"KafkaConfig\" property structure " + property[0] + ". Expected structure <name>=<value>;<name>=<value>"));
                }
                else if (property[0].equalsIgnoreCase("retry.backoff.ms")) {
                    this.retryBackoffms = Long.parseLong(property[1]);
                    props.put("retry.backoff.ms", this.retryBackoffms);
                }
                else if (property[0].equalsIgnoreCase("max.partition.fetch.bytes")) {
                    this.fetchSize = Integer.parseInt(property[1]);
                    if (this.fetchSize < 1000000) {
                        KafkaReader.logger.warn((Object)"Value of max.partition.fetch.bytes is less than default value of topic configuration max.message.bytes (1,000,000). Consumer can get stuck trying to fetch a large message on a certain partition.");
                    }
                    props.put("max.partition.fetch.bytes", this.fetchSize);
                }
                else if (property[0].equalsIgnoreCase("poll.timeout.ms")) {
                    this.pollTimeout = Long.parseLong(property[1]);
                    if (this.pollTimeout != 10000L && KafkaReader.logger.isInfoEnabled()) {
                        KafkaReader.logger.info((Object)("Overriding the default config of poll.timeout.ms to " + this.pollTimeout));
                    }
                }
                else if (property[0].equalsIgnoreCase("batch.size")) {
                    final int batchSize = Integer.parseInt(property[1]);
                    if (batchSize > 10485760) {
                        this.fetchSize = batchSize;
                        props.put("max.partition.fetch.bytes", this.fetchSize);
                    }
                }
                else if (!property[0].equalsIgnoreCase("linger.ms") && !property[0].equalsIgnoreCase("acks")) {
                    if (!property[0].equalsIgnoreCase("max.request.size")) {
                        props.put(property[0], property[1]);
                    }
                }
            }
        }
        final int requestTimeoutMs = Integer.parseInt(props.get("request.timeout.ms").toString());
        final int sessionTimeoutMs = Integer.parseInt(props.get("session.timeout.ms").toString());
        if (requestTimeoutMs <= sessionTimeoutMs) {
            throw new RuntimeException("'request.timeout.ms' cannot be less than equal to 'session.timeout.ms'. Default for 'request.timeout.ms' is 60001 and 'session.timeout.ms' is 60000");
        }
        if (props.containsKey("retry.backoff.ms")) {
            final int retryBackoffMs = Integer.parseInt(props.get("retry.backoff.ms").toString());
            final float minRetries = requestTimeoutMs / retryBackoffMs;
            if (minRetries <= 2.0f && KafkaReader.logger.isInfoEnabled()) {
                KafkaReader.logger.info((Object)"Number of retries is too less. Make sure brokers respond within this many retries or else KafkaConsumer will throw a TimeoutException.");
            }
        }
        if (this.topic.isEmpty()) {
            throw new RuntimeException("Invalid topic name. Topic name cannot be empty.");
        }
        if (this.prop.kafkaBrokerAddress == null) {
            throw new RuntimeException("Invalid Broker Address " + this.prop.kafkaBrokerAddress + ". BrokerAddress cannot be empty.");
        }
        props.put("bootstrap.servers", this.prop.getBrokerAddress());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        Object metaObject = null;
        String sourceName = "unknown_consumer";
        try {
            metaObject = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceRef, HDSecurityManager.TOKEN);
        }
        catch (Exception e) {
            KafkaReader.logger.warn((Object)("Unknown component UUID " + this.sourceRef + "." + e));
        }
        finally {
            if (metaObject != null) {
                sourceName = ((MetaInfo.MetaObject)metaObject).name;
            }
        }
        final String clientId = "DataExchange_KafkaReader_" + new UUID(System.currentTimeMillis()) + sourceName + ((distributionId != null && !distributionId.isEmpty()) ? ("_" + distributionId + "_") : "_") + this.topic;
        if (KafkaReader.logger.isInfoEnabled()) {
            KafkaReader.logger.info((Object)("KafkaConsumer client ID " + clientId));
        }
        props.put("client.id", clientId);
        this.oneConsumerForAllPartitions = new KafkaConsumer<byte[], byte[]>(props);
        List<PartitionInfo> partitionInfo;
        try {
            partitionInfo = this.oneConsumerForAllPartitions.partitionsFor(this.topic);
        }
        catch (Exception e3) {
            throw new SystemException("Problem in retreiving the Topic (" + this.topic + ") metadata. Please check if broker(s) at " + this.prop.getBrokerAddress() + " is available.");
        }
        if (partitionInfo == null) {
            throw new SystemException("Topic Not Found. Please check if the topic " + this.topic + " already exits.");
        }
        this.topicPartitionsAssignedToThisReader = new ArrayList<TopicPartition>();
        final List<Integer> userDefiniedPartitions = new ArrayList<Integer>();
        if (partitionList != null && !partitionList.trim().isEmpty()) {
            final String[] partitionIds = partitionList.split(";");
            for (int j = 0; j < partitionIds.length; ++j) {
                try {
                    final int partitionId = Integer.parseInt(partitionIds[j]);
                    if (partitionId < 0 || partitionId > partitionInfo.size() - 1) {
                        KafkaReader.logger.warn((Object)("Partition with Id - " + partitionIds[j] + " does not exist in " + this.topic));
                    }
                    else {
                        for (final PartitionInfo pInfo : partitionInfo) {
                            if (pInfo.partition() == partitionId) {
                                if (!this.autoMapPartition) {
                                    this.topicPartitionsAssignedToThisReader.add(new TopicPartition(this.topic, partitionId));
                                }
                                else {
                                    userDefiniedPartitions.add(partitionId);
                                }
                                ++this.partitionCount;
                                break;
                            }
                        }
                    }
                }
                catch (NumberFormatException e2) {
                    KafkaReader.logger.warn((Object)("Invalid partition Id '" + partitionIds[j] + "' found in the list " + partitionList + ". " + e2));
                }
            }
        }
        else {
            if (!this.autoMapPartition) {
                for (final PartitionInfo pInfo2 : partitionInfo) {
                    this.topicPartitionsAssignedToThisReader.add(new TopicPartition(this.topic, pInfo2.partition()));
                }
            }
            this.partitionCount = partitionInfo.size();
        }
        if (this.partitionCount < 1) {
            throw new SystemException("Invalid Topic name (" + this.topic + ") or partition list (" + partitionList + ")");
        }
        if (this.autoMapPartition) {
            final UUID this_server_uuid = BaseServer.getBaseServer().getServerID();
            if (KafkaReader.logger.isInfoEnabled()) {
                KafkaReader.logger.info((Object)("Auto mapping is enabled. List of server where " + sourceName + " is deployed - " + servers + ".\nCurrent Server UUID is " + this_server_uuid + ".Topic " + this.topic + " has " + this.partitionCount + " partitions totally."));
            }
            final RoundRobinPartitionManager rrm = new RoundRobinPartitionManager((List)servers);
            if (userDefiniedPartitions.isEmpty()) {
                for (int partitionId = 0; partitionId < this.partitionCount; ++partitionId) {
                    final UUID serverUUID = rrm.getOwnerForPartition();
                    if (serverUUID.equals((Object)this_server_uuid)) {
                        this.topicPartitionsAssignedToThisReader.add(new TopicPartition(this.topic, partitionId));
                    }
                }
            }
            else {
                for (final Integer partitionId2 : userDefiniedPartitions) {
                    final UUID serverUUID2 = rrm.getOwnerForPartition();
                    if (serverUUID2.equals((Object)this_server_uuid)) {
                        this.topicPartitionsAssignedToThisReader.add(new TopicPartition(this.topic, partitionId2));
                    }
                }
            }
            if (this.topicPartitionsAssignedToThisReader.isEmpty()) {
                KafkaReader.logger.warn((Object)("Seems like there are enough KafkaReader started already to read from all partitions in topic " + this.topic));
                if (this.oneConsumerForAllPartitions != null) {
                    this.oneConsumerForAllPartitions.close();
                    this.oneConsumerForAllPartitions = null;
                }
                return;
            }
            this.partitionCount = this.topicPartitionsAssignedToThisReader.size();
            if (KafkaReader.logger.isInfoEnabled()) {
                KafkaReader.logger.info((Object)("Will be reading from " + this.topicPartitionsAssignedToThisReader));
            }
        }
        this.assignPartitions(this.topicPartitionsAssignedToThisReader);
        if (startPosition != null && startPosition instanceof PartitionedSourcePosition && !((PartitionedSourcePosition)startPosition).isEmpty() && this.sendPositions) {
            this.kafkaSourcePositions = (PartitionedSourcePosition)startPosition;
            if (KafkaReader.logger.isInfoEnabled()) {
                KafkaReader.logger.info((Object)("Restart position " + this.kafkaSourcePositions.toString()));
            }
            this.posType = positiontype.WA_POSITION_OFFSET;
        }
        else if (this.posType.equals((Object)positiontype.WA_POSITION_OFFSET)) {
            for (final TopicPartition tp : this.topicPartitionsAssignedToThisReader) {
                final String key = this.topic + "-" + tp.partition();
                final KafkaSourcePosition sp = new KafkaSourcePosition(this.topic, tp.partition(), 0L, (long)this.prop.positionValue);
                this.kafkaSourcePositions.put(key, (SourcePosition)sp);
            }
        }
        final ThreadFactory kafkaParserThreadFactory = new ThreadFactoryBuilder().setNameFormat("KafkaParserThread-%d").build();
        this.threadPool = Executors.newCachedThreadPool(kafkaParserThreadFactory);
        this.initializeParser(this.topicPartitionsAssignedToThisReader);
        this.setConsumerPosition(this.topicPartitionsAssignedToThisReader, partitionInfo.size());
        if (KafkaReader.logger.isInfoEnabled()) {
            KafkaReader.logger.info((Object)("Will be reading from " + this.posType.toString() + " from topic " + this.topic + " partitions " + this.topicPartitionsAssignedToThisReader.toString()));
        }
    }
    
    public void assignPartitions(final List<TopicPartition> topicPartitions) {
        this.oneConsumerForAllPartitions.assign((List)topicPartitions);
    }
    
    public abstract void setConsumerPosition(final List<TopicPartition> p0, final int p1);
    
    public String getTopic() {
        return this.topic;
    }
    
    public Position getCheckpoint() {
		if (this.sendPositions && !this.stopFetching) {
			final PathManager p = new PathManager();
			if (!this.partitionMap.isEmpty()) {
				for (final Map.Entry item : this.partitionMap.entrySet()) {
					final KafkaPartitionHandler metadata = (KafkaPartitionHandler) item.getValue();
					final KafkaSourcePosition sp = metadata.getCurrentPosition();
					if (sp != null) {
						p.mergeHigherPositions(
								Position.from(this.sourceUUID, metadata.getDistributionId(), (SourcePosition) sp));
					}
				}
				return p.toPosition();
			}
		}
		return null;
	}
    
    public void initializeParser(final List<TopicPartition> topicPartitions) throws Exception {
        this.partitionMap = new ConcurrentHashMap<Integer, KafkaPartitionHandler>(topicPartitions.size());
        for (final TopicPartition tp : topicPartitions) {
            final KafkaPartitionHandler kph = new KafkaPartitionHandler();
            final KafkaProperty tmpProp = new KafkaProperty(this.prop.propMap);
            tmpProp.propMap.put("PartitionID", tp.partition());
            tmpProp.propMap.put("fetchSize", this.fetchSize);
            kph.init((Property)tmpProp);
            kph.sendPosition(this.sendPositions);
            this.partitionMap.put(tp.partition(), kph);
            this.threadPool.submit((Runnable)kph);
        }
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        final Long bytesRead = this.totalBytesRead;
        if (this.totalBytesRead > 0L) {
            final Long readRate = events.getRate(bytesRead, this.prevTotalBytesRead);
            if (readRate != null && !readRate.equals(this.prevReadRate)) {
                final double rateInMb = readRate / 1048576L;
                events.add(MonitorEvent.Type.KAFKA_CONSUMER_RATE_MB_per_SEC, String.valueOf(rateInMb), MonitorEvent.Operation.SUM);
            }
            this.prevReadRate = readRate;
        }
        final Long msgRead = this.totalMessagesRead;
        if (!msgRead.equals(this.prevTotalMessagesRead)) {
            events.add(MonitorEvent.Type.TOTAL_KAFKA_MSGS_READ, this.totalMessagesRead, MonitorEvent.Operation.SUM);
        }
        final Long messageRate = events.getRate(msgRead, this.prevTotalMessagesRead);
        if (messageRate != null && !messageRate.equals(this.prevMsgRate)) {
            events.add(MonitorEvent.Type.KAFKA_CONSUMER_MSG_RATE, messageRate, MonitorEvent.Operation.SUM);
        }
        if (this.topicPartitionsAssignedToThisReader != null && !this.topicPartitionsAssignedToThisReader.isEmpty()) {
            events.add(MonitorEvent.Type.KAFKA_READER_PARTITION_DISTRIBUTION_LIST, this.topicPartitionsAssignedToThisReader.toString(), MonitorEvent.Operation.NONE);
        }
        else {
            events.add(MonitorEvent.Type.KAFKA_READER_PARTITION_DISTRIBUTION_LIST, "N/A", MonitorEvent.Operation.NONE);
        }
        this.prevMsgRate = messageRate;
        this.prevTotalBytesRead = this.totalBytesRead;
        this.prevTotalMessagesRead = this.totalMessagesRead;
    }
    
    public void startConsumingData() throws Exception {
        final ByteBuffer payload = ByteBuffer.allocate(this.fetchSize + Constant.INTEGER_SIZE + Constant.LONG_SIZE + Constant.BYTE_SIZE);
        try {
            if (this.partitionMap.isEmpty()) {
                this.stopFetching = true;
                KafkaReader.logger.warn((Object)"No Kafka Brokers available to fetch data ");
                throw new ConnectionException("Failure in connecting to Kafka broker. No Kafka Brokers available to fetch data.");
            }
            final ConsumerRecords<byte[], byte[]> records = this.oneConsumerForAllPartitions.poll(this.pollTimeout);
            if (this.stopFetching) {
                if (KafkaReader.logger.isInfoEnabled()) {
                    KafkaReader.logger.info((Object)("Stopped consuming data from topic " + this.topic));
                }
                return;
            }
            if (records != null && records.count() > 0) {
                final Set<TopicPartition> recordsForPartitions = records.partitions();
                this.totalMessagesRead += records.count();
                for (final TopicPartition tp : recordsForPartitions) {
                    long offset = 0L;
                    long recordsFetchedPerPartition = 0L;
                    for (final ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                        final KafkaPartitionHandler parserForPartition = this.partitionMap.get(tp.partition());
                        if (this.stopFetching) {
                            if (KafkaReader.logger.isInfoEnabled()) {
                                KafkaReader.logger.info((Object)("Stopped consuming data from topic " + this.topic));
                            }
                            return;
                        }
                        final int recordSize = record.value().length;
                        this.totalBytesRead += recordSize;
                        offset = record.offset();
                        final byte[] bytes = new byte[recordSize + Constant.BYTE_SIZE + Constant.LONG_SIZE + Constant.INTEGER_SIZE];
                        payload.putLong(offset);
                        payload.put((byte)0);
                        payload.putInt(recordSize);
                        payload.put(record.value());
                        payload.flip();
                        payload.get(bytes);
                        if (parserForPartition.getPipedOut() != null) {
                            parserForPartition.getPipedOut().write(bytes);
                        }
                        payload.clear();
                        ++recordsFetchedPerPartition;
                    }
                }
            }
            else {
                ++this.emptyPartitionCount;
            }
            if (this.emptyPartitionCount > 10000) {
                Thread.sleep(5L);
                this.emptyPartitionCount = 0;
            }
        }
        catch (Exception e) {
            if (e instanceof InterruptedException || e instanceof InterruptedIOException) {
                if (KafkaReader.logger.isDebugEnabled()) {
                    KafkaReader.logger.debug((Object)e);
                }
            }
            else if (!this.stopFetching) {
                throw new ConnectionException((Throwable)e);
            }
        }
    }
    
    public void receiveImpl(final int channel, final Event out) throws Exception {
        try {
            synchronized (this) {
                if (!this.stopFetching) {
                    if (this.partitionMap != null) {
                        this.startConsumingData();
                    }
                    else {
                        Thread.sleep(5L);
                    }
                }
            }
        }
        catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
                KafkaReader.logger.error((Object)e);
                throw new ConnectionException("Failure in reading from " + this.topicPartitionsAssignedToThisReader, (Throwable)e);
            }
            if (KafkaReader.logger.isDebugEnabled()) {
                KafkaReader.logger.debug((Object)("Sleep interrupted while stopping application." + e));
            }
        }
    }
    
    public void close() throws Exception {
        this.stopFetching = true;
        if (this.partitionMap != null && !this.partitionMap.isEmpty()) {
            final Iterator it = this.partitionMap.entrySet().iterator();
            while (it.hasNext()) {
            		((KafkaPartitionHandler)	((Map.Entry) it.next()).getValue()).stopFetching(true);
            }
            while (it.hasNext()) {
                try {
                		((KafkaPartitionHandler)	((Map.Entry) it.next()).getValue()).cleanUp();
                    it.remove();
                }
                catch (Exception e) {
                    KafkaReader.logger.warn((Object)("Problem while stopping KafkaReader " + e.getMessage()));
                }
            }
        }
        if (this.oneConsumerForAllPartitions != null) {
            this.oneConsumerForAllPartitions.wakeup();
        }
        synchronized (this) {
            if (this.oneConsumerForAllPartitions != null) {
                this.oneConsumerForAllPartitions.close();
                this.oneConsumerForAllPartitions = null;
            }
            if (this.threadPool != null) {
                this.threadPool.shutdown();
                try {
                    if (!this.threadPool.awaitTermination(2000L, TimeUnit.MILLISECONDS)) {
                        this.threadPool.shutdownNow();
                    }
                }
                catch (InterruptedException e2) {
                    if (KafkaReader.logger.isInfoEnabled()) {
                        KafkaReader.logger.info((Object)("Parser thread interrupted while terminating forcefully " + e2));
                    }
                }
            }
            this.threadPool = null;
            if (this.partitionMap != null) {
                this.partitionMap.clear();
                this.partitionMap = null;
            }
        }
    }
    
    public boolean requiresPartitionedSourcePosition() {
        return true;
    }
    
    public boolean isAdapterShardable() {
        return true;
    }
    
    public static void main(final String[] args) {
        final String did = "node2";
        final HashMap<String, Object> mr = new HashMap<String, Object>();
        mr.put("brokerAddress", "localhost:9192");
        mr.put("startOffset", 0);
        mr.put("Topic", "test4p");
        mr.put("blocksize", "64");
        mr.put("KafkaConfig", "group.id=testautobalancing;enable.auto.commit=false");
        final HashMap<String, Object> mp = new HashMap<String, Object>();
        mp.put("handler", "DSVParser");
        mp.put("blocksize", "256");
        mp.put("columndelimiter", ",");
        mp.put("rowdelimiter", "\n");
        mp.put("charset", "UTF-8");
        System.out.println("Gonna initialize");
        final UUID uuid = new UUID(System.currentTimeMillis());
        mp.put(Property.SOURCE_UUID, uuid);
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaReader.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
