package com.datasphere.target.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.log4j.Logger;

import com.datasphere.common.exc.ConnectionException;
import com.datasphere.event.Event;
import com.datasphere.kafka.PartitionerIntf;
import com.datasphere.recovery.Acknowledgeable;
import com.datasphere.recovery.ImmutableStemma;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.source.kafka.KafkaProperty;
import com.datasphere.uuid.UUID;

public abstract class KafkaWriter extends MessageBusWriter implements Acknowledgeable
{
    private PartitionerIntf partitioner;
    private static Logger logger;
    protected KafkaProperty kafkaProp;
    
    public KafkaWriter() {
        this.kafkaProp = null;
    }
    
    @Override
    public void init(final Map<String, Object> writerProperties, final Map<String, Object> formatterProperties, final UUID inputStream, final String distributionID) throws Exception {
        super.init(writerProperties, formatterProperties, inputStream, distributionID);
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(writerProperties);
        localPropertyMap.putAll(formatterProperties);
        this.kafkaProp = new KafkaProperty((Map)localPropertyMap);
        this.topic = this.kafkaProp.topic;
        final String[] kafkaConfig = this.kafkaProp.getKafkaBrokerConfigList();
        this.mode = (String)localPropertyMap.get("Mode");
        final Properties props = new Properties();
        props.put("request.timeout.ms", 60001);
        props.put("session.timeout.ms", 60000);
        if (kafkaConfig != null) {
            for (int i = 0; i < kafkaConfig.length; ++i) {
                final String[] property = kafkaConfig[i].split("=");
                if (property == null || property.length < 2) {
                    KafkaWriter.logger.warn((Object)("Kafka Property \"" + property[0] + "\" is invalid."));
                    KafkaWriter.logger.warn((Object)("Invalid \"KafkaProducerConfig\" property structure " + property[0] + ". Expected structure <name>=<value>;<name>=<value>"));
                }
                else if (property[0].equalsIgnoreCase("key.serializer.class")) {
                    KafkaWriter.logger.warn((Object)"User defined Serializer is not supported.");
                }
                else if (property[0].equalsIgnoreCase("producer.type")) {
                    KafkaWriter.logger.warn((Object)"Configuration \"producer.type\" is deprecated. Please set it via Mode=[Sync/Async] property.");
                    if (property[1].equalsIgnoreCase("Sync") || property[1].equalsIgnoreCase("Async")) {
                        KafkaWriter.logger.warn((Object)("Mode is overriden to " + property[1]));
                        this.mode = property[1];
                    }
                    else {
                        KafkaWriter.logger.warn((Object)("Invalid value " + property[1] + " found for property \"producer.type\". Mode is set to \"Sync\"."));
                    }
                }
                else if (property[0].equalsIgnoreCase("partitioner.class")) {
                    final String partitionerClassName = property[1];
                    final Class<?> partitinerClass = ClassLoader.getSystemClassLoader().loadClass(partitionerClassName);
                    final Object o = partitinerClass.newInstance();
                    if (!PartitionerIntf.class.isInstance(o)) {
                        throw new RuntimeException("Invalid partitioner class specified. " + partitionerClassName + " is not of type " + PartitionerIntf.class.getName());
                    }
                    this.partitioner = (PartitionerIntf)o;
                }
                else {
                    props.put(property[0], property[1]);
                    if (property[0].equals("max.request.size") && Integer.parseInt(property[1]) > this.maxRequestSize) {
                        this.maxRequestSize = Integer.parseInt(property[1]);
                    }
                    if (KafkaWriter.logger.isInfoEnabled()) {
                        KafkaWriter.logger.info((Object)("Kafka Producer property \"" + property[0] + "\" is set with value \"" + property[1] + "\""));
                    }
                }
            }
        }
        final int requestTimeoutMs = Integer.parseInt(props.get("request.timeout.ms").toString());
        final int sessionTimeoutMs = Integer.parseInt(props.get("session.timeout.ms").toString());
        if (requestTimeoutMs <= sessionTimeoutMs) {
            throw new RuntimeException("'request.timeout.ms' cannot be less than equal to 'session.timeout.ms'. Default for 'request.timeout.ms' is 60001 and 'session.timeout.ms' is 60000");
        }
        if (this.partitioner == null) {
            this.partitioner = (PartitionerIntf)new DefaultKWPartitioner();
        }
        String messageVersion = "v2";
        if (localPropertyMap.containsKey("KafkaMessageFormatVersion")) {
            messageVersion = (String)localPropertyMap.get("KafkaMessageFormatVersion");
            if (!messageVersion.equalsIgnoreCase("v1") && !messageVersion.equalsIgnoreCase("v2")) {
                throw new RuntimeException("Invalid value " + messageVersion + " for \"" + "KafkaMessageFormatVersion" + "\" property. Accepted values - v1/v2");
            }
        }
        if (this.mode.equalsIgnoreCase("Sync") && !((String)localPropertyMap.get("adapterName")).equalsIgnoreCase("MapRStreamWriter") && messageVersion.equalsIgnoreCase("v2")) {
            if (super.isRecoveryEnabled) {
                this.e1p = true;
                if (props.containsKey("acks") && props.getProperty("acks").equalsIgnoreCase("0")) {
                    KafkaWriter.logger.warn((Object)"Setting \"acks\"=0 is not a optimal configuration in sync mode. Updating to default configuration \"acks\"=1");
                    props.put("acks", "1");
                }
                if (props.containsKey("retries") && Integer.parseInt(props.get("retries").toString()) > 0) {
                    KafkaWriter.logger.info((Object)("\"retries\" with value \"" + props.get("retries").toString() + "\" will be used by KafkaWriter internally and not by KafkaProducer in " + this.mode + " mode."));
                }
                if (props.containsKey("batch.size")) {
                    final int batchSize = Integer.parseInt(props.get("batch.size").toString());
                    if (batchSize > this.maxRequestSize) {
                        throw new RuntimeException("batch.size " + batchSize + " cannot be higher than " + "max.request.size" + " " + this.maxRequestSize + ". If batch.size is increased please make sure to increase " + "max.request.size" + " and max.message.bytes (topic level configuration)");
                    }
                }
            }
            this.mode = "BatchedSync";
        }
        if (KafkaWriter.logger.isInfoEnabled()) {
            KafkaWriter.logger.info((Object)("KafkaWriter will work in \"" + this.mode + "\" mode with e1p=" + this.e1p));
        }
        if (this.mode.equalsIgnoreCase("BatchedSync")) {
            if (props.containsKey("batch.size") && KafkaWriter.logger.isInfoEnabled()) {
                KafkaWriter.logger.info((Object)("batch.size - " + props.getProperty("batch.size") + " will be internally used by KafkaWriter and will not be reflected in Producer Configs."));
            }
            if (props.containsKey("linger.ms") && KafkaWriter.logger.isInfoEnabled()) {
                KafkaWriter.logger.info((Object)("linger.ms - " + props.getProperty("linger.ms") + " will be internally used by KafkaWriter and will not be reflected in Producer Configs."));
            }
        }
        if (props.containsKey("retries") && props.containsKey("retry.backoff.ms")) {
            final int retries = Integer.parseInt(props.get("retries").toString());
            final int retryBackoffMs = Integer.parseInt(props.get("retry.backoff.ms").toString());
            final int bias = 10;
            final long minRequestTimeoutMs = retries * retryBackoffMs + bias;
            if (minRequestTimeoutMs > requestTimeoutMs) {
                throw new RuntimeException("'request.timeout.ms' is too less compared to 'retry.timeout.ms'. It must be minimum of " + minRequestTimeoutMs + "ms. Please override this property via kafkaconfig or reduce values of 'retries' and 'retry.backoff.ms'.");
            }
        }
        else if (props.containsKey("retry.backoff.ms")) {
            final int retryBackoffMs2 = Integer.parseInt(props.get("retry.backoff.ms").toString());
            final float minRetries = requestTimeoutMs / retryBackoffMs2;
            if (minRetries <= 2.0f && KafkaWriter.logger.isInfoEnabled()) {
                KafkaWriter.logger.info((Object)"Number of retries is too less. Make sure brokers respond within this many retries or else KafkaProducer will throw a TimeoutException.");
            }
        }
        this.initializeProducer(props, localPropertyMap);
        this.isRunning = true;
    }
    
    protected abstract void initializeProducer(final Properties p0, final Map<String, Object> p1) throws Exception;
    
    @Override
    public Integer getPartitionId(final Event event) throws Exception {
        int partition = 0;
        if (this.eventLookup != null) {
            final List<Object> partitionList = (List<Object>)this.eventLookup.get(event);
            partition = this.partitioner.partition(this.topic, (Object)partitionList, (Object)event, this.noOfPartitions);
        }
        else {
            partition = this.partitioner.partition(this.topic, (Object)null, (Object)event, this.noOfPartitions);
        }
        return partition;
    }
    
    @Override
    public MonitorEvent.Type getSendCallsMonitorType() {
        return MonitorEvent.Type.TOTAL_KAFKA_SEND_CALLS;
    }
    
    public void receive(final int channel, final Event event, final ImmutableStemma pos) throws Exception {
        if (!this.isRunning || this.producer.swallowException()) {
            return;
        }
        try {
            if (this.isRunning) {
                this.bytesSent += this.producer.send(this.getPartitionId(event), event, pos);
            }
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw e;
            }
            if (e instanceof InterruptException) {
                throw new InterruptedException(e.getMessage());
            }
            if (this.isRunning) {
                KafkaWriter.logger.error((Object)"Problem while writing the event ", (Throwable)e);
                throw new ConnectionException("KafkaWriter is unable to produce events to " + this.topic, (Throwable)e);
            }
        }
    }
    
    static {
        KafkaWriter.logger = Logger.getLogger((Class)KafkaWriter.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}

