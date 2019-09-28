package com.datasphere.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.datasphere.common.exc.MetadataUnavailableException;
import com.datasphere.kafka.KafkaLongOffset;
import com.datasphere.kafka.Offset;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.persistence.DefaultJPAPersistenceLayerImpl;
import com.datasphere.persistence.KWCheckpointPersistenceLayer;
import com.datasphere.security.HDSecurityManager;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.target.kafka.AsyncProducer;
import com.datasphere.target.kafka.BatchedSyncProducer;
import com.datasphere.target.kafka.KafkaWriter;
import com.datasphere.target.kafka.Producer;
import com.datasphere.uuid.UUID;

public abstract class KafkaV10WriterImpl extends KafkaWriter
{
    private static Logger logger;
    private static Properties attachProp = new Properties();//附加属性
//    private String saslUsername = null;
//    private String saslPassword = null;
    private KafkaCheckpointPersistence persistCheckpt;
    
    public KafkaV10WriterImpl() {
        this.persistCheckpt = null;
    }
    
    public abstract int getVersion();
    
    public void initializeProducer(final Properties prop, final Map<String, Object> localPropertyMap) throws Exception {
        final Properties props = new Properties();
        props.putAll(prop);
        String brokerList = this.kafkaProp.getBrokerAddress();
        props.put("bootstrap.servers", brokerList);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("client.id", new UUID(System.currentTimeMillis()) + "_Consumer_MetadataChecker_" + this.topic);
        

        String confPath = System.getProperty("user.dir") + "/kafka.properties";
        if(confPath.length()>0) {
        		readProperties(confPath);
        		if(attachProp != null) {
        			Enumeration<?> enu = attachProp.propertyNames();
        			if(enu!=null) {
        				while (enu.hasMoreElements()) {
        					String key = (String)enu.nextElement();
        					String value = attachProp.getProperty(key);
//	    					if("saslUsername".equalsIgnoreCase(key)) {
//	    						saslUsername = value;
//	    					}else if("saslPassword".equalsIgnoreCase(key)) {
//	    						saslPassword = value;
//	    					}else {
	    					props.put(key, value);
//	    					}
        				}
        			}
        		}
        }
        
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        boolean foundTopic = false;
        if (!brokerList.endsWith(",")) {
            brokerList += ",";
        }
        long retryBackoffms = 1000L;
        Offset[] latestOffsetOfAllPartitions = null;
        Offset[] startOffsetOfAllPartitions = null;
        List<TopicPartition> topicPartitions = null;
        retryBackoffms = Long.parseLong(props.getProperty("retry.backoff.ms", "1000"));
        for (int i = 0; i < 10 && !foundTopic; ++i) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(this.topic);
            if (partitionInfos != null && !partitionInfos.isEmpty()) {
                if (this.e1p) {
                    latestOffsetOfAllPartitions = new Offset[partitionInfos.size()];
                    startOffsetOfAllPartitions = new Offset[partitionInfos.size()];
                    topicPartitions = new ArrayList<TopicPartition>(partitionInfos.size());
                }
                foundTopic = true;
                for (final PartitionInfo partitionInfo : partitionInfos) {
                    if (this.e1p) {
                        final TopicPartition tp = new TopicPartition(this.topic, partitionInfo.partition());
                        topicPartitions.add(tp);
                    }
                    ++this.noOfPartitions;
                    final Node[] inSyncReplicas = partitionInfo.inSyncReplicas();
                    for (int j = 0; j < inSyncReplicas.length; ++j) {
                        final String brokerAdd = inSyncReplicas[j].host() + ":" + inSyncReplicas[j].port();
                        if (!brokerList.contains(brokerAdd)) {
                            brokerList = brokerList + brokerAdd + ",";
                        }
                    }
                }
                if (brokerList.endsWith(",")) {
                    brokerList = brokerList.substring(0, brokerList.lastIndexOf(44));
                }
                if (KafkaV10WriterImpl.logger.isInfoEnabled()) {
                    KafkaV10WriterImpl.logger.info((Object)("Topic name - " + this.topic + " is valid."));
                }
                final long start = System.currentTimeMillis();
                if (this.e1p) {
                    consumer.assign(topicPartitions);
                    consumer.seekToEnd(new ArrayList<TopicPartition>());
                    for (final TopicPartition tp2 : topicPartitions) {
                        latestOffsetOfAllPartitions[tp2.partition()] = (Offset)new KafkaLongOffset(consumer.position(tp2) - 1L);
                        if (KafkaV10WriterImpl.logger.isInfoEnabled()) {
                            KafkaV10WriterImpl.logger.info((Object)("Latest offset of " + this.topic + "-" + tp2.partition() + " is " + latestOffsetOfAllPartitions[tp2.partition()]));
                        }
                    }
                    consumer.seekToBeginning(new ArrayList<TopicPartition>());
                    for (final TopicPartition tp2 : topicPartitions) {
                        if ((long)latestOffsetOfAllPartitions[tp2.partition()].getOffset() >= 0L) {
                            startOffsetOfAllPartitions[tp2.partition()] = (Offset)new KafkaLongOffset(consumer.position(tp2));
                            if (!KafkaV10WriterImpl.logger.isInfoEnabled()) {
                                continue;
                            }
                            KafkaV10WriterImpl.logger.info((Object)("Start offset of " + this.topic + "-" + tp2.partition() + " is " + startOffsetOfAllPartitions[tp2.partition()]));
                        }
                    }
                }
                if (KafkaV10WriterImpl.logger.isInfoEnabled()) {
                    KafkaV10WriterImpl.logger.info((Object)("Took " + (System.currentTimeMillis() - start) + " ms to find initial and latest offset for " + this.noOfPartitions + " partitions."));
                }
            }
            if (!foundTopic) {
                Thread.sleep(retryBackoffms);
            }
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if (!foundTopic) {
            throw new MetadataUnavailableException("Failure in getting metadata for Kafka Topic \"" + this.topic + "\". Please check if the topic (" + this.topic + ") and broker address (" + this.kafkaProp.getBrokerAddress() + ") is valid");
        }
        props.remove("client.id");
        props.remove("key.deserializer");
        props.remove("value.deserializer");
        props.remove("bootstrap.servers");
        if (!props.containsKey("retry.backoff.ms")) {
            props.put("retry.backoff.ms", "5000");
        }
        if (!props.containsKey("reconnect.backoff.ms")) {
            props.put("reconnect.backoff.ms", "5000");
        }
        final String acks = props.getProperty("acks", "1");
        props.put("acks", acks);
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        String fullyQualifiedComponentName;
        try {
            fullyQualifiedComponentName = MetadataRepository.getINSTANCE().getMetaObjectByUUID(super.uuid, HDSecurityManager.TOKEN).getFQN();
        }
        catch (MetaDataRepositoryException e) {
            throw new RuntimeException("Problem while getting Target Info from metadata repository " + e);
        }
        props.put("client.id", fullyQualifiedComponentName);
        this.producer = Producer.getProducer(props, super.mode);
        KWCheckpointPersistenceLayer defaultJPAPersistenceLayerImpl = null;
        Kafka10ClientImpl[] client;
        if (this.producer instanceof BatchedSyncProducer) {
            client = new Kafka10ClientImpl[this.noOfPartitions];
            for (int k = 0; k < this.noOfPartitions; ++k) {
                props.put("client.id", fullyQualifiedComponentName + "_" + this.topic + "_" + k + "_" + System.currentTimeMillis());
                client[k] = new Kafka10ClientImpl(props);
            }
            if (this.isNativeFormatter || this.isBinaryFormatterWithSchemaRegistry) {
                ((BatchedSyncProducer)this.producer).setBaseUri((String)localPropertyMap.get("schemaregistryurl"), Constant.FormatOptions.valueOf(((BaseFormatter)this.formatter).getFormatterProperties().get("formatAs").toString()));
            }
            if (this.e1p) {
                defaultJPAPersistenceLayerImpl = super.initPersistentLayer();
            }
            ((BatchedSyncProducer)this.producer).setE1P(this.e1p, defaultJPAPersistenceLayerImpl, super.uuid, latestOffsetOfAllPartitions, startOffsetOfAllPartitions);
        }
        else {
            client = new Kafka10ClientImpl[] { new Kafka10ClientImpl(props) };
            if (this.producer instanceof AsyncProducer && (this.isNativeFormatter || this.isBinaryFormatterWithSchemaRegistry)) {
                ((AsyncProducer)this.producer).setBaseUri((String)localPropertyMap.get("schemaregistryurl"), Constant.FormatOptions.valueOf(((BaseFormatter)this.formatter).getFormatterProperties().get("formatAs").toString()));
            }
        }
        this.producer.initiateProducer(this.topic, client, this.receiptCallback, this.formatter, this.isBinaryFormatter);
        if (this.e1p) {
            this.persistCheckpt = new KafkaCheckpointPersistence(this.getVersion(), localPropertyMap, (DefaultJPAPersistenceLayerImpl)defaultJPAPersistenceLayerImpl, ((BatchedSyncProducer)this.producer).getWaitPosition(), this.receiptCallback);
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (this.persistCheckpt != null) {
            this.persistCheckpt.close();
        }
    }
    

    /**
     * 读取配置文件
     * @param fileName
     */
    public static void readProperties(String fileName){
        try {
        		File file = new File(fileName);
        		InputStream in = new FileInputStream(file);
            //InputStream in = PropertiesUtil.class.getResourceAsStream(fileName);
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            attachProp.load(bf);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    
    static {
        KafkaV10WriterImpl.logger = Logger.getLogger((Class)KafkaV10WriterImpl.class);
    }
    
    public static void main(String[] args) {
    		String filePath = System.getProperty("user.dir") + "/kafka.properties";
        if(filePath.length()>0) {
        		readProperties(filePath);
        		if(attachProp != null) {
        			Enumeration<?> enu = attachProp.propertyNames();
        			if(enu!=null) {
        				while (enu.hasMoreElements()) {
        					String key = (String)enu.nextElement();
        					String value = attachProp.getProperty(key);
        					System.out.println(""+key + "="+ value);
        				}
        			}
        		}
        }
        
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}

