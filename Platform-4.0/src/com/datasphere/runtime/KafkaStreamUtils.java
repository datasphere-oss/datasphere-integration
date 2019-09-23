package com.datasphere.runtime;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.datasphere.kafka.KafkaException;
import com.datasphere.kafka.KafkaUtilsIntf;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;

public class KafkaStreamUtils implements Serializable
{
    private static final long serialVersionUID = 6880318751354053803L;
    private static Logger logger;
    public static final MDRepository md_repository;
    static long createTopicTimeoutMs;
    static long deleteTopicTimeoutMs;
    
    public static void validatePropertySet(final MetaInfo.PropertySet kpset) throws KafkaException {
        final String pConfig = (String)kpset.getProperties().get("producerconfig");
        Map<String, String> pConfigMap = null;
        if (pConfig != null) {
            pConfigMap = new HashMap<String, String>();
            final String[] split;
            final String[] pKVConfig = split = pConfig.split(",");
            for (final String ss : split) {
                final String[] kv = ss.split(":");
                if (kv[0] != null && kv[1] != null) {
                    pConfigMap.put(kv[0], kv[1]);
                }
            }
        }
        final String cConfig = (String)kpset.getProperties().get("consumerconfig");
        Map<String, String> cConfigMap = null;
        if (cConfig != null) {
            cConfigMap = new HashMap<String, String>();
            final String[] split2;
            final String[] cKVConfig = split2 = cConfig.split(",");
            for (final String ss2 : split2) {
                final String[] kv2 = ss2.split(":");
                if (kv2[0] != null && kv2[1] != null) {
                    cConfigMap.put(kv2[0], kv2[1]);
                }
            }
        }
        final String sConfig = (String)kpset.getProperties().get("securityconfig");
        Map<String, String> sConfigMap = null;
        if (sConfig != null) {
            sConfigMap = new HashMap<String, String>();
            final String[] split3;
            final String[] sKVConfig = split3 = sConfig.split(",");
            for (final String ss3 : split3) {
                final String[] kv3 = ss3.split(":");
                if (kv3[0] != null && kv3[1] != null) {
                    System.out.println(kv3[0] + " : " + kv3[1]);
                    sConfigMap.put(kv3[0], kv3[1]);
                }
            }
        }
        try {
            final KafkaUtilsIntf kafkaUtils = loadKafkaUtilsClass(kpset);
            kafkaUtils.validateProperties((Map)pConfigMap, (Map)cConfigMap, (Map)sConfigMap);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static MetaInfo.PropertySet getPropertySet(final MetaInfo.Stream streamInfo) {
        String namespace;
        String name;
        if (streamInfo.pset.indexOf(".") == -1) {
            namespace = streamInfo.getNsName();
            name = streamInfo.pset;
        }
        else {
            namespace = streamInfo.pset.split("\\.")[0];
            name = streamInfo.pset.split("\\.")[1];
        }
        try {
            final MetaInfo.PropertySet kakfka_props = (MetaInfo.PropertySet)KafkaStreamUtils.md_repository.getMetaObjectByName(EntityType.PROPERTYSET, namespace, name, null, HSecurityManager.TOKEN);
            return streamInfo.propertySet = kakfka_props;
        }
        catch (MetaDataRepositoryException e) {
            Logger.getLogger("KafkaStreams").error((Object)("Unable to find Property Set for stream " + streamInfo.name + ", not expected to happen"));
            return null;
        }
    }
    
    public static String getZkAddress(final MetaInfo.PropertySet propertySet) {
        final String zk_address_list = (String)propertySet.getProperties().get("zk.address");
        return zk_address_list;
    }
    
    public static int getNumReplicas(final MetaInfo.PropertySet propertySet) {
        final String zk_num_replicas = (String)propertySet.getProperties().get("replication.factor");
        if (zk_num_replicas == null) {
            return Integer.parseInt("1");
        }
        return Integer.parseInt(zk_num_replicas);
    }
    
    public static List<String> getBrokerAddress(final MetaInfo.Stream streamObject) {
        final MetaInfo.PropertySet kafka_props = getPropertySet(streamObject);
        final String[] broker_list = ((String)kafka_props.getProperties().get("bootstrap.brokers")).split(",");
        final List<String> broker_address_list = Arrays.asList(broker_list);
        return broker_address_list;
    }
    
    public static boolean createTopic(final MetaInfo.Stream streamObject) throws Exception {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            final MetaInfo.PropertySet kafkaPropset = getPropertySet(streamObject);
            final KafkaUtilsIntf kafkaUtils = loadKafkaUtilsClass(streamObject);
            Thread.currentThread().setContextClassLoader(kafkaUtils.getClass().getClassLoader());
            final String zkServers = getZkAddress(kafkaPropset);
            final String dataTopicName = createTopicName(streamObject);
            final String checkpointTopicName = getCheckpointTopicName(dataTopicName);
            final int numPartitions = getPartitionsCount(streamObject, kafkaPropset);
            final int numReplicas = getNumReplicas(kafkaPropset);
            kafkaUtils.createTopic(zkServers, dataTopicName, numPartitions, numReplicas, new Properties(), KafkaStreamUtils.createTopicTimeoutMs);
            final Properties checkpointTopicProps = new Properties();
            checkpointTopicProps.put("cleanup.policy", "compact");
            kafkaUtils.createTopic(zkServers, checkpointTopicName, numPartitions, numReplicas, checkpointTopicProps, KafkaStreamUtils.createTopicTimeoutMs);
            return true;
        }
        catch (Exception e) {
            throw new KafkaException((Throwable)e);
        }
        finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }
    
    public static KafkaUtilsIntf loadKafkaUtilsClass(final MetaInfo.Stream streamMetaObject) throws KafkaException {
        final MetaInfo.PropertySet kafkaPropset = getPropertySet(streamMetaObject);
        return loadKafkaUtilsClass(kafkaPropset);
    }
    
    public static KafkaUtilsIntf loadKafkaUtilsClass(final MetaInfo.PropertySet kafkaPropset) throws KafkaException {
        final Map<String, Object> pMap = kafkaPropset.getProperties();
        final TreeMap<String, Object> tMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        tMap.putAll(pMap);
        double version = 0.8;
            if (tMap.containsKey("kafkaversion")) {
                try {
                    version = Double.parseDouble((String)kafkaPropset.getProperties().get("kafkaversion"));
                }
                catch (Exception e2) {
                    throw new KafkaException("Unexpected value for kafkaversion. Valid values are 0.8 or 0.9 or 0.10");
                }
            }
            
            try {
                Class kafkaUtilsClass;
                if (version == 0.8) {
                    kafkaUtilsClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.KafkaUtils8");
                }
                else if (version == 0.9) {
                    kafkaUtilsClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.KafkaUtils9");
                }
                else {
                    if (version != 0.1) {
                        throw new KafkaException("Unexpected value for kafkaversion. Valid values are 0.8 or 0.9 or 0.10");
                    }
                    kafkaUtilsClass = ClassLoader.getSystemClassLoader().loadClass("com.datasphere.KafkaUtils10");
                }
                return (KafkaUtilsIntf)kafkaUtilsClass.getDeclaredConstructor((Class<?>[])new Class[0]).newInstance(new Object[0]);
            }
            catch (Exception e) {
                throw new KafkaException("Could not load Kafka utils class", (Throwable)e);
            }
    }
    
    public static int getPartitionsCount(final MetaInfo.Stream streamInfo, final MetaInfo.PropertySet streamPropset) {
        if (streamInfo.partitioningFields == null || streamInfo.partitioningFields.isEmpty()) {
            return 1;
        }
        try {
            if (streamPropset.getProperties().containsKey("partitions")) {
                final int userRequest = Integer.parseInt((String)(streamPropset.getProperties().get("partitions")));
                if (userRequest > 0) {
                    return userRequest;
                }
            }
        }
        catch (Exception e) {
            KafkaStreamUtils.logger.warn((Object)"Unexpected value for partitions, will use default value: 200");
        }
        return 200;
    }
    
    public static int getBatchSize(final MetaInfo.Stream streamInfo) {
        try {
            final MetaInfo.PropertySet pset = getPropertySet(streamInfo);
            if (pset.getProperties().containsKey("batch.size")) {
                final int userRequest = Integer.parseInt((String)(pset.getProperties().get("batch.size")));
                if (userRequest > 0) {
                    return userRequest;
                }
            }
        }
        catch (Exception e) {
            KafkaStreamUtils.logger.warn((Object)"Unexpected value for batch.size, will use default value : 1572864");
        }
        return 1572864;
    }
    
    public static int getLingerTime(final MetaInfo.Stream streamInfo) {
        try {
            final MetaInfo.PropertySet pset = getPropertySet(streamInfo);
            if (pset.getProperties().containsKey("linger.ms")) {
                final int userRequest = Integer.parseInt((String)(pset.getProperties().get("linger.ms")));
                if (userRequest > 0) {
                    return userRequest;
                }
            }
        }
        catch (Exception e) {
            KafkaStreamUtils.logger.warn((Object)"Unexpected value for linger.ms, will use default value : 200");
        }
        return 200;
    }
    
    public static Map<String, String> getSecurityProperties(final MetaInfo.PropertySet propertySet) {
        final String sConfig = (String)propertySet.getProperties().get("securityconfig");
        Map<String, String> sConfigMap = null;
        if (sConfig != null) {
            sConfigMap = new HashMap<String, String>();
            final String[] split;
            final String[] sKVConfig = split = sConfig.split(",");
            for (final String ss : split) {
                final String[] kv = ss.split(":");
                if (kv[0] != null && kv[1] != null) {
                    sConfigMap.put(kv[0], kv[1]);
                }
            }
        }
        return sConfigMap;
    }
    
    public static String createTopicName(final MetaInfo.Stream streamObject) {
        final StringBuilder topic_name_builder = new StringBuilder();
        topic_name_builder.append(streamObject.getNsName()).append(":").append(streamObject.getName());
        final String topic_name = topic_name_builder.toString();
        return makeSafeKafkaTopicName(topic_name);
    }
    
    public static String makeSafeKafkaTopicName(final String unsafeTopicName) {
        final String result = unsafeTopicName.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return result;
    }
    
    public static boolean deleteTopic(final MetaInfo.Stream streamInfo) throws MetaDataRepositoryException, KafkaException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            final String dataTopicName = createTopicName(streamInfo);
            final String checkpointTopicName = getCheckpointTopicName(dataTopicName);
            final MetaInfo.PropertySet kafkaPropset = getPropertySet(streamInfo);
            if (kafkaPropset != null) {
                final KafkaUtilsIntf kafkaUtils = loadKafkaUtilsClass(streamInfo);
                Thread.currentThread().setContextClassLoader(kafkaUtils.getClass().getClassLoader());
                final String zkServers = getZkAddress(kafkaPropset);
                kafkaUtils.deleteTopic(zkServers, dataTopicName, KafkaStreamUtils.deleteTopicTimeoutMs);
                kafkaUtils.deleteTopic(zkServers, checkpointTopicName, KafkaStreamUtils.deleteTopicTimeoutMs);
                return true;
            }
            throw new KafkaException("Property set : " + streamInfo.pset + " is not available in metadata repository.");
        }
        catch (Exception e) {
            throw new KafkaException((Throwable)e);
        }
        finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }
    
    public static CreateTopic getCreateTopicExecutor(final MetaInfo.Stream streamObject) {
        return new CreateTopic(streamObject);
    }
    
    public static DeleteTopic getDeleteTopicExecutor(final MetaInfo.Stream metaObject) {
        return new DeleteTopic(metaObject);
    }
    
    public static String getCheckpointTopicName(final String kafkaTopicName) {
        return kafkaTopicName + "_CHECKPOINT";
    }
    
    public static String getKafkaStreamLockName(final String kafkaTopicName, final int partitionId) {
        return "KafkaStreamLock_" + kafkaTopicName + "_" + partitionId;
    }
    
    public static boolean retryBackOffMillis(final long milliseconds) {
        try {
            Thread.sleep(milliseconds);
            return true;
        }
        catch (InterruptedException e) {
            Logger.getLogger("KafkaStreams").warn((Object)"Kafka thread interrupted while sleeping", (Throwable)e);
            e.printStackTrace();
            return false;
        }
    }
    
    static {
        KafkaStreamUtils.logger = Logger.getLogger((Class)KafkaStreamUtils.class);
        md_repository = MetadataRepository.getINSTANCE();
        KafkaStreamUtils.createTopicTimeoutMs = 2000L;
        KafkaStreamUtils.deleteTopicTimeoutMs = 2000L;
    }
    
    public static class CreateTopic implements RemoteCall
    {
        private static final long serialVersionUID = 1164757976316951507L;
        public MetaInfo.Stream streamObject;
        
        public CreateTopic(final MetaInfo.Stream streamObject) {
            this.streamObject = streamObject;
        }
        
        @Override
        public Object call() throws Exception {
            return KafkaStreamUtils.createTopic(this.streamObject);
        }
    }
    
    public static class DeleteTopic implements RemoteCall
    {
        private static final long serialVersionUID = -5529822347005672810L;
        public MetaInfo.Stream streamObject;
        
        public DeleteTopic(final MetaInfo.Stream streamObject) {
            this.streamObject = streamObject;
        }
        
        @Override
        public Object call() throws Exception {
            return KafkaStreamUtils.deleteTopic(this.streamObject);
        }
    }
}
