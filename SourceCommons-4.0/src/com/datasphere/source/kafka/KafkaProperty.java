package com.datasphere.source.kafka;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.type.*;

import org.apache.log4j.*;
import java.util.*;

public class KafkaProperty extends Property
{
    public static final String PARTITIONIDLIST = "PartitionIDList";
    public static final String STARTOFFSET = "startOffset";
    public static final String STARTTIMESTAMP = "startTimestamp";
    public static final String KAFKABROKER_ADDRESS = "brokerAddress";
    public static final String KAFKACONFIG = "KafkaConfig";
    public positiontype posType;
    public int positionValue;
    public String[] kafkaBrokerAddress;
    public String partitionList;
    private String brokerAddressList;
    private String[] kafkaBrokerConfigList;
    private static final Logger logger;
    
    @Override
    public Map<String, Object> getMap() {
        return (Map<String, Object>)super.propMap;
    }
    
    public KafkaProperty(final Map<String, Object> mp) {
        super(mp);
        this.posType = positiontype.WA_POSITION_EOF;
        this.partitionList = null;
        this.brokerAddressList = null;
        this.kafkaBrokerConfigList = null;
        if (this.propMap.get("brokerAddress") != null) {
            this.setBrokerAddress(this.removeLineBreaksAndSpace((String)this.propMap.get("brokerAddress")));
            this.kafkaBrokerAddress = this.brokerAddressList.split(",");
        }
        if (this.propMap.get("PartitionIDList") != null) {
            this.partitionList = (String)this.propMap.get("PartitionIDList");
        }
        else {
            this.partitionList = System.getProperty("com.datasphere.kafkareader.partitionlist");
        }
        this.partitionList = this.removeLineBreaksAndSpace(this.partitionList);
        if (this.propMap.get("startOffset") != null) {
            this.positionValue = Integer.parseInt(this.propMap.get("startOffset").toString());
            if (this.propMap.get("startTimestamp") != null && this.positionValue >= 0) {
                throw new RuntimeException("Position by startOffset (" + this.positionValue + ") and position by timestamp (" + this.propMap.get("startTimestamp") + ") can not be supported at the same time. Please specify anyone.");
            }
            if (this.positionValue == -1) {
                this.posType = positiontype.WA_POSITION_EOF;
            }
            else if (this.positionValue == 0) {
                this.posType = positiontype.WA_POSITION_SOF;
            }
            else if (this.positionValue > 0) {
                this.posType = positiontype.WA_POSITION_OFFSET;
            }
            else if (this.positionValue < -1) {
                throw new RuntimeException("Invalid start offset value " + this.positionValue + ". Supported values are 0(beginning of all partition(s)), -1 (End of all partition(s)), Valid Kafka offset (supported only for single partition setup).");
            }
        }
        if (this.propMap.get("startTimestamp") != null) {
            this.posType = positiontype.WA_POSITION_TIMESTAMP;
        }
        if (this.propMap.get("KafkaConfig") != null) {
            String properties = (String)this.propMap.get("KafkaConfig");
            properties = this.removeLineBreaksAndSpace(properties);
            this.setKafkaBrokerConfigList(properties.split(";"));
        }
    }
    
    public String getBrokerAddress() {
        return this.brokerAddressList;
    }
    
    public void setBrokerAddress(final String brokerAddressList) {
        this.brokerAddressList = brokerAddressList;
    }
    
    public String[] getKafkaBrokerConfigList() {
        return this.kafkaBrokerConfigList;
    }
    
    public void setKafkaBrokerConfigList(final String[] kafkaBrokerConfigList) {
        if (kafkaBrokerConfigList != null) {
            this.kafkaBrokerConfigList = Arrays.copyOf(kafkaBrokerConfigList, kafkaBrokerConfigList.length);
        }
    }
    
    public String removeLineBreaksAndSpace(String property) {
        if (property != null && !property.isEmpty()) {
            property = property.replace("\n", "").replace("\r", "").replace("\t", "").trim();
        }
        return property;
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaProperty.class);
    }
}
