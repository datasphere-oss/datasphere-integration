package com.datasphere.source.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.datasphere.source.lib.prop.Property;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaPartitionHandler_8 extends KafkaPartitionHandler
{
    public PartitionMetadata partitionMetadata;
    public List<String> replicaBrokers;
    public SimpleConsumer consumer;
    public long kafkaReadOffset;
    private int numOfErrors;
    private int leaderLookUp;
    private int so_timeout;
    private static final Logger logger;
    
    public KafkaPartitionHandler_8() {
        this.kafkaReadOffset = 0L;
        this.numOfErrors = 0;
        this.leaderLookUp = 0;
        this.so_timeout = 0;
    }
    
    public void init(final PartitionMetadata parMetadata, final KafkaProperty prop) throws Exception {
        super.init((Property)prop);
        this.partitionMetadata = parMetadata;
        this.partitionId = this.partitionMetadata.partitionId();
        this.leaderIp = this.partitionMetadata.leader().host();
        this.leaderPort = this.partitionMetadata.leader().port();
        this.so_timeout = prop.getMap().get("so_timeout") == null? 0 : Integer.parseInt(prop.getMap().get("so_timeout").toString());
        if (prop.propMap.containsKey("KafkaReadOffset")) {
            this.kafkaReadOffset = prop.propMap.get("KafkaReadOffset") == null? 0L : Long.parseLong(prop.propMap.get("KafkaReadOffset").toString());
        }
        this.replicaBrokers = new ArrayList<String>();
        for (final Broker replica : this.partitionMetadata.replicas()) {
            final String bokerid = replica.host() + ":" + replica.port();
            this.replicaBrokers.add(bokerid);
        }
        this.clientName = "Client_" + this.topic + "_" + this.partitionId;
        this.consumer = new SimpleConsumer(this.partitionMetadata.leader().host(), this.partitionMetadata.leader().port(), this.so_timeout, this.blocksize, this.clientName);
    }
    
    public void initializeConsumer() {
        this.consumer = new SimpleConsumer(this.partitionMetadata.leader().host(), this.partitionMetadata.leader().port(), this.so_timeout, this.blocksize, this.clientName);
    }
    
    public void updatePartitionMetadata(final PartitionMetadata pm, final long offset) {
        this.partitionMetadata = pm;
        this.partitionId = pm.partitionId();
        this.leaderIp = pm.leader().host();
        this.leaderPort = pm.leader().port();
        this.kafkaReadOffset = offset;
        this.numOfErrors = 0;
        this.replicaBrokers.clear();
        this.leaderLookUp = 0;
        for (final Broker replica : this.partitionMetadata.replicas()) {
            final String bokerid = replica.host() + ":" + replica.port();
            this.replicaBrokers.add(bokerid);
        }
        if (this.consumer != null) {
            this.consumer.close();
            this.consumer = null;
        }
        this.consumer = new SimpleConsumer(pm.leader().host(), pm.leader().port(), this.so_timeout, this.blocksize, this.clientName);
    }
    
    public PartitionMetadata getPartitionMetadata() {
        return this.partitionMetadata;
    }
    
    public long getParitionReadOffset() {
        return this.kafkaReadOffset;
    }
    
    public void increaseLeaderLookUpCount(final int count) {
        this.leaderLookUp += count;
    }
    
    public int getLeaderLookUpCount() {
        return this.leaderLookUp;
    }
    
    public int noOfErrors() {
        return this.numOfErrors;
    }
    
    public void increaseErrorCount(final int errorCount) {
        this.numOfErrors += errorCount;
    }
    
    public void cleanUp() throws Exception {
        super.cleanUp();
        if (this.consumer != null) {
            this.consumer.close();
            this.consumer = null;
        }
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaPartitionHandler_8.class);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
