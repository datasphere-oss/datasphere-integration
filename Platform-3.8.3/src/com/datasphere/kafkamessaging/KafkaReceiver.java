package com.datasphere.kafkamessaging;

import org.apache.log4j.*;

import com.datasphere.runtime.meta.*;
import com.datasphere.messaging.*;
import java.util.*;

import com.datasphere.jmqmessaging.*;
import com.datasphere.kafka.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;
import com.datasphere.recovery.*;

public class KafkaReceiver implements Receiver
{
    private static Logger logger;
    protected final KafkaReceiverInfo info;
    protected MetaInfo.Stream streamInfo;
    protected final String name;
    protected final boolean isEncrypted;
    Map<Object, Object> properties;
    KafkaPuller kafkaPuller;
    ExecutorService executorService;
    Future future;
    
    public KafkaReceiver(final KafkaReceiverInfo info, final MetaInfo.Stream streamInfo, final Handler rcvr, final boolean encrypted) {
        this.info = info;
        this.name = info.getRcvr_name();
        this.isEncrypted = encrypted;
        this.streamInfo = streamInfo;
        this.kafkaPuller = new KafkaPuller(streamInfo, info, rcvr);
        if (KafkaReceiver.logger.isInfoEnabled()) {
            KafkaReceiver.logger.info((Object)("Created KafkaReceiver info.name=" + info.getName()));
        }
    }
    
    @Override
    public void start(final Map<Object, Object> properties) throws Exception {
        this.properties = properties;
        final List<Integer> partitions = (List<Integer>)this.properties.get("partitions_i_own");
        if (partitions.isEmpty()) {
            throw new KafkaException("Cannot start Kafka Puller for " + this.info.getTopic_name() + " with zero partitions");
        }
        List<String> kafkaBrokers = (List<String>)properties.get(KafkaConstants.broker_address_list);
        if (kafkaBrokers == null || kafkaBrokers.isEmpty()) {
            kafkaBrokers = KafkaStreamUtils.getBrokerAddress(this.streamInfo);
        }
        try {
            this.kafkaPuller.init(partitions, kafkaBrokers);
            if (KafkaReceiver.logger.isInfoEnabled()) {
                KafkaReceiver.logger.info((Object)("Initializing Kafka Puller for " + this.info.getRcvr_name()));
            }
        }
        catch (Exception e) {
            KafkaReceiver.logger.warn((Object)("Failed to initialize Kafka Puller " + this.info.getRcvr_name() + ". Reason -> " + e.getMessage()), (Throwable)e);
            this.kafkaPuller.doCleanUp();
            KafkaReceiver.logger.error((Object)"Could not start Kafka receiver", (Throwable)e);
            throw new KafkaException("Could not start Kafka receiver", (Throwable)e);
        }
    }
    
    @Override
    public boolean stop() throws InterruptedException {
        this.kafkaPuller.stop();
        this.future.cancel(true);
        this.executorService.shutdown();
        this.kafkaPuller.doCleanUp();
        if (!this.executorService.awaitTermination(100L, TimeUnit.MILLISECONDS)) {
            if (KafkaReceiver.logger.isInfoEnabled()) {
                KafkaReceiver.logger.info((Object)"Waiting for 2 seconds to shutdown executor service fro Kafka Puller");
            }
            this.executorService.shutdownNow();
        }
        if (KafkaReceiver.logger.isInfoEnabled()) {
            KafkaReceiver.logger.info((Object)("Stopped Kafka Receiver : " + this.name));
        }
        return false;
    }
    
    public void setPosition(final PartitionedSourcePosition position) {
        this.kafkaPuller.setStartPosition(position);
    }
    
    public void startForEmitting() throws Exception {
        this.executorService = Executors.newSingleThreadExecutor();
        this.future = this.executorService.submit(this.kafkaPuller);
    }
    
    public Position getComponentCheckpoint() {
        return this.kafkaPuller.getComponentCheckpoint();
    }
    
    static {
        KafkaReceiver.logger = Logger.getLogger((Class)KafkaReceiver.class);
    }
}
