package com.datasphere.kafkamessaging;

import org.apache.log4j.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;

public class KafkaPullerStatsTask implements Runnable
{
    private static final Logger logger;
    private final Pair<KafkaPullerTask, Future>[] consumerThreads;
    private final String kafkaReceiverName;
    int total_events_processed;
    private boolean showDetail;
    boolean isUserTriggered;
    
    public KafkaPullerStatsTask(final Pair<KafkaPullerTask, Future>[] consumerThreads, final String kafkaReceiverName) {
        this.total_events_processed = 0;
        this.showDetail = false;
        this.isUserTriggered = false;
        this.consumerThreads = consumerThreads;
        this.kafkaReceiverName = kafkaReceiverName;
    }
    
    @Override
    public void run() {
        if (!this.showDetail) {
            for (final Pair<KafkaPullerTask, Future> taskFuturePair : this.consumerThreads) {
                for (final Integer cc : taskFuturePair.first.emittedCountPerPartition) {
                    this.total_events_processed += cc;
                }
                if (KafkaPullerStatsTask.logger.isDebugEnabled()) {
                    KafkaPullerStatsTask.logger.debug((Object)("Fetch stats for : Avg: " + taskFuturePair.first.avg_fetch_size + ", Max: " + taskFuturePair.first.max_fetch_size + ", Min: " + taskFuturePair.first.min_fetch_size));
                }
            }
            if (KafkaPullerStatsTask.logger.isDebugEnabled()) {
                KafkaPullerStatsTask.logger.debug((Object)("Kafka puller for " + this.kafkaReceiverName + " consumed " + this.total_events_processed));
            }
            this.total_events_processed = 0;
        }
    }
    
    public void stop() {
        this.isUserTriggered = true;
    }
    
    static {
        logger = Logger.getLogger((Class)KafkaPullerStatsTask.class);
    }
}
