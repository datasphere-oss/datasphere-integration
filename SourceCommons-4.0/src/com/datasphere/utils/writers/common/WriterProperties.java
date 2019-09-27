package com.datasphere.utils.writers.common;

import org.apache.log4j.*;
import java.util.*;

public class WriterProperties
{
    private static Logger logger;
    private final long batchInterval;
    private final long batchCount;
    private final Map<String, String> sourceTargetMap;
    private Checkpointer checkpointerStrategy;
    private final BatchableWriter writerStrategy;
    private final PKUpdateHandlingMode pkUpdateHandlingMode;
    
    public WriterProperties(final long batchInterval, final long batchCount, final Map<String, String> sourceTargetMap, final Checkpointer checkpointerStrategy, final BatchableWriter writerStrategy, final PKUpdateHandlingMode pkUpdateMode) {
        if (batchInterval > 0L) {
            this.batchInterval = batchInterval;
        }
        else {
            WriterProperties.logger.warn((Object)"Batch interval cannot be less or equals to 0 disabling interval policy");
            this.batchInterval = 0L;
        }
        if (batchCount > 0L) {
            this.batchCount = batchCount;
        }
        else {
            WriterProperties.logger.warn((Object)"Batch count cannot be less or equals to 0 setting to default as 1000");
            this.batchCount = 1000L;
        }
        this.sourceTargetMap = sourceTargetMap;
        this.checkpointerStrategy = checkpointerStrategy;
        this.writerStrategy = writerStrategy;
        this.pkUpdateHandlingMode = pkUpdateMode;
    }
    
    public long getBatchInterval() {
        return this.batchInterval;
    }
    
    public long getBatchCount() {
        return this.batchCount;
    }
    
    public Map<String, String> getSourceTargetMap() {
        return this.sourceTargetMap;
    }
    
    public Checkpointer getCheckpointerStrategy() {
        return this.checkpointerStrategy;
    }
    
    public BatchableWriter getWriterStrategy() {
        return this.writerStrategy;
    }
    
    public PKUpdateHandlingMode getPKUpdateHandlingMode() {
        return this.pkUpdateHandlingMode;
    }
    
    @Override
    public String toString() {
        return "WriterProperties [batchInterval=" + this.batchInterval + ", batchCount=" + this.batchCount + ", sourceTargetMap=" + this.sourceTargetMap + ", checkpointerStrategy=" + this.checkpointerStrategy + ", writerStrategy=" + this.writerStrategy + ", pkUpdateHandlingMode=" + this.pkUpdateHandlingMode + "]";
    }
    
    static {
        WriterProperties.logger = Logger.getLogger((Class)WriterProperties.class);
    }
    
    public enum PKUpdateHandlingMode
    {
        ERROR, 
        IGNORE, 
        DELETEANDINSERT;
    }
}
