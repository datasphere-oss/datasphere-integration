package com.datasphere.utils.writers.common;

import org.apache.log4j.*;

import com.datasphere.recovery.*;

public class SimpleCheckpointer implements Checkpointer
{
    private static Logger logger;
    private PathManager checkpointingValue;
    
    public SimpleCheckpointer() {
        this.checkpointingValue = new PathManager();
    }
    
    @Override
    public void updatePosition(final Position pos) {
        this.checkpointingValue.mergeHigherPositions(pos);
    }
    
    @Override
    public Position getAckPosition() {
        return this.checkpointingValue.toPosition();
    }
    
    static {
        SimpleCheckpointer.logger = Logger.getLogger((Class)SimpleCheckpointer.class);
    }
}
