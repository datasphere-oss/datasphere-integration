package com.datasphere.recovery;

import java.io.*;
import java.sql.*;

public class PendingAppCheckpointSummary extends AbstractAppCheckpointSummary implements Serializable
{
    public long commandTimestamp;
    
    private PendingAppCheckpointSummary() {
    }
    
    public PendingAppCheckpointSummary(final String nodeUri, final String flowUri, final String componentUri, final String sourceUri, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final boolean atOrAfter, final long commandTimestamp) {
        super(nodeUri, flowUri, componentUri, sourceUri, lowSourcePosition, highSourcePosition, atOrAfter);
        this.commandTimestamp = commandTimestamp;
    }
    
    @Override
    public void prePersist() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    @Override
    public void preUpdate() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
}
