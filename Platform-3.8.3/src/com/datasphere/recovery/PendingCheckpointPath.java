package com.datasphere.recovery;

import java.io.*;
import com.datasphere.uuid.*;
import java.sql.*;

public class PendingCheckpointPath extends AbstractCheckpointPath implements Serializable
{
    public long commandTimestamp;
    
    private PendingCheckpointPath() {
    }
    
    public PendingCheckpointPath(final UUID flowUuid, final Path.ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final String atOrAfter, final long commandTimestamp) {
        super(flowUuid, pathItems, lowSourcePosition, highSourcePosition, atOrAfter);
        this.commandTimestamp = commandTimestamp;
    }
    
    public void prePersist() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    public void preUpdate() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
}
