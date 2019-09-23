package com.datasphere.recovery;

import java.io.*;
import com.datasphere.uuid.*;
import java.sql.*;

public class CheckpointPath extends AbstractCheckpointPath implements Serializable
{
    private CheckpointPath() {
    }
    
    public CheckpointPath(final UUID flowUuid, final Path.ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final String atOrAfter) {
        super(flowUuid, pathItems, lowSourcePosition, highSourcePosition, atOrAfter);
    }
    
    public void prePersist() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    public void preUpdate() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
}
