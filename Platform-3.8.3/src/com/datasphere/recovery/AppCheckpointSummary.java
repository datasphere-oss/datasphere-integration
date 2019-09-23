package com.datasphere.recovery;

import java.io.*;
import org.apache.commons.lang.builder.*;
import java.sql.*;

public class AppCheckpointSummary extends AbstractAppCheckpointSummary implements Serializable
{
    private AppCheckpointSummary() {
    }
    
    private AppCheckpointSummary(final String nodeUri, final String flowUri, final String componentUri, final String sourceUri, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final boolean atOrAfter) {
        super(nodeUri, flowUri, componentUri, sourceUri, lowSourcePosition, highSourcePosition, atOrAfter);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.toString()).toHashCode();
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
