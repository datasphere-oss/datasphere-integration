package com.datasphere.recovery;

import com.fasterxml.jackson.annotation.*;
import java.util.*;
import java.io.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public class PartitionedSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = 5411945471054888685L;
    public static final String NULL_PARTITION_KEY = "*";
    private final Map<String, SourcePosition> sourcePositions;
    private final Map<String, Boolean> atOrAfters;
    
    public PartitionedSourcePosition() {
        this.sourcePositions = new HashMap<String, SourcePosition>();
        this.atOrAfters = new HashMap<String, Boolean>();
    }
    
    public PartitionedSourcePosition(final Map<String, SourcePosition> sourcePositions, final Map<String, Boolean> atOrAfters) {
        this.sourcePositions = sourcePositions;
        this.atOrAfters = atOrAfters;
    }
    
    public SourcePosition get(final String partID) {
        return this.sourcePositions.get(partID);
    }
    
    public Boolean getAtOrAfter(final String partId) {
        return this.atOrAfters.get(partId);
    }
    
    public boolean isEmpty() {
        return this.sourcePositions.isEmpty();
    }
    
    public void put(final String distID, final SourcePosition sourcePosition) {
        this.sourcePositions.put(distID, sourcePosition);
    }
    
    public boolean containsKey(final String partID) {
        return this.sourcePositions.containsKey(partID);
    }
    
    public Map<String, SourcePosition> getSourcePositions() {
        return this.sourcePositions;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        throw new RuntimeException("PartitionedSourcePosition objects cannot be compared. Instead use compareTo(SourcePosition, String)");
    }
    
    public int compareTo(final SourcePosition arg0, final String partID) throws IllegalArgumentException {
        if (!this.sourcePositions.containsKey(partID)) {
            throw new IllegalArgumentException("This PartitionedSourcePosition object does not contain a SourcePosition for partition " + partID);
        }
        return this.sourcePositions.get(partID).compareTo(arg0);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (final String key : this.sourcePositions.keySet()) {
            final SourcePosition value = this.sourcePositions.get(key);
            sb.append(key).append("=").append(value).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        final String result = sb.toString();
        return result;
    }
}
