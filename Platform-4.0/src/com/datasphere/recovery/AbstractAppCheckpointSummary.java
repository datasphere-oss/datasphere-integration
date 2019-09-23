package com.datasphere.recovery;

import java.io.*;
import java.sql.*;
import org.apache.commons.lang.builder.*;
import org.apache.log4j.*;

public class AbstractAppCheckpointSummary implements Serializable
{
    private static final long serialVersionUID = -1850611944670314876L;
    public int id;
    public final String nodeUri;
    public final String flowUri;
    public final String componentUri;
    public final String sourceUri;
    public SourcePosition lowSourcePosition;
    public String lowSourcePositionText;
    public SourcePosition highSourcePosition;
    public String highSourcePositionText;
    public boolean atOrAfter;
    public Timestamp updated;
    
    protected AbstractAppCheckpointSummary() {
        this.id = 0;
        this.nodeUri = null;
        this.flowUri = null;
        this.componentUri = null;
        this.sourceUri = null;
        this.lowSourcePosition = null;
        this.lowSourcePositionText = null;
        this.highSourcePosition = null;
        this.highSourcePositionText = null;
        this.atOrAfter = false;
    }
    
    protected AbstractAppCheckpointSummary(final String nodeUri, final String flowUri, final String componentUri, final String sourceUri, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final boolean atOrAfter) {
        this.id = 0;
        this.nodeUri = nodeUri;
        this.flowUri = flowUri;
        this.componentUri = componentUri;
        this.sourceUri = sourceUri;
        this.lowSourcePosition = lowSourcePosition;
        this.lowSourcePositionText = ((lowSourcePosition == null) ? "" : lowSourcePosition.toHumanReadableString());
        this.highSourcePosition = highSourcePosition;
        this.highSourcePositionText = ((highSourcePosition == null) ? "" : highSourcePosition.toHumanReadableString());
        this.atOrAfter = atOrAfter;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.toString()).toHashCode();
    }
    
    @Override
    public String toString() {
        return this.sourceUri + "=[" + this.lowSourcePositionText + ", " + this.highSourcePositionText + "]";
    }
    
    public String toHumanReadableString() {
        final String atOrAfterString = this.atOrAfter ? "^" : "@";
        if (this.highSourcePosition == null || this.lowSourcePosition.compareTo(this.highSourcePosition) == 0) {
            return "[" + atOrAfterString + this.lowSourcePositionText + "]";
        }
        return "[" + atOrAfterString + this.lowSourcePositionText + ", " + atOrAfterString + this.highSourcePositionText + "]";
    }
    
    public void prePersist() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    public void preUpdate() {
        this.updated = new Timestamp(System.currentTimeMillis());
    }
    
    public void setLowSourcePosition(final SourcePosition lowSourcePosition) {
        this.lowSourcePosition = lowSourcePosition;
        this.lowSourcePositionText = ((lowSourcePosition == null) ? "" : lowSourcePosition.toHumanReadableString());
    }
    
    public void setHighSourcePosition(final SourcePosition highSourcePosition) {
        this.highSourcePosition = highSourcePosition;
        this.highSourcePositionText = ((highSourcePosition == null) ? "" : highSourcePosition.toHumanReadableString());
    }
    
    public String getLowSourcePositionTextTrim() {
        if (this.lowSourcePositionText == null) {
            if (Logger.getLogger("Recovery").isInfoEnabled()) {
                Logger.getLogger("Recovery").info((Object)"App Checkpoint Summary found null value for low text. This shouldn't cause a problem but is unexpected.");
            }
            return null;
        }
        if (this.lowSourcePositionText.length() <= 512) {
            return this.lowSourcePositionText;
        }
        return this.lowSourcePositionText.substring(0, 512);
    }
    
    public String getHighSourcePositionTextTrim() {
        if (this.highSourcePositionText == null) {
            return null;
        }
        if (this.highSourcePositionText.length() <= 512) {
            return this.highSourcePositionText;
        }
        return this.highSourcePositionText.substring(0, 512);
    }
    
    public void setLowSourcePositionText(final String lowSourcePositionText) {
        this.lowSourcePositionText = lowSourcePositionText;
    }
    
    public void setHighSourcePositionText(final String highSourcePositionText) {
        this.highSourcePositionText = highSourcePositionText;
    }
}
