package com.datasphere.recovery;

import org.joda.time.*;

public class SalesforceReaderSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -5432586375077517449L;
    private boolean enabledInitialLoad;
    private boolean initializationDone;
    private String lastEpochDataReceived;
    
    public SalesforceReaderSourcePosition(final String lastEpoc, final boolean enableInitialLoad, final boolean initializationDone) {
        this.lastEpochDataReceived = lastEpoc;
        this.enabledInitialLoad = enableInitialLoad;
        this.initializationDone = initializationDone;
    }
    
    public String getLastEpochDataReceived() {
        return this.lastEpochDataReceived;
    }
    
    public void setLastEpochDataReceived(final String lastEpochDataReceived) {
        this.lastEpochDataReceived = lastEpochDataReceived;
    }
    
    public boolean isEnabledInitialLoad() {
        return this.enabledInitialLoad;
    }
    
    public void setEnabledInitialLoad(final boolean enabledInitialLoad) {
        this.enabledInitialLoad = enabledInitialLoad;
    }
    
    public boolean isInitializationDone() {
        return this.initializationDone;
    }
    
    public void setInitializationDone(final boolean initializationDone) {
        this.initializationDone = initializationDone;
    }
    
    @Override
    public int compareTo(final SourcePosition that) {
        if (that != null && that instanceof SalesforceReaderSourcePosition) {
            final SalesforceReaderSourcePosition thatPosition = (SalesforceReaderSourcePosition)that;
            return DateTime.parse(this.lastEpochDataReceived).compareTo((ReadableInstant)DateTime.parse(thatPosition.lastEpochDataReceived));
        }
        return Integer.MIN_VALUE;
    }
    
    @Override
    public String toString() {
        return "SalesforceReaderSourcePosition [enabledInitialLoad=" + this.enabledInitialLoad + ", initializationDone=" + this.initializationDone + ", lastPollingDateTime=" + this.lastEpochDataReceived + "]";
    }
}
