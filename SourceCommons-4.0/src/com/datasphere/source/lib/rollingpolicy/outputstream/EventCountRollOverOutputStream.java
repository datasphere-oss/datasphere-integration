package com.datasphere.source.lib.rollingpolicy.outputstream;

import java.io.*;

import com.datasphere.source.lib.rollingpolicy.util.*;

public class EventCountRollOverOutputStream extends RollOverOutputStream
{
    private final long maxEventCount;
    private long currentEventCounter;
    
    public EventCountRollOverOutputStream(final OutputStreamBuilder outputStreamProvider, final RolloverFilenameFormat filenameFormat, final long maxEventCount, final boolean dataHasHeader) throws IOException {
        super(outputStreamProvider, filenameFormat);
        this.currentEventCounter = 0L;
        this.maxEventCount = maxEventCount;
        if (dataHasHeader) {
            this.currentEventCounter = -1L;
        }
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        this.out.write(b);
        ++this.currentEventCounter;
        if (this.currentEventCounter == this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes) throws IOException {
        this.out.write(bytes);
        ++this.currentEventCounter;
        if (this.currentEventCounter == this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes, final int off, final int len) throws IOException {
        this.out.write(bytes, off, len);
        ++this.currentEventCounter;
        if (this.currentEventCounter == this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public void rollover() throws IOException {
        super.rollover();
        this.currentEventCounter = 0L;
    }
}
