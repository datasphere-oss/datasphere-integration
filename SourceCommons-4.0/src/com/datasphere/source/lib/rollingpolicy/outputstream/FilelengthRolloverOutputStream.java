package com.datasphere.source.lib.rollingpolicy.outputstream;

import java.io.*;

import com.datasphere.source.lib.rollingpolicy.util.*;

public class FilelengthRolloverOutputStream extends RollOverOutputStream
{
    private final long rolloverTriggerLength;
    private long currentLength;
    
    public FilelengthRolloverOutputStream(final OutputStreamBuilder outputStreamProvider, final RolloverFilenameFormat filenameFormat, final long rolloverTriggerLength) throws IOException {
        super(outputStreamProvider, filenameFormat);
        this.currentLength = 0L;
        this.rolloverTriggerLength = rolloverTriggerLength;
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        this.out.write(b);
        this.currentLength += 4L;
        if (this.currentLength >= this.rolloverTriggerLength) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes) throws IOException {
        this.out.write(bytes);
        this.currentLength += bytes.length;
        if (this.currentLength >= this.rolloverTriggerLength) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes, final int off, final int len) throws IOException {
        this.out.write(bytes, off, len);
        this.currentLength += len;
        if (this.currentLength >= this.rolloverTriggerLength) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void rollover() throws IOException {
        this.currentLength = 0L;
        super.rollover();
    }
}
