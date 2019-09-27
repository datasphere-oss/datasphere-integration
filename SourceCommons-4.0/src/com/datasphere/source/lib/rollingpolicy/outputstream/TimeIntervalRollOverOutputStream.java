package com.datasphere.source.lib.rollingpolicy.outputstream;

import org.apache.log4j.*;

import com.datasphere.source.lib.rollingpolicy.util.*;

import java.util.*;
import java.io.*;

public class TimeIntervalRollOverOutputStream extends RollOverOutputStream
{
    private long rotationTimeMillis;
    private Timer rollOverTimer;
    private long eventCounter;
    private Logger logger;
    private int minimumEventCountToTriggerRollOver;
    
    public TimeIntervalRollOverOutputStream(final OutputStreamBuilder outputStreamProvider, final RolloverFilenameFormat filenameFormat, final long maxRotationTime, final boolean dataHasHeader) throws IOException {
        super(outputStreamProvider, filenameFormat);
        this.eventCounter = 0L;
        this.logger = Logger.getLogger((Class)TimeIntervalRollOverOutputStream.class);
        this.minimumEventCountToTriggerRollOver = 0;
        this.rotationTimeMillis = maxRotationTime;
        this.rollOverTimer = new Timer("TimeIntervalRollOverOutputStream");
        if (dataHasHeader) {
            this.minimumEventCountToTriggerRollOver = 1;
        }
        this.rollOverTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (TimeIntervalRollOverOutputStream.this.eventCounter > TimeIntervalRollOverOutputStream.this.minimumEventCountToTriggerRollOver) {
                        TimeIntervalRollOverOutputStream.this.rollover();
                        TimeIntervalRollOverOutputStream.this.eventCounter = TimeIntervalRollOverOutputStream.this.minimumEventCountToTriggerRollOver;
                    }
                }
                catch (IOException e) {
                    TimeIntervalRollOverOutputStream.this.logger.error((Object)"Failure in performing roll over", (Throwable)e);
                }
            }
        }, this.rotationTimeMillis, this.rotationTimeMillis);
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        this.out.write(b);
        ++this.eventCounter;
    }
    
    @Override
    public synchronized void write(final byte[] bytes) throws IOException {
        this.out.write(bytes);
        ++this.eventCounter;
    }
    
    @Override
    public synchronized void write(final byte[] bytes, final int off, final int len) throws IOException {
        this.out.write(bytes, off, len);
        ++this.eventCounter;
    }
    
    @Override
    public synchronized void close() throws IOException {
        super.close();
        this.rollOverTimer.cancel();
    }
}
