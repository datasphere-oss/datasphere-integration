package com.datasphere.source.lib.rollingpolicy.outputstream;

import org.apache.log4j.*;

import com.datasphere.source.lib.rollingpolicy.util.*;

import java.util.*;
import java.io.*;

public class EventCountAndTimeRolloverOutputStream extends RollOverOutputStream
{
    private static final Logger logger;
    private final long maxEventCount;
    private long currentEventCounter;
    private final long rotationTimeMillis;
    private Timer rollOverTimer;
    private int minimumEventCountToTriggerRollOver;
    
    public EventCountAndTimeRolloverOutputStream(final OutputStreamBuilder builder, final RolloverFilenameFormat format, final long maxEventCount, final long rotationTime, final boolean dataHasHeader) throws IOException {
        super(builder, format);
        this.currentEventCounter = 0L;
        this.minimumEventCountToTriggerRollOver = 0;
        this.maxEventCount = maxEventCount;
        this.rotationTimeMillis = rotationTime;
        if (dataHasHeader) {
            this.minimumEventCountToTriggerRollOver = 1;
        }
        (this.rollOverTimer = new Timer("EventCountAndTimeRolloverOutputStream")).schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (EventCountAndTimeRolloverOutputStream.this.currentEventCounter > EventCountAndTimeRolloverOutputStream.this.minimumEventCountToTriggerRollOver) {
                        EventCountAndTimeRolloverOutputStream.this.rollover();
                        EventCountAndTimeRolloverOutputStream.this.currentEventCounter = EventCountAndTimeRolloverOutputStream.this.minimumEventCountToTriggerRollOver;
                    }
                }
                catch (IOException e) {
                    EventCountAndTimeRolloverOutputStream.logger.error((Object)"Failure in performing roll over", (Throwable)e);
                }
            }
        }, this.rotationTimeMillis, this.rotationTimeMillis);
    }
    
    @Override
    public synchronized void write(final int b) throws IOException {
        this.out.write(b);
        ++this.currentEventCounter;
        if (this.currentEventCounter >= this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes) throws IOException {
        this.out.write(bytes);
        ++this.currentEventCounter;
        if (this.currentEventCounter >= this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public synchronized void write(final byte[] bytes, final int off, final int len) throws IOException {
        this.out.write(bytes, off, len);
        ++this.currentEventCounter;
        if (this.currentEventCounter >= this.maxEventCount) {
            this.rollover();
        }
    }
    
    @Override
    public void rollover() throws IOException {
        super.rollover();
        this.currentEventCounter = 0L;
        this.rollOverTimer.cancel();
        (this.rollOverTimer = new Timer("EventCountAndTimeRolloverOutputStream")).schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (EventCountAndTimeRolloverOutputStream.this.currentEventCounter > EventCountAndTimeRolloverOutputStream.this.minimumEventCountToTriggerRollOver) {
                        EventCountAndTimeRolloverOutputStream.this.rollover();
                        EventCountAndTimeRolloverOutputStream.this.currentEventCounter = EventCountAndTimeRolloverOutputStream.this.minimumEventCountToTriggerRollOver;
                    }
                }
                catch (IOException e) {
                    EventCountAndTimeRolloverOutputStream.logger.error((Object)"Failure in performing roll over", (Throwable)e);
                }
            }
        }, this.rotationTimeMillis, this.rotationTimeMillis);
    }
    
    @Override
    public synchronized void close() throws IOException {
        super.close();
        this.rollOverTimer.cancel();
    }
    
    static {
        logger = Logger.getLogger((Class)EventCountAndTimeRolloverOutputStream.class);
    }
}
