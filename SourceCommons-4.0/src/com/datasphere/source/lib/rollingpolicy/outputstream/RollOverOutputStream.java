package com.datasphere.source.lib.rollingpolicy.outputstream;

import com.datasphere.source.lib.intf.*;
import com.datasphere.source.lib.rollingpolicy.util.*;

import java.util.*;

import org.apache.log4j.*;
import java.io.*;

public abstract class RollOverOutputStream extends FilterOutputStream
{
    protected final RolloverFilenameFormat filenameFormat;
    private boolean changed;
    private Vector<RollOverObserver> observers;
    private OutputStreamBuilder outputStreamBuilder;
    private static final Logger logger;
    private boolean isRollingOver;
    
    public RollOverOutputStream(final OutputStreamBuilder outputStreamProvider, final RolloverFilenameFormat filenameFormat) throws IOException {
        super(outputStreamProvider.buildOutputStream(filenameFormat));
        this.changed = false;
        this.isRollingOver = false;
        this.outputStreamBuilder = outputStreamProvider;
        this.filenameFormat = filenameFormat;
        this.observers = new Vector<RollOverObserver>();
    }
    
    @Override
    public void write(final byte[] bytes) throws IOException {
        this.out.write(bytes);
    }
    
    public void registerObserver(final RollOverObserver observer) {
        if (!this.observers.contains(observer)) {
            this.observers.addElement(observer);
        }
    }
    
    public void notifyObserversBeforeRollover() throws IOException {
        Object[] localArray = null;
        synchronized (this) {
            if (!this.changed) {
                return;
            }
            localArray = this.observers.toArray();
            this.changed = false;
        }
        for (int i = 0; i < localArray.length; ++i) {
            ((RollOverObserver)localArray[i]).preRollover();
        }
    }
    
    public void notifyObserversAfterRollover(final String filename) throws IOException {
        Object[] localArray = null;
        synchronized (this) {
            if (!this.changed) {
                return;
            }
            localArray = this.observers.toArray();
            this.changed = false;
        }
        for (int i = 0; i < localArray.length; ++i) {
            ((RollOverObserver)localArray[i]).postRollover(filename);
        }
    }
    
    public synchronized void setChanged() {
        this.changed = true;
    }
    
    public synchronized void rollover() throws IOException {
        if (this.isRollingOver) {
            return;
        }
        this.isRollingOver = true;
        this.setChanged();
        this.notifyObserversBeforeRollover();
        final OutputStream tobeClosed = this.out;
        tobeClosed.close();
        final String lastRolledoverFile = this.filenameFormat.getCurrentFileName();
        this.out = this.outputStreamBuilder.buildOutputStream(this.filenameFormat);
        if (RollOverOutputStream.logger.isInfoEnabled()) {
            RollOverOutputStream.logger.info((Object)("Closing the file " + lastRolledoverFile + ". Opening a new file " + this.filenameFormat.getCurrentFileName()));
        }
        this.setChanged();
        this.notifyObserversAfterRollover(lastRolledoverFile);
        this.isRollingOver = false;
    }
    
    @Override
    public void close() throws IOException {
        this.setChanged();
        this.notifyObserversBeforeRollover();
        super.close();
        final String lastRolledoverFile = this.filenameFormat.getCurrentFileName();
        this.setChanged();
        this.notifyObserversAfterRollover(lastRolledoverFile);
    }
    
    static {
        logger = Logger.getLogger((Class)RollOverOutputStream.class);
    }
    
    public interface OutputStreamBuilder
    {
        OutputStream buildOutputStream(final RolloverFilenameFormat p0) throws IOException;
    }
}
