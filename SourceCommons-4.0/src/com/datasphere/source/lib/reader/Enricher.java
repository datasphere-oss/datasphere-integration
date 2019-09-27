package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;

public class Enricher extends ReaderBase
{
    private boolean skipBom;
    private Logger logger;
    
    protected Enricher(final ReaderBase link) throws AdapterException {
        super(link);
        this.logger = Logger.getLogger((Class)Enricher.class);
    }
    
    public void init() throws AdapterException {
        super.init();
        this.skipBom = true;
        BOM.init();
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)"Enricher layer is initialized");
        }
    }
    
    @Override
    public void setCheckPointDetails(final CheckpointDetail cp) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setCheckPointDetails(cp);
        }
        else {
            this.recoveryCheckpoint = cp;
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        final ByteBuffer buffer = (ByteBuffer)this.linkedStrategy.readBlock();
        if (buffer != null) {
            try {
                if (this.skipBom) {
                    BOM.skip(this.CharSet(), buffer);
                    this.skipBom = false;
                }
            }
            catch (IOException e) {
                throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            }
        }
        return buffer;
    }
    
    @Override
    protected void onClose(final Object identifier) {
        super.onClose(identifier);
        this.skipBom = true;
    }
    
    @Override
    protected void onOpen(final Object identifier) {
        super.onOpen(identifier);
        this.skipBom = true;
    }
}
