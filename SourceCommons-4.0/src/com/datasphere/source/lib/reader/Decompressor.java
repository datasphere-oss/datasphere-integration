package com.datasphere.source.lib.reader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class Decompressor extends StreamReader
{
    private Logger logger;
    boolean failedDuringInit;
    int retryCnt;
    InputStream failedStream;
    Map<String, String> typeMap;
    String compressionType;
    
    protected Decompressor(final ReaderBase link) throws AdapterException {
        super(link);
        this.logger = Logger.getLogger((Class)Decompressor.class);
        this.typeMap = new HashMap<String, String>() {
            private static final long serialVersionUID = -5991261969577676448L;
            
            {
                this.put("gzip", "gz");
            }
        };
    }
    
    public void init() throws AdapterException {
        super.init();
        this.compressionType = this.linkedStrategy.property().getString(Property.COMPRESSION_TYPE, "").toLowerCase();
        if (this.typeMap.get(this.compressionType) != null) {
            this.compressionType = this.typeMap.get(this.compressionType);
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        try {
            if (this.dataSource == null || this.failedDuringInit) {
                InputStream ip = null;
                if (this.failedDuringInit) {
                    ip = this.failedStream;
                }
                else {
                    ip = this.linkedStrategy.getInputStream();
                }
                if (ip != null) {
                    if (this.logger.isTraceEnabled()) {
                        this.logger.trace((Object)("Decompression layer is initialized with compression type {" + this.compressionType + "}"));
                    }
                    this.failedStream = ip;
                    this.dataSource = (InputStream)new CompressorStreamFactory().createCompressorInputStream(this.compressionType, ip);
                    this.failedStream = null;
                    this.failedDuringInit = false;
                    this.retryCnt = 0;
                }
                else {
                    this.dataSource = null;
                }
            }
        }
        catch (CompressorException e) {
            if (this.closeCalled) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)"Closed is called, returning null");
                }
                return null;
            }
            if (this.retryCnt < 5) {
                try {
                    if (!this.failedStream.markSupported()) {
                        if (this.failedStream instanceof FileInputStream) {
                            final FileChannel fc = ((FileInputStream)this.failedStream).getChannel();
                            fc.position(0L);
                        }
                    }
                    else {
                        this.failedStream.reset();
                    }
                }
                catch (IOException ioExp) {
                    this.logger.warn((Object)("Got exception while positioning the stream to begining {" + ioExp.getMessage() + "}"));
                }
                this.failedDuringInit = true;
                ++this.retryCnt;
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)("Got excepption in Decompressor: {" + e.getCause().getMessage() + "} this could be due to partial write. Retry attempt will be made at reading from the begining {" + this.name() + "} again"));
                }
                return null;
            }
            final InputStream nextStream = this.linkedStrategy.getInputStream();
            if (nextStream != null) {
                this.logger.warn((Object)("Retried reinitializing Decompressor {" + this.retryCnt + "} times, moving to {" + this.name() + "}"));
                this.failedStream = nextStream;
                this.failedDuringInit = true;
            }
            else {
                this.retryCnt = 0;
                this.failedDuringInit = true;
            }
            try {
                Thread.sleep(100L);
            }
            catch (InterruptedException ex) {}
            return null;
        }
        if (this.dataSource != null) {
            try {
                return super.readBlock();
            }
            catch (AdapterException exp) {
                if (exp.getType() == Error.END_OF_DATASOURCE) {
                    if (this.dataSource != null) {
                        try {
                            this.dataSource.close();
                        }
                        catch (IOException e2) {
                            e2.printStackTrace();
                        }
                        this.dataSource = null;
                    }
                    return null;
                }
                throw exp;
            }
        }
        return null;
    }
    
    @Override
    public void close() {
        try {
            super.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void position(final CheckpointDetail recordAttribute, final boolean position) throws AdapterException {
        if (recordAttribute != null) {
            recordAttribute.setRecordBeginOffset(0L);
            recordAttribute.setRecordLength(0L);
            recordAttribute.setRecordEndOffset(0L);
        }
        if (this.linkedStrategy != null) {
            this.linkedStrategy.position(recordAttribute, position);
        }
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.getEventMetadata();
        }
        return this.eventMetadataMap;
    }
}
