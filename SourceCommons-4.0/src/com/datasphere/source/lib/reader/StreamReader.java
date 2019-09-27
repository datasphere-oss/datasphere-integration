package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class StreamReader extends ReaderBase
{
    protected InputStream dataSource;
    boolean closeCalled;
    boolean isPipedInputStream;
    private Logger logger;
    
    protected StreamReader(final ReaderBase link) throws AdapterException {
        super(link);
        this.logger = Logger.getLogger((Class)StreamReader.class);
    }
    
    public StreamReader(final InputStream input) throws AdapterException {
        super((ReaderBase)null);
        this.logger = Logger.getLogger((Class)StreamReader.class);
        this.dataSource = input;
        this.isPipedInputStream = this.isPipedInputStream(input);
    }
    
    public StreamReader(final Property prop, final InputStream input) throws AdapterException {
        super(prop);
        this.logger = Logger.getLogger((Class)StreamReader.class);
        this.property = prop;
        this.dataSource = input;
        this.isPipedInputStream = this.isPipedInputStream(input);
        if (prop.getMap().get("EventMetadata") != null) {
            this.eventMetadataMap = (Map<String, Object>)prop.getMap().get("EventMetadata");
        }
    }
    
    private boolean isPipedInputStream(final InputStream in) {
        return in instanceof PipedInputStream;
    }
    
    @Override
    protected void init() throws AdapterException {
        super.init();
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
        try {
            if (this.closeCalled || (this.isPipedInputStream && this.dataSource.available() <= 0)) {
                return null;
            }
            final int bytes = this.dataSource.read(this.internalBuffer.array(), 0, this.internalBuffer.capacity());
            if (bytes >= 0) {
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)("{" + bytes + "} bytes have been read from {" + this.name() + "}"));
                }
                this.internalBuffer.position(0);
                this.internalBuffer.limit(bytes);
                return this.internalBuffer;
            }
            throw new AdapterException(Error.END_OF_DATASOURCE);
        }
        catch (IOException e) {
            if (!this.closeCalled) {
                throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            }
            return null;
        }
    }
    
    @Override
    public void setInputStream(final InputStream dataSource) throws AdapterException {
        if (this.dataSource != null) {
            try {
                this.close();
            }
            catch (IOException e) {
                throw new AdapterException(Error.GENERIC_EXCEPTION, (Throwable)e);
            }
            this.dataSource = null;
        }
        this.dataSource = dataSource;
    }
    
    @Override
    public InputStream getInputStream() {
        final InputStream tmpStream = this.dataSource;
        this.dataSource = null;
        return tmpStream;
    }
    
    @Override
    public void close() throws IOException {
        this.closeCalled = true;
        if (this.dataSource != null) {
            this.dataSource.close();
        }
        super.close();
    }
    
    @Override
    public int available() throws IOException {
        return this.dataSource.available();
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        return this.eventMetadataMap;
    }
}
