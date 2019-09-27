package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.locks.LockSupport;

import org.apache.log4j.Logger;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.common.exc.RecordException;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.prop.Property;

public abstract class ReaderBase extends Observable
{
    private final int block = 65536;
    protected ReaderBase linkedStrategy;
    protected int blockSize;
    protected boolean isThreaded;
    protected boolean hasQueue;
    protected Property property;
    protected String name;
    protected ByteBuffer internalBuffer;
    protected boolean stopRead;
    protected String identifier;
    protected long bytesToSkip;
    protected CheckpointDetail recoveryCheckpoint;
    protected boolean supportsMutipleEndpoint;
    protected InputStream inputStream;
    protected ReaderBase upstream;
    private Logger logger;
    int retryCount;
    protected boolean dontBlockOnEOF;
    protected boolean breakOnNoRecord;
    protected Map<String, Object> eventMetadataMap;
    protected String componentName;
    protected UUID componentUUID;
    protected String distributionID;
    
    protected ReaderBase(final Property prop) throws AdapterException {
        this.name = "";
        this.stopRead = false;
        this.bytesToSkip = 0L;
        this.logger = Logger.getLogger((Class)ReaderBase.class);
        this.retryCount = 0;
        this.eventMetadataMap = new HashMap<String, Object>();
        this.componentName = null;
        this.componentUUID = null;
        this.distributionID = "";
        this.property = prop;
        this.blockSize = this.property.blocksize * 1024;
        this.breakOnNoRecord = this.property.getBoolean("breakonnorecord", false);
        this.dontBlockOnEOF = this.property.dontBlockOnEOF;
    }
    
    protected ReaderBase(final ReaderBase link) throws AdapterException {
        this.name = "";
        this.stopRead = false;
        this.bytesToSkip = 0L;
        this.logger = Logger.getLogger((Class)ReaderBase.class);
        this.retryCount = 0;
        this.eventMetadataMap = new HashMap<String, Object>();
        this.componentName = null;
        this.componentUUID = null;
        this.distributionID = "";
        this.linkedStrategy = link;
        if (link != null) {
            this.blockSize = this.linkedStrategy.blockSize();
            link.upstream(this);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("Reader " + this.name() + " is linked with the following strategy " + link.name()));
            }
            this.breakOnNoRecord = this.linkedStrategy.breakOnNoRecord();
        }
    }
    
    protected void init(final Property prop) throws AdapterException {
        this.property = prop;
        this.blockSize = this.property.blocksize * 1024;
        this.dontBlockOnEOF = this.property.dontBlockOnEOF;
        this.init();
    }
    
    protected void init() throws AdapterException {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.init();
        }
        this.init(false);
    }
    
    protected void init(final boolean forwardTheCall) throws AdapterException {
        if (this.blockSize == 0) {
            this.blockSize = 65536;
        }
        (this.internalBuffer = ByteBuffer.allocate(this.blockSize())).flip();
        this.recoveryCheckpoint = new CheckpointDetail();
    }
    
    public String name() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.name();
        }
        return this.name;
    }
    
    protected void name(final String name) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.name(name);
        }
        else {
            this.name = name;
        }
    }
    
    public int blockSize() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.blockSize();
        }
        return this.blockSize;
    }
    
    protected void blockSize(final int size) {
        this.blockSize = size;
    }
    
    public Property property() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.property();
        }
        return this.property;
    }
    
    public long skip(final long bytes) throws IOException {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.skip(bytes);
        }
        return this.bytesToSkip = bytes;
    }
    
    public InputStream getInputStream() throws AdapterException {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.getInputStream();
        }
        return this.inputStream;
    }
    
    public long bytesToSkip() {
        return this.bytesToSkip;
    }
    
    public String CharSet() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.CharSet();
        }
        if (this.property != null) {
            return this.property.charset;
        }
        return null;
    }
    
    protected void registerWithSelector(final Selector selector) throws AdapterException {
    }
    
    protected Object readBlock(final SelectableChannel sc) throws AdapterException {
        return null;
    }
    
    protected void enqueue(final Object obj) {
    }
    
    public abstract Object readBlock() throws AdapterException;
    
    public Object readBlock(final boolean multiEndpointSupport) throws AdapterException {
        return null;
    }
    
    public void close() throws IOException {
        this.stopRead = true;
        if (this.linkedStrategy != null) {
            this.linkedStrategy.close();
            this.linkedStrategy = null;
        }
        if (this.internalBuffer != null) {
            this.internalBuffer.clear();
            this.internalBuffer = null;
        }
    }
    
    public int available() throws IOException {
        try {
            if (this.internalBuffer == null) {
                this.internalBuffer = (ByteBuffer)this.readBlock();
            }
            if (this.internalBuffer != null) {
                if (this.internalBuffer.limit() - this.internalBuffer.position() == 0) {
                    this.internalBuffer = (ByteBuffer)this.readBlock();
                    if (this.internalBuffer == null) {
                        return 0;
                    }
                }
                return this.internalBuffer.limit() - this.internalBuffer.position();
            }
        }
        catch (AdapterException e) {
            this.logger.error((Object)e);
        }
        return 0;
    }
    
    private void retryWait() {
        if (this.retryCount < 10000) {
            ++this.retryCount;
        }
        else if (this.retryCount < 20000) {
            Thread.yield();
            ++this.retryCount;
        }
        else {
            LockSupport.parkNanos(this.retryCount);
            if (this.retryCount < 10000000) {
                this.retryCount += 10000;
            }
        }
    }
    
    public int read() throws IOException {
        if (this.internalBuffer != null && this.internalBuffer.position() < this.internalBuffer.limit()) {
            return this.internalBuffer.get() & 0xFF;
        }
        ByteBuffer tmp = null;
        this.retryCount = 0;
        do {
            try {
                tmp = (ByteBuffer)this.readBlock();
                if (tmp != null) {
                    this.internalBuffer = tmp;
                    this.retryCount = 0;
                    return this.internalBuffer.get() & 0xFF;
                }
                if (this.breakOnNoRecord) {
                    throw new RuntimeException((Throwable)new RecordException(RecordException.Type.NO_RECORD));
                }
                this.retryWait();
                if (this.stopRead || (this.dontBlockOnEOF && this.retryCount > 30000)) {
                    return -1;
                }
            }
            catch (AdapterException exp) {
                exp.printStackTrace();
            }
        } while (tmp == null);
        return 0;
    }
    
    public int read(final byte[] buffer) throws AdapterException {
        return this.read(buffer, 0, buffer.length);
    }
    
    public byte[] peek(final int peekSize) throws AdapterException {
        if (this.internalBuffer == null) {
            this.internalBuffer = (ByteBuffer)this.readBlock();
            if (this.internalBuffer == null) {
                return null;
            }
        }
        int size = this.internalBuffer.limit() - this.internalBuffer.position();
        if (size < peekSize) {
            final ByteBuffer tmpBuff = (ByteBuffer)this.readBlock();
            if (tmpBuff == null) {
                return null;
            }
            final int requiredSize = size + tmpBuff.limit();
            final ByteBuffer newBuffer = ByteBuffer.allocate(requiredSize);
            System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), newBuffer.array(), 0, size);
            System.arraycopy(tmpBuff.array(), 0, newBuffer.array(), size, tmpBuff.limit());
            newBuffer.position(0);
            newBuffer.limit(requiredSize);
            this.internalBuffer = newBuffer;
        }
        size = this.internalBuffer.limit() - this.internalBuffer.position();
        if (size < peekSize) {
            return null;
        }
        final byte[] peekData = new byte[peekSize];
        System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), peekData, 0, peekSize);
        return peekData;
    }
    
    public void discardPacket() throws AdapterException {
        this.internalBuffer = (ByteBuffer)this.readBlock();
    }
    
    public int read(final byte[] buffer, final int off, final int len) {
        int size = 0;
        if (len == 0) {
            return 0;
        }
        if (this.internalBuffer != null) {
            size = this.internalBuffer.limit() - this.internalBuffer.position();
        }
        if (size == 0) {
            this.retryCount = 0;
            do {
                try {
                    this.internalBuffer = (ByteBuffer)this.readBlock();
                    if (this.internalBuffer == null) {
                        if (this.breakOnNoRecord) {
                            throw new RuntimeException((Throwable)new RecordException(RecordException.Type.NO_RECORD));
                        }
                        this.retryWait();
                        if (this.stopRead || (this.dontBlockOnEOF && this.retryCount > 30000)) {
                            return -1;
                        }
                    }
                    if (this.logger.isDebugEnabled()) {
                        try {
                            if (this.internalBuffer != null) {
                                final byte[] tmp = new byte[this.internalBuffer.limit() + 1];
                                System.arraycopy(this.internalBuffer.array(), 0, tmp, 0, this.internalBuffer.limit());
                                this.logger.debug((Object)("Data : [" + new String(tmp, "UTF-8") + "]"));
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                catch (AdapterException sExp) {
                    return -1;
                }
            } while (this.internalBuffer == null && !Thread.currentThread().isInterrupted());
            if (this.internalBuffer != null) {
                size = this.internalBuffer.limit() - this.internalBuffer.position();
            }
        }
        if (len < size) {
            size = len;
        }
        if (this.internalBuffer != null) {
            System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), buffer, off, size);
            this.internalBuffer.position(this.internalBuffer.position() + size);
        }
        this.retryCount = 0;
        return size;
    }
    
    public void registerObserver(final Object observer) {
        this.addObserver((Observer)observer);
    }
    
    public long getEOFPosition() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.getEOFPosition();
        }
        return 0L;
    }
    
    public void position(final CheckpointDetail recordAttribute, final boolean position) throws AdapterException {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.position(recordAttribute, position);
        }
    }
    
    public long skipBytes(final long offset) throws AdapterException {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.skipBytes(offset);
        }
        return 0L;
    }
    
    public CheckpointDetail getCheckpointDetail() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.getCheckpointDetail();
        }
        return this.recoveryCheckpoint;
    }
    
    public void setCheckPointDetails(final CheckpointDetail cp) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setCheckPointDetails(cp);
        }
        else {
            this.recoveryCheckpoint = cp;
        }
    }
    
    public boolean breakOnNoRecord() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.breakOnNoRecord();
        }
        return this.breakOnNoRecord;
    }
    
    public String identifier() {
        if (this.identifier != null) {
            return this.identifier;
        }
        return this.linkedStrategy.identifier();
    }
    
    public boolean supportsMutipleEndpoint() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.supportsMutipleEndpoint();
        }
        return this.supportsMutipleEndpoint;
    }
    
    protected void onClose(final Object identifier) {
        if (this.upstream != null) {
            this.upstream.onClose(Constant.eventType.ON_CLOSE);
        }
        else {
            this.setChanged();
            this.notifyObservers(identifier);
        }
    }
    
    protected void onOpen(final Object identifier) {
        if (this.upstream != null) {
            this.upstream.onOpen(Constant.eventType.ON_OPEN);
        }
        else {
            this.setChanged();
            this.notifyObservers(identifier);
        }
    }
    
    public void setChangedFlag() {
        this.setChanged();
    }
    
    public void upstream(final ReaderBase up) {
        this.upstream = up;
    }
    
    public void downstream(final ReaderBase down) {
        this.linkedStrategy = down;
    }
    
    public int eofdelay() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.eofdelay();
        }
        return this.property().eofdelay;
    }
    
    public boolean markSupported() {
        return false;
    }
    
    public void setInputStream(final InputStream dataSource) throws AdapterException {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setInputStream(dataSource);
        }
    }
    
    public void setCharacterBuffer(final CharBuffer buff) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setCharacterBuffer(buff);
        }
    }
    
    public Map<String, Object> getEventMetadata() {
        if (this.linkedStrategy != null) {
            this.eventMetadataMap = this.linkedStrategy.getEventMetadata();
        }
        return this.eventMetadataMap;
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.publishMonitorEvents(events);
        }
    }
    
    public void setComponentName(final String componentName) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setComponentName(componentName);
        }
        else {
            this.componentName = componentName;
        }
    }
    
    public void setComponentUUID(final UUID componentUUID) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setComponentUUID(componentUUID);
        }
        else {
            this.componentUUID = componentUUID;
        }
    }
    
    public void setDistributionID(final String distributionID) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setDistributionID(distributionID);
        }
        else {
            this.distributionID = distributionID;
        }
    }
    
    public FileMetadataExtension getFileMetadataExtension() {
        if (this.linkedStrategy != null) {
            return this.linkedStrategy.getFileMetadataExtension();
        }
        return null;
    }
}
