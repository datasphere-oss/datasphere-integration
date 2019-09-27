package com.datasphere.source.lib.io.common;

import java.io.*;
import com.datasphere.kafka.*;
import com.datasphere.source.lib.meta.*;

public abstract class EventPipedInputStream extends PipedInputStream
{
    private static final int DEFAULT_PIPE_LENGHT = 1024;
    private byte[] olvBuffer;
    private int olvBufferOffset;
    protected Column[] columns;
    protected Object[] data;
    private int lastProcessedColumn;
    private boolean readNewMessage;
    
    public EventPipedInputStream() {
        this.olvBufferOffset = 0;
        this.lastProcessedColumn = 0;
        this.readNewMessage = false;
        this.initOLVBuffer(1024);
        this.initColumns();
    }
    
    public EventPipedInputStream(final int pipeLength) {
        super(pipeLength);
        this.olvBufferOffset = 0;
        this.lastProcessedColumn = 0;
        this.readNewMessage = false;
        this.initOLVBuffer(pipeLength);
        this.initColumns();
    }
    
    public void initOLVBuffer(final int pipeLength) {
        if (pipeLength <= 0) {
            throw new IllegalArgumentException("Pipe Size <= 0");
        }
        this.olvBuffer = new byte[pipeLength];
    }
    
    public abstract void initColumns();
    
    @Override
    public int read(final byte[] b) throws IOException {
        return 0;
    }
    
    @Override
    public int available() throws IOException {
        return super.available();
    }
    
    @Override
    public void connect(final PipedOutputStream src) throws IOException {
        super.connect(src);
    }
    
    @Override
    public int read(final byte[] b, final int offset, final int length) throws IOException {
        int index = this.lastProcessedColumn;
        this.readNewMessage = false;
        if (length > 0) {
            while (index < this.columns.length) {
                if (index != 3) {
                    final int bytes = super.read(this.olvBuffer, this.olvBufferOffset, this.columns[index].getSize() - this.olvBufferOffset);
                    if (bytes < this.columns[index].getSize() - this.olvBufferOffset) {
                        this.olvBufferOffset += bytes;
                        this.lastProcessedColumn = index;
                    }
                    else {
                        this.olvBufferOffset = 0;
                        this.data[index] = this.columns[index].getValue(this.olvBuffer, 0, this.columns[index].getSize());
                        this.readNewMessage = true;
                        ++index;
                    }
                }
                else {
                    this.columns[index].setSize((int)this.data[2]);
                    if (this.columns[index].getSize() < length) {
                        final int bytes = super.read(b, offset, this.columns[index].getSize());
                        if (bytes < this.columns[index].getSize()) {
                            this.lastProcessedColumn = index;
                            this.data[2] = this.columns[index].getSize() - bytes;
                        }
                        else {
                            this.lastProcessedColumn = 0;
                        }
                        return bytes;
                    }
                    final int bytes = super.read(b, offset, length);
                    this.lastProcessedColumn = index;
                    this.data[2] = this.columns[index].getSize() - bytes;
                    return bytes;
                }
            }
        }
        return 0;
    }
    
    public static void main(final String[] args) {
    }
    
    public abstract Offset getMessageOffset();
    
    public boolean isNewMessage() {
        return this.data[1] != null && (byte)this.data[1] != 1 && this.readNewMessage;
    }
}
