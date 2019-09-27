package com.datasphere.source.lib.reader;

import com.datasphere.common.exc.*;
import java.util.*;
import java.nio.*;

public class BufferManager extends ReaderBase
{
    private Map<String, Object> bufferMap;
    
    public BufferManager(final ReaderBase strategy) throws AdapterException {
        super(strategy);
        this.init();
    }
    
    public void init() throws AdapterException {
        super.init();
        this.bufferMap = new TreeMap<String, Object>();
        this.supportsMutipleEndpoint = true;
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.linkedStrategy.readBlock();
    }
    
    @Override
    public Object readBlock(final boolean multiEndpointSupport) throws AdapterException {
        final Object obj = this.linkedStrategy.readBlock(true);
        if (obj != null) {
            return this.appendData((DataPacket)obj);
        }
        return null;
    }
    
    protected Object allocateInitialBuffer() {
        return ByteBuffer.allocate(this.blockSize() * 2).flip();
    }
    
    public Object appendData(final DataPacket packet) {
        final String id = packet.id();
        Object obj = this.bufferMap.get(id);
        if (obj == null) {
            final Object bufferObj = this.allocateInitialBuffer();
            this.bufferMap.put(id, bufferObj);
            obj = bufferObj;
        }
        return this.appendData(obj, packet.data());
    }
    
    protected Object appendData(final Object obj, final Object data) {
        final ByteBuffer buf = (ByteBuffer)obj;
        final ByteBuffer dataBuf = (ByteBuffer)data;
        int position = buf.position();
        final int limit = buf.limit();
        int length = limit - position;
        final byte[] array = buf.array();
        if (position > 0) {
            System.arraycopy(array, position, array, 0, length);
            position = length;
        }
        length = position + dataBuf.limit();
        System.arraycopy(dataBuf.array(), 0, array, position, limit);
        buf.position(0);
        buf.limit(limit);
        return buf;
    }
    
    @Override
    public boolean supportsMutipleEndpoint() {
        return this.supportsMutipleEndpoint;
    }
}
