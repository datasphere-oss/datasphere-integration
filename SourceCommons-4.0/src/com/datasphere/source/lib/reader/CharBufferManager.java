package com.datasphere.source.lib.reader;

import com.datasphere.common.exc.*;
import java.nio.*;

public class CharBufferManager extends BufferManager
{
    public CharBufferManager(final ReaderBase strategy) throws AdapterException {
        super(strategy);
    }
    
    @Override
    public void init() throws AdapterException {
        super.init();
    }
    
    @Override
    protected Object allocateInitialBuffer() {
        return CharBuffer.allocate(this.blockSize() * 2).flip();
    }
    
    @Override
    protected Object appendData(final Object obj, final Object data) {
        final CharBuffer buf = (CharBuffer)obj;
        final CharBuffer dataBuf = (CharBuffer)data;
        final int position = buf.position();
        int limit = buf.limit();
        int length = limit - position;
        final char[] array = buf.array();
        if (position > 0) {
            System.arraycopy(array, position, array, 0, length);
            limit = length;
        }
        length = dataBuf.limit();
        System.arraycopy(dataBuf.array(), 0, array, limit, length);
        limit += length;
        buf.position(0);
        buf.limit(limit);
        return buf;
    }
}
