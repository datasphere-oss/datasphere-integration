package com.datasphere.source.lib.reader;

import java.nio.*;

import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

public class StringReader extends ReaderBase
{
    private CharBuffer buffer;
    
    protected StringReader(final Property prop) throws AdapterException {
        super(prop);
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.buffer;
    }
    
    @Override
    public void setCharacterBuffer(final CharBuffer buffer) {
        this.buffer = buffer;
    }
}
