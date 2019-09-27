package com.datasphere.source.lib.reader;

import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;
import com.datasphere.source.lib.prop.*;

public class RecoveryPositioner extends ReaderBase
{
    public RecoveryPositioner(final ReaderBase link) throws AdapterException {
        super(link);
    }
    
    protected RecoveryPositioner(final Property prop) throws AdapterException {
        super(prop);
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.linkedStrategy.readBlock();
    }
    
    @Override
    public void position(final CheckpointDetail attr, final boolean pos) throws AdapterException {
        this.linkedStrategy.position(attr, pos);
        long bytes = 0L;
        if (attr != null && attr.getRecordBeginOffset() != null) {
            try {
                bytes = this.linkedStrategy.skip(attr.getRecordBeginOffset());
            }
            catch (Exception exp) {
                throw new AdapterException("Exception while positioning reader", (Throwable)exp);
            }
        }
    }
}
