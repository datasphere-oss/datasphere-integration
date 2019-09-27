package com.datasphere.source.lib.reader;

import com.datasphere.recovery.*;
import com.datasphere.source.lib.prop.*;
import com.datasphere.common.exc.*;

public class GGTrailPositioner extends ReaderBase
{
    protected GGTrailPositioner(final Property prop) throws AdapterException {
        super(prop);
    }
    
    public GGTrailPositioner(final ReaderBase link) throws AdapterException {
        super(link);
        if (link != null) {
            this.linkedStrategy = link;
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.linkedStrategy.readBlock();
    }
    
    @Override
    public void position(final CheckpointDetail record, final boolean position) throws AdapterException {
        super.position(record, position);
        if (record != null) {
            this.skipBytes(record.getRecordEndOffset());
        }
        this.recoveryCheckpoint = new CheckpointDetail(record);
    }
}
