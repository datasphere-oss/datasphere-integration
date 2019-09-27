package com.datasphere.source.kafka;

import com.datasphere.kafka.*;
import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.io.common.*;
import com.datasphere.source.lib.meta.*;

public class OLVPipedInputStream extends EventPipedInputStream
{
    public OLVPipedInputStream(final int pipeLength) {
        super(pipeLength);
    }
    
    public OLVPipedInputStream() {
    }
    
    @Override
    public Offset getMessageOffset() {
        return (Offset)new KafkaLongOffset((long)this.data[0]);
    }
    
    @Override
    public void initColumns() {
        this.columns = new Column[4];
        (this.columns[0] = new LongColumn()).setSize(CDCConstant.LONG_SIZE);
        (this.columns[1] = new ByteColumn()).setSize(CDCConstant.BYTE_SIZE);
        (this.columns[2] = new IntegerColumn()).setSize(CDCConstant.INTEGER_SIZE);
        this.columns[3] = new StringColumn();
        this.data = new Object[4];
    }
}
