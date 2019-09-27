package com.datasphere.source.lib.reader;

import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;
import com.datasphere.source.lib.constant.*;
import com.datasphere.source.lib.io.common.*;
import com.datasphere.source.lib.prop.*;

import java.io.*;
import java.util.*;
import com.datasphere.kafka.*;

public class KafkaPartitionReader extends StreamReader
{
    private EventPipedInputStream olvStream;
    
    protected KafkaPartitionReader(final ReaderBase link) throws AdapterException {
        super(link);
    }
    
    public KafkaPartitionReader(final Property prop, final InputStream input) throws AdapterException {
        super(prop, input);
        this.olvStream = (EventPipedInputStream)input;
        this.eventMetadataMap.put("TopicName", prop.getMap().get("Topic"));
        this.eventMetadataMap.put("PartitionID", prop.getMap().get("PartitionID"));
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
        final Object obj = super.readBlock();
        if (this.olvStream.isNewMessage() && obj != null) {
            this.onOpen(Constant.eventType.ON_OPEN);
        }
        return obj;
    }
    
    @Override
    public void close() throws IOException {
        this.closeCalled = true;
        super.close();
    }
    
    @Override
    public Map<String, Object> getEventMetadata() {
        if (this.olvStream.getMessageOffset() instanceof KafkaLongOffset) {
            this.eventMetadataMap.put("KafkaRecordOffset", this.olvStream.getMessageOffset().getOffset());
        }
        else {
            this.eventMetadataMap.put("KinesisRecordOffset", this.olvStream.getMessageOffset().getOffset());
        }
        return this.eventMetadataMap;
    }
    
    @Override
    public String name() {
        final String name = this.olvStream.getMessageOffset().getName();
        return name;
    }
}
