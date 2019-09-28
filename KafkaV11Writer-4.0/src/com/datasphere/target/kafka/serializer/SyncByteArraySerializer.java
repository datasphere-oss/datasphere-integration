package com.datasphere.target.kafka.serializer;

import com.datasphere.intf.*;
import com.datasphere.source.lib.constant.*;
import java.nio.*;

public class SyncByteArraySerializer implements KafkaMessageSerilaizer
{
    private Formatter formatter;
    private boolean isBinaryFormatter;
    
    public SyncByteArraySerializer(final Formatter formatter, final boolean isBinaryFormatter) {
        this.formatter = formatter;
        this.isBinaryFormatter = isBinaryFormatter;
    }
    
    @Override
    public byte[] convertToBytes(final Object data) throws Exception {
        final byte[] hdEventBytes = this.formatter.format(data);
        byte[] value;
        if (this.isBinaryFormatter) {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(hdEventBytes.length + Constant.INTEGER_SIZE);
            byteBuffer.putInt(hdEventBytes.length);
            byteBuffer.put(hdEventBytes);
            value = byteBuffer.array();
        }
        else {
            value = hdEventBytes;
        }
        return value;
    }
}
