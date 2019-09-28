package com.datasphere.target.kafka.serializer;

import java.nio.ByteBuffer;

import com.datasphere.intf.Formatter;
import com.datasphere.source.lib.constant.Constant;

public class DefaultByteArraySerializer implements KafkaMessageSerilaizer
{
    private Formatter formatter;
    private boolean isBinaryFormatter;
    
    public DefaultByteArraySerializer(final Formatter formatter, final boolean isBinaryFormatter) {
        this.formatter = formatter;
        this.isBinaryFormatter = isBinaryFormatter;
    }
    
    @Override
    public byte[] convertToBytes(final Object data) throws Exception {
        final byte[] headerBytes = this.formatter.addHeader();
        final byte[] footerBytes = this.formatter.addFooter();
        final byte[] hdEventBytes = this.formatter.format(data);
        byte[] value = null;
        if (this.isBinaryFormatter) {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(hdEventBytes.length + Constant.INTEGER_SIZE);
            byteBuffer.putInt(hdEventBytes.length);
            byteBuffer.put(hdEventBytes);
            value = byteBuffer.array();
        }
        else if (headerBytes != null && footerBytes != null) {
            value = new byte[headerBytes.length + hdEventBytes.length + footerBytes.length];
            System.arraycopy(headerBytes, 0, value, 0, headerBytes.length);
            System.arraycopy(hdEventBytes, 0, value, headerBytes.length, hdEventBytes.length);
            System.arraycopy(footerBytes, 0, value, headerBytes.length + hdEventBytes.length, footerBytes.length);
        }
        else {
            value = hdEventBytes;
        }
        return value;
    }
}

