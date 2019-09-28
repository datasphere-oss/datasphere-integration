package com.datasphere.target.kafka.serializer;

import java.nio.ByteBuffer;

import com.datasphere.intf.Formatter;
import com.datasphere.kafka.schemaregistry.SchemaRegistry;
import com.datasphere.proc.BaseFormatter;
import com.datasphere.source.lib.intf.SchemaCreater;

public class DefaultByteWithSchemaRegistrySerializer implements KafkaMessageSerilaizer
{
    protected Formatter formatter;
    private int schemaId;
    private String subjectName;
    
    public DefaultByteWithSchemaRegistrySerializer(final Formatter formatter, final SchemaRegistry schemaRegistryClient) throws Exception {
        this.formatter = formatter;
        if (((BaseFormatter)formatter).getFormatterProperties().containsKey("EventType")) {
            this.subjectName = (String)((BaseFormatter)formatter).getFormatterProperties().get("EventType");
        }
        else if (((BaseFormatter)formatter).getFormatterProperties().containsKey("TypeName")) {
            this.subjectName = (String)((BaseFormatter)formatter).getFormatterProperties().get("TypeName");
        }
        this.schemaId = (Integer)((SchemaCreater)formatter).getSchemaRegistryId(this.subjectName, schemaRegistryClient);
    }
    
    @Override
    public byte[] convertToBytes(final Object data) throws Exception {
        final byte[] hdEventBytes = this.formatter.format(data);
        final ByteBuffer idLengthAndDataBuffer = ByteBuffer.allocate(8 + hdEventBytes.length);
        idLengthAndDataBuffer.putInt(hdEventBytes.length + 4);
        idLengthAndDataBuffer.putInt(this.schemaId);
        idLengthAndDataBuffer.put(hdEventBytes);
        return idLengthAndDataBuffer.array();
    }
}

