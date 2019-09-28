package com.datasphere.target.kafka.serializer;

import com.datasphere.intf.*;
import com.datasphere.kafka.schemaregistry.*;
import com.datasphere.proc.events.*;

public class SyncNativeAvroSerializer extends NativeAvroSerializer
{
    public SyncNativeAvroSerializer(final Formatter formatter, final SchemaRegistry schemaRegistryClient) {
        super(formatter, schemaRegistryClient);
    }
    
    @Override
    public byte[] convertToBytes(final Object data) throws Exception {
        final HDEvent cdcEvent = (HDEvent)((com.datasphere.runtime.containers.HDEvent)data).data;
        final Integer schemaId = this.getSchemaId(cdcEvent);
        if (schemaId == null) {
            return null;
        }
        return super.formatData(data, schemaId);
    }
}
