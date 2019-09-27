package com.datasphere.source.lib.intf;

import org.apache.avro.*;
import com.datasphere.kafka.schemaregistry.*;
import com.datasphere.source.kafka.*;

public interface SchemaCreater
{
    Schema createSchemaForTable(final Object p0) throws Exception;
    
    Schema createSchemaForControlRecord(final String p0);
    
    Integer getSchemaRegistryId(final String p0, final SchemaRegistry p1) throws Exception;
    
    void setAvroDeserializer(final AvroDeserializer p0);
}
