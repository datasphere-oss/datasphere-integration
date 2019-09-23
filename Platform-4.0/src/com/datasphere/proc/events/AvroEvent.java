package com.datasphere.proc.events;

import org.apache.log4j.*;
import org.apache.avro.generic.*;

import java.util.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import java.io.*;
import org.codehaus.jettison.json.*;
import com.fasterxml.jackson.databind.jsontype.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:AvroEvent:1.0")
public class AvroEvent extends SimpleEvent implements JsonSerializable
{
    static Logger logger;
    private static final long serialVersionUID = 5544519789903788150L;
    @EventTypeData
    public GenericRecord data;
    public Map<String, Object> metadata;
    
    public AvroEvent() {
    }
    
    public AvroEvent(final long timestamp) {
        super(timestamp);
    }
    
    public GenericRecord getData() {
        return this.data;
    }
    
    public void setData(final GenericRecord record) {
        this.data = record;
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (GenericRecord)payload[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data, this.metadata };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        if (this.data == null) {
            output.writeByte((byte)0);
        }
        else {
            output.writeByte((byte)1);
            kryo.writeClassAndObject(output, (Object)this.data);
        }
        if (this.metadata == null) {
            output.writeByte((byte)0);
        }
        else {
            output.writeByte((byte)1);
            kryo.writeClassAndObject(output, (Object)this.metadata);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        byte b = input.readByte();
        if (b != 0) {
            this.data = (GenericRecord)kryo.readClassAndObject(input);
        }
        b = input.readByte();
        if (b != 0) {
            this.metadata = (Map<String, Object>)kryo.readClassAndObject(input);
        }
    }
    
    public void serialize(final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("data", (Object)this.data.toString());
        jsonGenerator.writeStringField("metadata", this.metadata.entrySet().toString());
        jsonGenerator.writeEndObject();
    }
    
    public String toString() {
        final JSONObject superJSON = super.getJSONObject();
        try {
            superJSON.put("data", (Object)this.data.toString());
            superJSON.put("meta", (Object)this.metadata.entrySet().toString());
        }
        catch (JSONException e) {
            if (AvroEvent.logger.isDebugEnabled()) {
                AvroEvent.logger.debug((Object)e.getMessage());
            }
        }
        return superJSON.toString();
    }
    
    public void serializeWithType(final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
    }
    
    static {
        AvroEvent.logger = Logger.getLogger((Class)AvroEvent.class);
    }
}
