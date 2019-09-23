package com.datasphere.proc.events;

import org.apache.log4j.*;
import com.fasterxml.jackson.databind.*;

import java.util.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import org.codehaus.jettison.json.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:JsonNodeEvent:1.0")
public class JsonNodeEvent extends SimpleEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    static Logger logger;
    @EventTypeData
    public JsonNode data;
    public Map<String, Object> metadata;
    
    public JsonNodeEvent() {
    }
    
    public JsonNodeEvent(final long timestamp) {
        super(timestamp);
    }
    
    public JsonNode getData() {
        return this.data;
    }
    
    public void setData(final JsonNode data) {
        this.data = data;
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (JsonNode)payload[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data, this.metadata };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        String dataVal = null;
        try {
            dataVal = JsonNodeEvent.jsonMapper.writeValueAsString((Object)this.data);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not serialize json data: " + this.data, e);
        }
        output.writeString(dataVal);
        kryo.writeClassAndObject(output, (Object)this.metadata);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final String dataVal = input.readString();
        if (dataVal != null) {
            try {
                this.data = JsonNodeEvent.jsonMapper.readTree(dataVal);
            }
            catch (Exception e) {
                throw new RuntimeException("Could not deserialize json data: " + dataVal, e);
            }
        }
        this.metadata = (Map<String, Object>)kryo.readClassAndObject(input);
    }
    
    public String toString() {
        final JSONObject superJSON = super.getJSONObject();
        try {
            superJSON.put("data", (Object)this.data.toString());
            superJSON.put("meta", (Object)this.metadata.entrySet().toString());
        }
        catch (JSONException e) {
            if (JsonNodeEvent.logger.isDebugEnabled()) {
                JsonNodeEvent.logger.debug((Object)e.getMessage());
            }
        }
        return superJSON.toString();
    }
    
    static {
        JsonNodeEvent.logger = Logger.getLogger((Class)JsonNodeEvent.class);
    }
}
