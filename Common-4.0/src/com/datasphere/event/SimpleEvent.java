package com.datasphere.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.anno.SpecialEventAttribute;
import com.datasphere.distribution.Partitionable;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import flexjson.JSON;

public class SimpleEvent extends Event implements Serializable, Partitionable, KryoSerializable
{
    private static final long serialVersionUID = -4431260571600094020L;
    public static transient ObjectMapper jsonMapper;
    @SpecialEventAttribute
    public String _id;
    @SpecialEventAttribute
    @JSON(include = false)
    @JsonIgnore
    public UUID _da_SimpleEvent_ID;
    @SpecialEventAttribute
    public long timeStamp;
    @SpecialEventAttribute
    public long originTimeStamp;
    @SpecialEventAttribute
    public String key;
    @JSON(include = false)
    @JsonIgnore
    @SpecialEventAttribute
    public Object[] payload;
    @JSON(include = false)
    @JsonIgnore
    @SpecialEventAttribute
    public List<Object[]> linkedSourceEvents;
    @JSON(include = false)
    @JsonIgnore
    @SpecialEventAttribute
    public byte[] fieldIsSet;
    @JSON(include = false)
    @JsonIgnore
    @SpecialEventAttribute
    public boolean allFieldsSet;
    
    public SimpleEvent() {
        this.allFieldsSet = true;
    }
    
    public SimpleEvent(final long timestamp) {
        this.allFieldsSet = true;
        this.timeStamp = timestamp;
        this._da_SimpleEvent_ID = new UUID(this.timeStamp);
    }
    
    public void init(final long timestamp) {
        this.timeStamp = timestamp;
        this._da_SimpleEvent_ID = new UUID(this.timeStamp);
    }
    
    @JSON(include = false)
    @JsonIgnore
    @Override
    public UUID get_da_SimpleEvent_ID() {
        return this._da_SimpleEvent_ID;
    }
    
    public String getIDString() {
        return (this._da_SimpleEvent_ID == null) ? "<NOTSET>" : this._da_SimpleEvent_ID.getUUIDString();
    }
    
    public void setIDString(final String IDstring) {
        this._da_SimpleEvent_ID = new UUID(IDstring);
    }
    
    @Override
    public long getTimeStamp() {
        return this.timeStamp;
    }
    
    @Override
    public void setTimeStamp(final long timestamp) {
        this.timeStamp = timestamp;
    }
    
    @Override
    public void setPayload(final Object[] payload) {
        this.payload = payload;
    }
    
    @Override
    public Object[] getPayload() {
        return this.payload;
    }
    
    @Override
    public String getKey() {
        return this.key;
    }
    
    @Override
    public void setKey(final String key) {
        this.key = key;
    }
    
    public Object fromJSON(final String json) {
        try {
            return SimpleEvent.jsonMapper.readValue(json, (Class)this.getClass());
        }
        catch (Exception ex) {
            return "<Undeserializable>";
        }
    }
    
    public String toJSON() {
        try {
            return SimpleEvent.jsonMapper.writeValueAsString((Object)this);
        }
        catch (Exception ex) {
            return "<Undeserializable>";
        }
    }
    
    public String toString() {
        return this.toJSON();
    }
    
    @Override
    public boolean usePartitionId() {
        return false;
    }
    
    @JSON(include = false)
    @JsonIgnore
    @Override
    public Object getPartitionKey() {
        return (this.key == null) ? this._da_SimpleEvent_ID : this.key;
    }
    
    @JSON(include = false)
    @JsonIgnore
    @Override
    public int getPartitionId() {
        return 0;
    }
    
    public void setSourceEvents(final List<Object[]> sourceEvents) {
        this.linkedSourceEvents = sourceEvents;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public JSONObject getJSONObject() {
        final JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("_id", (Object)this._da_SimpleEvent_ID);
            jsonObject.put("timeStamp", this.timeStamp);
            jsonObject.put("key", (Object)this.key);
        }
        catch (JSONException ex) {}
        return jsonObject;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this._da_SimpleEvent_ID != null) {
            output.writeByte(0);
            this._da_SimpleEvent_ID.write(kryo, output);
        }
        else {
            output.writeByte(1);
        }
        output.writeLong(this.timeStamp);
        output.writeString(this.key);
        output.writeBoolean(this.payload != null);
        if (this.payload != null) {
            output.writeInt(this.payload.length);
            for (int p = 0; p < this.payload.length; ++p) {
                kryo.writeClassAndObject(output, this.payload[p]);
            }
        }
        output.writeBoolean(this.linkedSourceEvents != null);
        if (this.linkedSourceEvents != null) {
            output.writeInt(this.linkedSourceEvents.size());
            for (final Object[] oa : this.linkedSourceEvents) {
                kryo.writeClassAndObject(output, (Object)oa);
            }
        }
        output.writeBoolean(this.fieldIsSet != null);
        if (this.fieldIsSet != null) {
            output.writeInt(this.fieldIsSet.length);
            output.write(this.fieldIsSet);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        final byte has_da_SimpleEvent_ID = input.readByte();
        if (has_da_SimpleEvent_ID == 0) {
            (this._da_SimpleEvent_ID = new UUID()).read(kryo, input);
        }
        this.timeStamp = input.readLong();
        this.key = input.readString();
        final boolean payloadNotNull = input.readBoolean();
        if (payloadNotNull) {
            final int numPayloadObjects = input.readInt();
            this.payload = new Object[numPayloadObjects];
            for (int p = 0; p < numPayloadObjects; ++p) {
                this.payload[p] = kryo.readClassAndObject(input);
            }
        }
        final boolean linkedSourceEventsNotNull = input.readBoolean();
        if (linkedSourceEventsNotNull) {
            final int numLinkedSourceEvents = input.readInt();
            this.linkedSourceEvents = new ArrayList<Object[]>(numLinkedSourceEvents);
            for (int l = 0; l < numLinkedSourceEvents; ++l) {
                final Object[] oa = (Object[])kryo.readClassAndObject(input);
                this.linkedSourceEvents.add(oa);
            }
        }
        final boolean fieldIsSetNotNull = input.readBoolean();
        if (fieldIsSetNotNull) {
            final int fieldIsSetLength = input.readInt();
            input.read(this.fieldIsSet = new byte[fieldIsSetLength]);
        }
    }
    
    public boolean isFieldSet(final int index) {
        if (this.allFieldsSet) {
            return true;
        }
        if (this.fieldIsSet == null) {
            return true;
        }
        final int pos = index / 7;
        final int offset = index % 7;
        final byte b = (byte)(1 << offset);
        final int val = (this.fieldIsSet[pos] & b) >> offset;
        return val == 1;
    }
    
    public int hashCode() {
        return this._da_SimpleEvent_ID.hashCode();
    }
    
    public boolean equals(final Object obj) {
        return obj instanceof SimpleEvent && this._da_SimpleEvent_ID.equals(((SimpleEvent)obj)._da_SimpleEvent_ID);
    }
    
    static {
        SimpleEvent.jsonMapper = ObjectMapperFactory.newInstance();
    }
}
