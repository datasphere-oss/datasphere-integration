package com.datasphere.proc.records;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.anno.EventType;
import com.datasphere.anno.EventTypeData;
import com.datasphere.event.SourceEvent;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:DAEvent:1.0")
public class Record extends SourceEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    @EventTypeData
    public Object[] data;
    public HashMap<String, Object> metadata;
    public HashMap<String, Object> userdata;
    public Object[] before;
    public byte[] dataPresenceBitMap;
    public byte[] beforePresenceBitMap;
    public UUID typeUUID;
    
    public Record(final long ts) {
        super(ts);
        this.typeUUID = null;
    }
    
    public void setData(final int index, final Object o) {
        this.data[index] = o;
        final int pos = index / 7;
        final int offset = index % 7;
        final byte bitset = (byte)(1 << offset);
        final byte b = this.dataPresenceBitMap[pos];
        this.dataPresenceBitMap[pos] = (byte)(b | bitset);
    }
    
    public void setBefore(final int index, final Object o) {
        this.before[index] = o;
        final int pos = index / 7;
        final int offset = index % 7;
        final byte bitset = (byte)(1 << offset);
        final byte b = this.beforePresenceBitMap[pos];
        this.beforePresenceBitMap[pos] = (byte)(b | bitset);
    }
    
    public Record() {
        this.typeUUID = null;
    }
    
    public Record(final int dataFieldCount, final UUID sourceUUID) {
        super(sourceUUID);
        this.typeUUID = null;
        final int bitmapSize = dataFieldCount / 7 + 1;
        this.dataPresenceBitMap = new byte[bitmapSize];
        this.beforePresenceBitMap = new byte[bitmapSize];
    }
    
    @Override
    public void setPayload(final Object[] payload) {
        this.data = payload;
    }
    
    public void putUserdata(final String key, final Object value) {
        synchronized (this) {
            if (this.userdata == null) {
                this.userdata = new HashMap<String, Object>();
            }
            this.userdata.put(key, value);
        }
    }
    
    public void removeUserData(final String key) {
        synchronized (this) {
            if (this.userdata != null) {
                this.userdata.remove(key);
            }
        }
    }
    
    @Override
    public Object[] getPayload() {
        return new Object[] { this.data, this.metadata };
    }
    
    public static Record makeCopy(final Record event) {
        if (event == null) {
            return event;
        }
        final Record newEvent = new Record(event.data.length, event.sourceUUID);
        newEvent.timeStamp = event.timeStamp;
        newEvent.typeUUID = event.typeUUID;
        if (event.data != null) {
            newEvent.data = new Object[event.data.length];
            int ii = 0;
            for (final Object o : event.data) {
                newEvent.setData(ii++, o);
            }
        }
        if (event.before != null) {
            newEvent.before = new Object[event.before.length];
            int ii = 0;
            for (final Object o : event.before) {
                newEvent.setBefore(ii++, o);
            }
        }
        if (event.metadata != null) {
            (newEvent.metadata = new HashMap<String, Object>()).putAll(event.metadata);
        }
        if (event.userdata != null) {
            (newEvent.userdata = new HashMap<String, Object>()).putAll(event.userdata);
        }
        return newEvent;
    }
    
    @Override
    public JSONObject getJSONObject() {
        final JSONObject superJSON = super.getJSONObject();
        final JSONArray jArray = new JSONArray();
        for (final Object object : this.data) {
            jArray.put(object);
        }
        try {
            superJSON.put("data", (Object)jArray);
            superJSON.put("meta", (Map)this.metadata);
            superJSON.put("userdata", (Map)this.userdata);
        }
        catch (JSONException ex) {}
        return superJSON;
    }
    
    @Override
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeBoolean(this.data != null);
        if (this.data != null) {
            output.writeInt(this.data.length);
            for (int i = 0; i < this.data.length; ++i) {
                kryo.writeClassAndObject(output, this.data[i]);
            }
        }
        kryo.writeClassAndObject(output, (Object)this.metadata);
        kryo.writeClassAndObject(output, (Object)this.userdata);
        output.writeBoolean(this.before != null);
        if (this.before != null) {
            output.writeInt(this.before.length);
            for (int i = 0; i < this.before.length; ++i) {
                kryo.writeClassAndObject(output, this.before[i]);
            }
        }
        output.writeBoolean(this.dataPresenceBitMap != null);
        if (this.dataPresenceBitMap != null) {
            output.writeInt(this.dataPresenceBitMap.length);
            output.writeBytes(this.dataPresenceBitMap);
        }
        output.writeBoolean(this.beforePresenceBitMap != null);
        if (this.beforePresenceBitMap != null) {
            output.writeInt(this.beforePresenceBitMap.length);
            output.writeBytes(this.beforePresenceBitMap);
        }
        output.writeBoolean(this.typeUUID != null);
        if (this.typeUUID != null) {
            kryo.writeClassAndObject(output, (Object)this.typeUUID);
        }
    }
    
    @Override
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final boolean dataNotNull = input.readBoolean();
        if (dataNotNull) {
            final int dataLen = input.readInt();
            this.data = new Object[dataLen];
            for (int i = 0; i < dataLen; ++i) {
                this.data[i] = kryo.readClassAndObject(input);
            }
        }
        this.metadata = (HashMap<String, Object>)kryo.readClassAndObject(input);
        this.userdata = (HashMap<String, Object>)kryo.readClassAndObject(input);
        final boolean beforeNotNull = input.readBoolean();
        if (beforeNotNull) {
            final int beforeLen = input.readInt();
            this.before = new Object[beforeLen];
            for (int j = 0; j < beforeLen; ++j) {
                this.before[j] = kryo.readClassAndObject(input);
            }
        }
        final boolean dataPresenceBitMapNotNull = input.readBoolean();
        if (dataPresenceBitMapNotNull) {
            final int len = input.readInt();
            input.readBytes(this.dataPresenceBitMap = new byte[len]);
        }
        final boolean beforePresenceBitMapNotNull = input.readBoolean();
        if (beforePresenceBitMapNotNull) {
            final int len2 = input.readInt();
            input.readBytes(this.beforePresenceBitMap = new byte[len2]);
        }
        final boolean typeUUIDNotNull = input.readBoolean();
        if (typeUUIDNotNull) {
            this.typeUUID = (UUID)kryo.readClassAndObject(input);
        }
    }
}
