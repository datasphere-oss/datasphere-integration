package com.datasphere.event;

import org.apache.log4j.*;
import org.codehaus.jettison.json.*;

import com.datasphere.anno.*;
import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
/*
 * 数据源端产生的事件
 */
public class SourceEvent extends SimpleEvent
{
    private static final long serialVersionUID = -1311015713656728401L;
    private static final Logger logger;
    @SpecialEventAttribute
    public UUID sourceUUID;
    
    public SourceEvent() {
        this.sourceUUID = null;
    }
    
    public SourceEvent(final long ts) {
        super(ts);
    }
    
    public SourceEvent(final UUID sourceUUID) {
        super(System.currentTimeMillis());
        this.sourceUUID = sourceUUID;
    }
    
    @Override
    public JSONObject getJSONObject() {
        final JSONObject superJSON = super.getJSONObject();
        try {
            superJSON.put("sourceUUID", (Object)this.sourceUUID);
        }
        catch (JSONException ex) {}
        return superJSON;
    }
    
    @Override
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        if (this.sourceUUID != null) {
            output.writeBoolean(true);
            this.sourceUUID.write(kryo, output);
        }
        else {
            output.writeBoolean(false);
        }
    }
    
    @Override
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final boolean hasSouceUuid = input.readBoolean();
        if (hasSouceUuid) {
            (this.sourceUUID = new UUID()).read(kryo, input);
        }
        else {
            this.sourceUUID = null;
        }
    }
    
    static {
        logger = Logger.getLogger((Class)SourceEvent.class);
    }
}
