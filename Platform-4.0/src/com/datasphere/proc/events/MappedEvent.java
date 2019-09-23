package com.datasphere.proc.events;

import java.util.Map;

import com.datasphere.event.SourceEvent;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datasphere.uuid.UUID;

public class MappedEvent extends SourceEvent
{
    private static final long serialVersionUID = -739598948838180457L;
    public Map<String, Object> data;
    
    public MappedEvent() {
    }
    
    public MappedEvent(final UUID sourceUUID) {
        super(sourceUUID);
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeBoolean(this.data != null);
        if (this.data != null) {
            kryo.writeClassAndObject(output, (Object)this.data);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        final boolean dataNotNull = input.readBoolean();
        if (dataNotNull) {
            this.data = (Map<String, Object>)kryo.readClassAndObject(input);
        }
        else {
            this.data = null;
        }
    }
}
