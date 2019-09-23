package com.datasphere.messaging;

import java.io.*;
import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import org.apache.commons.lang.builder.*;

public class From implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = -5191939960746595129L;
    private UUID from;
    private String componentName;
    
    public From() {
    }
    
    public From(final UUID from, final String componentName) {
        this.from = from;
        this.componentName = componentName;
    }
    
    public UUID getFrom() {
        return this.from;
    }
    
    public String getComponentName() {
        return this.componentName;
    }
    
    public void setFrom(final UUID from) {
        this.from = from;
    }
    
    public void setComponentName(final String componentName) {
        this.componentName = componentName;
    }
    
    public void write(final Kryo kryo, final Output output) {
        kryo.writeObjectOrNull(output, (Object)this.from, (Class)UUID.class);
        output.writeString(this.componentName);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.from = (UUID)kryo.readObjectOrNull(input, (Class)UUID.class);
        this.componentName = input.readString();
    }
    
    @Override
    public String toString() {
        return this.from.toString() + ":" + this.componentName;
    }
    
    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof From)) {
            return false;
        }
        final From that = (From)object;
        return this.from.equals((Object)that.getFrom()) && this.componentName.equals(that.getComponentName());
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.from.toString()).append((Object)this.componentName).toHashCode();
    }
}
