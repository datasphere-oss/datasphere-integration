package com.datasphere.runtime;

import java.io.*;

import com.datasphere.runtime.containers.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class StreamEvent implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = -8855223767719326349L;
    private ITaskEvent taskEvent;
    private DistLink link;
    
    public StreamEvent() {
    }
    
    public StreamEvent(final ITaskEvent te, final DistLink link) {
        this.taskEvent = te;
        this.link = link;
    }
    
    public ITaskEvent getTaskEvent() {
        return this.taskEvent;
    }
    
    public void setTaskEvent(final ITaskEvent taskEvent) {
        this.taskEvent = taskEvent;
    }
    
    public DistLink getLink() {
        return this.link;
    }
    
    public void setLink(final DistLink link) {
        this.link = link;
    }
    
    public void read(final Kryo kr, final Input input) {
        final byte hasTE = input.readByte();
        if (hasTE == 0) {
            this.taskEvent = (ITaskEvent)kr.readClassAndObject(input);
        }
        final byte hasLink = input.readByte();
        if (hasLink == 0) {
            (this.link = new DistLink()).read(kr, input);
        }
    }
    
    public void write(final Kryo kr, final Output output) {
        if (this.taskEvent != null) {
            output.writeByte(0);
            kr.writeClassAndObject(output, (Object)this.taskEvent);
        }
        else {
            output.writeByte(1);
        }
        if (this.link != null) {
            output.writeByte(0);
            this.link.write(kr, output);
        }
        else {
            output.writeByte(1);
        }
    }
    
    @Override
    public String toString() {
        return this.taskEvent + ":" + this.link;
    }
}
