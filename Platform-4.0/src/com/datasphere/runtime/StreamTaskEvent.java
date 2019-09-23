package com.datasphere.runtime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datasphere.proc.events.StriimParserEvent;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;

public class StreamTaskEvent extends TaskEvent
{
    private static final long serialVersionUID = 8677542861922120067L;
    DARecord xevent;
    
    public StreamTaskEvent() {
    }
    
    public StreamTaskEvent(final Object data, final Position pos) {
        if (data instanceof StriimParserEvent) {
            this.xevent = new DARecord(((StriimParserEvent)data).getData(), pos);
        }
        else {
            this.xevent = new DARecord(data, pos);
        }
    }
    
    public Batch batch() {
        return Batch.asBatch(this.xevent);
    }
    
    public void write(final Kryo kryo, final Output output) {
        this.xevent.write(kryo, output);
        if (this.isLagRecord()) {
            output.writeBoolean(true);
            final LagMarker lagMarker = this.getLagMarker();
            lagMarker.write(kryo, output);
        }
        else {
            output.writeBoolean(false);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        (this.xevent = new DARecord()).read(kryo, input);
        final boolean isLagRecord = input.readBoolean();
        if (isLagRecord) {
            final LagMarker lagMarker = new LagMarker();
            lagMarker.read(kryo, input);
            this.setLagMarker(lagMarker);
            this.setLagRecord(true);
        }
        else {
            this.setLagRecord(false);
        }
    }
}
