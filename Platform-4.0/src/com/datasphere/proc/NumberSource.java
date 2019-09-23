package com.datasphere.proc;

import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.proc.events.ObjectArrayEvent;
import com.datasphere.recovery.NumberSourcePosition;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.components.Flow;
import com.datasphere.uuid.UUID;

@PropertyTemplate(name = "NumberSource", type = AdapterType.internal, properties = { @PropertyTemplateProperty(name = "lowValue", type = Long.class, required = true, defaultValue = "0"), @PropertyTemplateProperty(name = "highValue", type = Long.class, required = true, defaultValue = "100000"), @PropertyTemplateProperty(name = "repeat", type = Boolean.class, required = false, defaultValue = "false"), @PropertyTemplateProperty(name = "delayMillis", type = Long.class, required = false, defaultValue = "1") }, inputType = ObjectArrayEvent.class)
public class NumberSource extends SourceProcess
{
    private Logger logger;
    boolean stopped;
    long value;
    private long lowValue;
    private long highValue;
    private Boolean repeat;
    private long delayMillis;
    private boolean sendPositions;
    
    public NumberSource() {
        this.logger = Logger.getLogger((Class)NumberSource.class);
        this.stopped = false;
    }
    
    @Override
    public synchronized void init(final Map<String, Object> properties, final Map<String, Object> parserProperties, final UUID uuid, final String distributionID, final SourcePosition restartPosition, final boolean sendPositions, final Flow flow) throws Exception {
        super.init(properties, parserProperties, uuid, distributionID, restartPosition, sendPositions, flow);
        this.lowValue = this.getLongValue(properties.get("lowValue"));
        this.highValue = this.getLongValue(properties.get("highValue"));
        this.repeat = this.getBooleanValue(properties.get("repeat"));
        this.delayMillis = this.getLongValue(properties.get("delayMillis"));
        this.sendPositions = sendPositions;
        if (restartPosition instanceof NumberSourcePosition) {
            this.value = ((NumberSourcePosition)restartPosition).value;
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Set restart position from restartPosition to " + this.value));
            }
        }
        else {
            this.value = this.lowValue;
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Null restart position therefore " + this.value));
            }
        }
        this.stopped = false;
    }
    
    @Override
    public synchronized void close() throws Exception {
        super.close();
    }
    
    @Override
    public synchronized void receiveImpl(final int channel, final Event event) throws Exception {
        if (this.stopped) {
            return;
        }
        if (!this.repeat && this.value > this.highValue) {
            this.stopped = true;
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Number Source has reached the max value " + this.highValue));
            }
            return;
        }
        final ObjectArrayEvent outEvent = new ObjectArrayEvent(System.currentTimeMillis());
        (outEvent.data = new Object[2])[0] = System.currentTimeMillis();
        outEvent.data[1] = this.value;
        final Position outPos = this.sendPositions ? Position.from(this.sourceUUID, this.distributionID, (SourcePosition)new NumberSourcePosition(this.value)) : null;
        this.send((Event)outEvent, 0, outPos);
        ++this.value;
        if (this.delayMillis > 0L) {
            Thread.sleep(this.delayMillis);
        }
    }
    
    @Override
    public void setSourcePosition(final SourcePosition sourcePosition) {
        if (sourcePosition instanceof NumberSourcePosition) {
            final NumberSourcePosition lsp = (NumberSourcePosition)sourcePosition;
            this.value = lsp.value;
        }
        else {
            this.logger.warn((Object)("LongSource.setSourcePosition() called with wrong kind of SourcePosition: " + sourcePosition.getClass()));
        }
    }
    
    @Override
    public Position getCheckpoint() {
        final SourcePosition sourcePosition = (SourcePosition)new NumberSourcePosition(this.value);
        final Position result = Position.from(this.sourceUUID, this.distributionID, sourcePosition);
        if (this.logger.isDebugEnabled()) {
            this.logger.debug((Object)("NumberSource checkpoint is " + result));
        }
        return result;
    }
    
    private Boolean getBooleanValue(final Object data) {
        if (data instanceof Boolean) {
            return (Boolean)data;
        }
        return data.toString().equals("TRUE");
    }
    
    private long getLongValue(final Object data) {
        if (data instanceof Integer) {
            return (long)data;
        }
        if (data instanceof Long) {
            return (long)data;
        }
        try {
            return Long.valueOf(data.toString());
        }
        catch (NumberFormatException e) {
            this.logger.warn((Object)("LongSource cannot turn \"" + data + "\" into a Long value"), (Throwable)e);
            return 0L;
        }
    }
}
