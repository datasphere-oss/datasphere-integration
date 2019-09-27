package com.datasphere.source.lib.intf;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.Event;
import com.datasphere.intf.Parser;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;

public class BaseParser implements Parser, Iterator<Event>
{
    Map<String, Object> prop;
    UUID srcId;
    
    public BaseParser(final Map<String, Object> property, final UUID uuid) throws AdapterException {
        this.prop = property;
        this.srcId = uuid;
    }
    
    public Iterator<Event> parse(final InputStream in) throws Exception {
        Reader reader;
        if (!(in instanceof Reader)) {
            this.prop.put(Reader.STREAM, in);
            this.prop.put(Reader.READER_TYPE, Reader.STREAM_READER);
            reader = Reader.createInstance(new Property(this.prop));
        }
        else {
            reader = (Reader)in;
        }
        return this.parse(reader);
    }
    
    public Iterator<Event> parse(final Reader reader) throws Exception {
        return null;
    }
    
    public void close() throws Exception {
    }
    
    public boolean hasNext() {
        return false;
    }
    
    public Event next() {
        return null;
    }
    
    public void remove() {
    }
    
    public void publishMonitorEvents(final MonitorEventsCollection events) {
    }
}
