package com.datasphere.proc.events;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import org.joda.time.*;
import org.apache.mahout.math.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:WindowsLogEvent:1.0")
public class WindowsLogEvent extends SimpleEvent
{
    private static final long serialVersionUID = -8077354738580620696L;
    @EventTypeData
    public WindowsLog data;
    
    public WindowsLog getData() {
        return this.data;
    }
    
    public void setData(final WindowsLog data) {
        this.data = data;
    }
    
    public void setPayload(final Object[] payload) {
        this.data = (WindowsLog)payload[0];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.data };
    }
    
    public String toString() {
        return this.data.toString();
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, (Object)this.data);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.data = (WindowsLog)kryo.readClassAndObject(input);
    }
    
    public static class WindowsLog
    {
        public String sourceName;
        public String computerName;
        public String userSid;
        public long recordNumber;
        public DateTime timeGenerated;
        public DateTime timeWritten;
        public int eventID;
        public short eventType;
        public int eventCategory;
        public String[] stringPayload;
        
        @Override
        public String toString() {
            final StringBuffer buffer = new StringBuffer();
            if (this.sourceName != null) {
                buffer.append("SourceName : " + this.sourceName + " \n");
            }
            if (this.computerName != null) {
                buffer.append("ComputerName : " + this.computerName + " \n");
            }
            if (this.userSid != null) {
                buffer.append("UserSid : " + this.userSid + " \n");
            }
            buffer.append("RecordNumber : " + this.recordNumber + " \n");
            if (this.timeGenerated != null) {
                buffer.append("TimeGenerated : " + this.timeGenerated.toString() + " \n");
            }
            if (this.timeWritten != null) {
                buffer.append("TimeWritten : " + this.timeWritten.toString() + " \n");
            }
            buffer.append("EventID : " + this.eventID + " \n");
            buffer.append("EventType : " + this.eventType + " \n");
            buffer.append("EventCategory : " + this.eventCategory + " \n");
            if (this.stringPayload != null && this.stringPayload.length > 0) {
                buffer.append("String : " + Arrays.toString((Object[])this.stringPayload) + " \n");
            }
            return buffer.toString();
        }
    }
}
