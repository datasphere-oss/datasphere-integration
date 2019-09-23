package com.datasphere.proc.events;

import java.util.*;
import java.text.*;

import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class AlertEvent extends SimpleEvent
{
    private static final long serialVersionUID = -1211231231L;
    public String name;
    public String keyVal;
    public String severity;
    public String flag;
    public String message;
    private String channel;
    private Severity sevVal;
    private alertFlag flagVal;
    
    public AlertEvent() {
    }
    
    public AlertEvent(final long timestamp) {
        super(timestamp);
    }
    
    public void validate() {
        this.sevVal = Severity.valueOf(this.severity);
        this.flagVal = alertFlag.valueOf(this.flag);
    }
    
    public Severity severity() {
        return this.sevVal;
    }
    
    public alertFlag flag() {
        return this.flagVal;
    }
    
    public AlertEvent(final String name, final String keyVal, final String severity, final String flag, final String message) {
        super(System.currentTimeMillis());
        this.name = name;
        this.keyVal = keyVal;
        this.severity = severity;
        this.flag = flag;
        this.message = message;
        this.validate();
    }
    
    public void setChannel(final String channel) {
        this.channel = channel;
    }
    
    public String getChannel() {
        return this.channel;
    }
    
    public String getTimeStampInDateFormat() {
        final Date date = new Date(this.timeStamp);
        final Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(date).toString();
    }
    
    public String toString() {
        return this.getTimeStampInDateFormat() + " - " + this.message;
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeString(this.name);
        output.writeString(this.keyVal);
        output.writeString(this.severity);
        output.writeString(this.flag);
        output.writeString(this.message);
        output.writeString(this.channel);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.name = input.readString();
        this.keyVal = input.readString();
        this.severity = input.readString();
        this.flag = input.readString();
        this.message = input.readString();
        this.channel = input.readString();
        this.validate();
    }
    
    public Object[] getPayload() {
        final Object[] result = { this.name, this.keyVal, this.severity, this.flag, this.message };
        return result;
    }
    
    public enum Severity
    {
        info, 
        warning, 
        error;
    }
    
    public enum alertFlag
    {
        raise, 
        cancel;
    }
}
