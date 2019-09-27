package com.datasphere.source.lib.reader;

public class DataPacket
{
    private Object data;
    private String id;
    
    public DataPacket(final Object buffer, final String identifier) {
        this.data = buffer;
        this.id = identifier;
    }
    
    public Object data() {
        return this.data;
    }
    
    public void data(final Object buffer) {
        this.data = buffer;
    }
    
    public String id() {
        return this.id;
    }
    
    public void id(final String identifier) {
        this.id = identifier;
    }
}
