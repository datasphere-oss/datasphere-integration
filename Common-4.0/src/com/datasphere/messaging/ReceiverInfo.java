package com.datasphere.messaging;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class ReceiverInfo implements Serializable, KryoSerializable
{
    private String name;
    private String host;
    private Address address;
    
    public ReceiverInfo() {
    }
    
    public ReceiverInfo(final String name, final String host) {
        this.name = name;
        this.host = host;
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public Address getAddress() {
        return this.address;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public void setAddress(final Address address) {
        this.address = address;
    }
    
    public String getTcpURI() {
        return this.address.getTcp();
    }
    
    public String getIpcURI() {
        return this.address.getIpc();
    }
    
    public String getInprocURI() {
        return this.address.getInproc();
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeString(this.name);
        output.writeString(this.host);
        kryo.writeClassAndObject(output, (Object)this.address);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.name = input.readString();
        this.host = input.readString();
        this.address = (Address)kryo.readClassAndObject(input);
    }
}
