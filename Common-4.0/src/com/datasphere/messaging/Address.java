package com.datasphere.messaging;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public class Address implements Serializable, KryoSerializable
{
    private String name;
    private String tcp;
    private String ipc;
    private String inproc;
    
    public Address() {
        this.name = null;
        this.tcp = null;
        this.ipc = null;
        this.inproc = null;
    }
    
    public Address(final String name, final String tcp, final String ipc, final String inproc) {
        this.name = null;
        this.tcp = null;
        this.ipc = null;
        this.inproc = null;
        this.name = name;
        this.tcp = tcp;
        this.ipc = ipc;
        this.inproc = inproc;
    }
    
    public String getName() {
        return this.name;
    }
    
    public String getInproc() {
        return this.inproc;
    }
    
    public String getIpc() {
        return this.ipc;
    }
    
    public String getTcp() {
        return this.tcp;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public void setTcp(final String tcp) {
        this.tcp = tcp;
    }
    
    public void setIpc(final String ipc) {
        this.ipc = ipc;
    }
    
    public void setInproc(final String inproc) {
        this.inproc = inproc;
    }
    
    @Override
    public String toString() {
        return "TCP: " + this.tcp + "\nIPC: " + this.ipc + "\nINPROC: " + this.inproc;
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeString(this.name);
        output.writeString(this.inproc);
        output.writeString(this.ipc);
        output.writeString(this.tcp);
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.name = input.readString();
        this.inproc = input.readString();
        this.ipc = input.readString();
        this.tcp = input.readString();
    }
}
