package com.datasphere.jmqmessaging;

import java.util.Map;

import org.zeromq.ZContext;

import com.datasphere.messaging.Address;
import com.datasphere.messaging.Handler;
import com.datasphere.messaging.Receiver;
import com.datasphere.uuid.UUID;

public abstract class ZMQReceiver implements Receiver
{
    private final ZContext ctx;
    private final Handler rcvr;
    private final String name;
    private Address address;
    private String ipAddress;
    private UUID serverID;
    private int tcpPort;
    private int ipcPort;
    protected final boolean isEncrypted;
    
    public ZMQReceiver(final ZContext ctx, final Handler rcvr, final String name, final boolean encrypted) {
        this.address = null;
        this.tcpPort = -1;
        this.ipcPort = -1;
        this.ctx = ctx;
        this.rcvr = rcvr;
        this.name = name;
        this.isEncrypted = encrypted;
    }
    
    public void setIpAddress(final String ip) {
        this.ipAddress = ip;
    }
    
    public void setServerId(final UUID sId) {
        this.serverID = sId;
    }
    
    @Override
    public abstract void start(final Map<Object, Object> p0) throws Exception;
    
    @Override
    public abstract boolean stop() throws Exception;
    
    public int getTcpPort() {
        return this.tcpPort;
    }
    
    public void setTcpPort(final int port) {
        this.tcpPort = port;
    }
    
    public int getIpcPort() {
        return this.ipcPort;
    }
    
    public void setIpcPort(final int ipcPort) {
        this.ipcPort = ipcPort;
    }
    
    public String getName() {
        return this.name;
    }
    
    public Handler getRcvr() {
        return this.rcvr;
    }
    
    public ZContext getCtx() {
        return this.ctx;
    }
    
    public String getTcpAddress() {
        return this.address.getTcp();
    }
    
    public String getIpcAddress() {
        return this.address.getIpc();
    }
    
    public String getInProcAddress() {
        return this.address.getInproc();
    }
    
    public Address getAddress() {
        return this.address;
    }
    
    public Address makeAddress() {
        final String tcpAddress = "tcp://" + this.ipAddress + ":" + ((this.getTcpPort() == -1) ? "*" : this.getTcpPort());
        final String ipcAddress = "ipc://" + this.serverID + ":" + this.getName() + ":" + ((this.getIpcPort() == -1) ? "*" : this.getIpcPort());
        final String inprocAddress = "inproc://" + this.serverID + ":" + this.getName();
        return this.address = new Address(this.getName(), tcpAddress, ipcAddress, inprocAddress);
    }
}
