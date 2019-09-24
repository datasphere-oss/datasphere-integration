package com.datasphere.jmqmessaging;

import java.io.*;

import com.datasphere.messaging.*;
import com.esotericsoftware.kryo.*;
/*
 * ZMQ 接收器信息
 */
public class ZMQReceiverInfo extends ReceiverInfo implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 88218429128226711L;
    
    public ZMQReceiverInfo() {
    }
    
    public ZMQReceiverInfo(final String name, final String host) {
        super(name, host);
    }
    
    public String toString() {
        return super.getName() + "'s inprocURI : " + super.getInprocURI() + "\nipcURI : " + super.getIpcURI() + "\ntcpURI : " + super.getTcpURI() + "\n--------------------------------------------\n";
    }
    
    public boolean equals(final Object object) {
        if (!(object instanceof ZMQReceiverInfo)) {
            return false;
        }
        final ZMQReceiverInfo that = (ZMQReceiverInfo)object;
        boolean ret = this.getName().equals(that.getName());
        ret &= this.getHost().equals(that.getHost());
        return ret;
    }
    
    public int hashCode() {
        int result = 31;
        result = 37 * result + ((this.getName() == null) ? 0 : this.getName().hashCode());
        result = 37 * result + ((this.getHost() == null) ? 0 : this.getHost().hashCode());
        return result;
    }
}
