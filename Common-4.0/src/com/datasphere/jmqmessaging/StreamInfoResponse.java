package com.datasphere.jmqmessaging;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.runtime.DistLink;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StreamInfoResponse implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = -4603793648273123481L;
    private UUID peerID;
    private ReceiverInfo streamReceiverInfo;
    private List<DistLink> subscriberList;
    private boolean encrypted;
    
    public StreamInfoResponse() {
        this.encrypted = false;
    }
    
    public StreamInfoResponse(final UUID peer, final ReceiverInfo stream, final List<DistLink> subList, final boolean encrypted) {
        this.encrypted = false;
        this.peerID = peer;
        this.streamReceiverInfo = stream;
        this.subscriberList = subList;
        this.encrypted = encrypted;
    }
    
    public UUID getPeerID() {
        return this.peerID;
    }
    
    public ReceiverInfo getStreamReceiverInfo() {
        return this.streamReceiverInfo;
    }
    
    public List<DistLink> getSubscriberList() {
        return this.subscriberList;
    }
    
    public boolean isEncrypted() {
        return this.encrypted;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.subscriberList != null && this.subscriberList.size() > 0) {
            if (this.subscriberList.size() == 1) {
                output.writeByte(1);
                kryo.writeObject(output, (Object)this.subscriberList.get(0));
            }
            else {
                output.writeByte(2);
                kryo.writeClassAndObject(output, (Object)this.subscriberList);
            }
        }
        else {
            output.writeByte(0);
        }
        if (this.streamReceiverInfo != null) {
            output.writeByte(0);
            kryo.writeClassAndObject(output, (Object)this.streamReceiverInfo);
        }
        else {
            output.writeByte(1);
        }
        if (this.peerID != null) {
            output.writeByte(0);
            kryo.writeClassAndObject(output, (Object)this.peerID);
        }
        else {
            output.writeByte(1);
        }
        output.writeBoolean(this.encrypted);
    }
    
    public void read(final Kryo kryo, final Input input) {
        final byte hasSubList = input.readByte();
        if (hasSubList == 0) {
            this.subscriberList = Collections.emptyList();
        }
        else if (hasSubList == 1) {
            final DistLink dl = (DistLink)kryo.readObject(input, (Class)DistLink.class);
            this.subscriberList = Collections.singletonList(dl);
        }
        else {
            final Object o = kryo.readClassAndObject(input);
            if (o instanceof List) {
                this.subscriberList = (List<DistLink>)o;
            }
        }
        final byte hasStreamInfo = input.readByte();
        if (hasStreamInfo == 0) {
            this.streamReceiverInfo = (ZMQReceiverInfo)kryo.readClassAndObject(input);
        }
        else {
            this.streamReceiverInfo = null;
        }
        final byte hasStreamUUID = input.readByte();
        if (hasStreamUUID == 0) {
            this.peerID = (UUID)kryo.readClassAndObject(input);
        }
        else {
            this.peerID = null;
        }
        this.encrypted = input.readBoolean();
    }
    
    @Override
    public String toString() {
        return "StreamInfoResponse (" + this.peerID + ", \n " + this.streamReceiverInfo.toString() + ", \n " + this.subscriberList + " )";
    }
}
