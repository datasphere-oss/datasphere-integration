package com.datasphere.recovery;

public class Packet
{
    private final int sequenceNo;
    private final Object object;
    private boolean isAcknowledged;
    
    public Packet(final int seq, final Object obj) {
        this.sequenceNo = seq;
        this.object = obj;
        this.isAcknowledged = false;
    }
    
    public synchronized void setAcknowledged() {
        this.isAcknowledged = true;
    }
    
    public synchronized boolean isAcknowledged() {
        return this.isAcknowledged;
    }
    
    public synchronized int getSequenceNo() {
        return this.sequenceNo;
    }
    
    public synchronized Object getObject() {
        return this.object;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        final Packet passedPacket = (Packet)obj;
        return this.sequenceNo == passedPacket.sequenceNo && (this.object == passedPacket.object || (this.object != null && this.object.equals(passedPacket.object)));
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int returnValue = 1;
        returnValue = 31 * returnValue + this.sequenceNo;
        returnValue = 31 * returnValue + ((this.object == null) ? 0 : this.object.hashCode());
        return returnValue;
    }
    
    @Override
    public String toString() {
        return "Packet: {" + this.sequenceNo + " , " + this.object.toString() + "" + this.isAcknowledged + "}";
    }
}
