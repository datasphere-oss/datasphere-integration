package com.datasphere.recovery;

import java.util.concurrent.*;
import java.util.*;

public class OutofOrderAckReassembler
{
    protected Deque<Packet> packetDeque;
    
    public OutofOrderAckReassembler() {
        this.packetDeque = new ConcurrentLinkedDeque<Packet>();
    }
    
    public void appendWrittenObject(final int seqNo, final Object object) {
        this.packetDeque.addLast(new Packet(seqNo, object));
    }
    
    public Object acknowledgeReceive(final int sequenceNo) {
        for (final Packet packet : this.packetDeque) {
            if (packet.getSequenceNo() == sequenceNo) {
                packet.setAcknowledged();
            }
        }
        Object returnValue = null;
        for (final Packet packet2 : this.packetDeque) {
            if (!packet2.isAcknowledged()) {
                break;
            }
            returnValue = packet2.getObject();
            this.packetDeque.removeFirstOccurrence(packet2);
        }
        return returnValue;
    }
    
    public int size() {
        return this.packetDeque.size();
    }
}
