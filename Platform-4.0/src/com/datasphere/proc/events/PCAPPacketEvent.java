package com.datasphere.proc.events;

import org.pcap4j.packet.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

@EventType(schema = "Internal", classification = "All", uri = "com.datasphere.proc.events:PCAPPacketEvent:1.0")
public class PCAPPacketEvent extends SimpleEvent
{
    private static final long serialVersionUID = -861041374336281417L;
    public long ts;
    public int tsns;
    public int len;
    @EventTypeData
    public Packet packet;
    public String linkType;
    public EthernetPacket.EthernetHeader ether;
    public String networkType;
    public IpV4Packet.IpV4Header ipv4;
    public IpV6Packet.IpV6Header ipv6;
    public ArpPacket.ArpHeader arp;
    public String transportType;
    public TcpPacket.TcpHeader tcp;
    public UdpPacket.UdpHeader udp;
    public IcmpV4CommonPacket.IcmpV4CommonHeader icmpV4;
    public IcmpV6CommonPacket.IcmpV6CommonHeader icmpV6;
    public String srcAddress;
    public String dstAddress;
    public int srcPort;
    public int dstPort;
    public String appPayload;
    
    public PCAPPacketEvent() {
        this.packet = null;
        this.linkType = null;
        this.ether = null;
        this.networkType = null;
        this.ipv4 = null;
        this.ipv6 = null;
        this.arp = null;
        this.transportType = null;
        this.tcp = null;
        this.udp = null;
        this.icmpV4 = null;
        this.icmpV6 = null;
        this.srcAddress = null;
        this.dstAddress = null;
        this.srcPort = -1;
        this.dstPort = -1;
        this.appPayload = null;
    }
    
    public PCAPPacketEvent(final long timestamp) {
        super(timestamp);
        this.packet = null;
        this.linkType = null;
        this.ether = null;
        this.networkType = null;
        this.ipv4 = null;
        this.ipv6 = null;
        this.arp = null;
        this.transportType = null;
        this.tcp = null;
        this.udp = null;
        this.icmpV4 = null;
        this.icmpV6 = null;
        this.srcAddress = null;
        this.dstAddress = null;
        this.srcPort = -1;
        this.dstPort = -1;
        this.appPayload = null;
    }
    
    public PCAPPacketEvent(final long ts, final int tsns, final int len, final Packet packet) {
        this.packet = null;
        this.linkType = null;
        this.ether = null;
        this.networkType = null;
        this.ipv4 = null;
        this.ipv6 = null;
        this.arp = null;
        this.transportType = null;
        this.tcp = null;
        this.udp = null;
        this.icmpV4 = null;
        this.icmpV6 = null;
        this.srcAddress = null;
        this.dstAddress = null;
        this.srcPort = -1;
        this.dstPort = -1;
        this.appPayload = null;
        this.ts = ts;
        this.tsns = tsns;
        this.len = len;
        if (packet instanceof EthernetPacket) {
            this.linkType = "ETHER";
            final EthernetPacket ep = (EthernetPacket)packet;
            this.ether = ep.getHeader();
            final Packet network = ep.getPayload();
            if (network instanceof IpV4Packet) {
                this.networkType = "IPV4";
                final IpV4Packet ip = (IpV4Packet)network;
                this.ipv4 = ip.getHeader();
                this.srcAddress = this.ipv4.getSrcAddr().getHostAddress();
                this.dstAddress = this.ipv4.getDstAddr().getHostAddress();
            }
            else if (network instanceof IpV6Packet) {
                this.networkType = "IPV6";
                final IpV6Packet ip2 = (IpV6Packet)network;
                this.ipv6 = ip2.getHeader();
                this.srcAddress = this.ipv6.getSrcAddr().getHostAddress();
                this.dstAddress = this.ipv6.getDstAddr().getHostAddress();
            }
            else if (network instanceof ArpPacket) {
                this.networkType = "ARP";
                final ArpPacket ip3 = (ArpPacket)network;
                this.arp = ip3.getHeader();
                this.srcAddress = this.arp.getSrcProtocolAddr().getHostAddress();
                this.dstAddress = this.arp.getDstProtocolAddr().getHostAddress();
            }
            else {
                this.networkType = "UNKNOWN";
            }
            if (network != null) {
                final Packet transport = network.getPayload();
                if (transport instanceof TcpPacket) {
                    this.transportType = "TCP";
                    final TcpPacket t = (TcpPacket)transport;
                    this.tcp = t.getHeader();
                    this.srcPort = this.tcp.getSrcPort().valueAsInt();
                    this.dstPort = this.tcp.getDstPort().valueAsInt();
                }
                else if (transport instanceof UdpPacket) {
                    this.transportType = "UDP";
                    final UdpPacket t2 = (UdpPacket)transport;
                    this.udp = t2.getHeader();
                    this.srcPort = this.udp.getSrcPort().valueAsInt();
                    this.dstPort = this.udp.getDstPort().valueAsInt();
                }
                else if (transport instanceof IcmpV4CommonPacket) {
                    this.transportType = "ICMPV4";
                    final IcmpV4CommonPacket t3 = (IcmpV4CommonPacket)transport;
                    this.icmpV4 = t3.getHeader();
                }
                else if (transport instanceof IcmpV6CommonPacket) {
                    this.transportType = "ICMPV6";
                    final IcmpV6CommonPacket t4 = (IcmpV6CommonPacket)transport;
                    this.icmpV6 = t4.getHeader();
                }
                else if (transport == null) {
                    this.transportType = network.getClass().getSimpleName();
                }
                else {
                    this.transportType = transport.getClass().getSimpleName();
                }
                if (transport != null && transport.getPayload() != null) {
                    this.appPayload = new String(transport.getPayload().getRawData());
                }
            }
        }
        else {
            this.linkType = "UNKNOWN";
            this.packet = packet;
        }
    }
    
    public void setPayload(final Object[] payload) {
        this.ts = (long)payload[0];
        this.tsns = (int)payload[1];
        this.len = (int)payload[2];
        this.packet = (Packet)payload[3];
    }
    
    public Object[] getPayload() {
        return new Object[] { this.ts, this.tsns, this.len, this.packet };
    }
    
    public void write(final Kryo kryo, final Output output) {
        super.write(kryo, output);
        output.writeLong(this.ts);
        output.writeInt(this.tsns);
        output.writeInt(this.len);
        kryo.writeClassAndObject(output, (Object)this.packet);
    }
    
    public void read(final Kryo kryo, final Input input) {
        super.read(kryo, input);
        this.ts = input.readLong();
        this.tsns = input.readInt();
        this.len = input.readInt();
        this.packet = (Packet)kryo.readClassAndObject(input);
    }
}
