package com.datasphere.metaRepository;

import java.io.*;
import java.net.*;

public class SimpleHZMemberAddressProvider implements HazelcastMemberAddressProvider, Serializable
{
    private final String publicAddress;
    private final String bindAddress;
    private final int port;
    
    public SimpleHZMemberAddressProvider(final String publicAddress, final String bindAddress, final int port) {
        this.publicAddress = publicAddress;
        this.bindAddress = bindAddress;
        this.port = port;
    }
    
    @Override
    public InetSocketAddress getBindAddress() {
        if (this.bindAddress != null) {
            return new InetSocketAddress(this.bindAddress, this.port);
        }
        return null;
    }
    
    @Override
    public InetSocketAddress getPublicAddress() {
        if (this.publicAddress != null) {
            return new InetSocketAddress(this.publicAddress, this.port);
        }
        return null;
    }
}
