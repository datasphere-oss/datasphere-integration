package com.datasphere.metaRepository;

import java.net.*;

public interface HazelcastMemberAddressProvider
{
    InetSocketAddress getBindAddress();
    
    InetSocketAddress getPublicAddress();
}
