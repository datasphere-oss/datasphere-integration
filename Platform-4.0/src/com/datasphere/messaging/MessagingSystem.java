package com.datasphere.messaging;

import com.datasphere.runtime.meta.*;
import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.exceptions.*;

public interface MessagingSystem
{
    void createReceiver(final Class p0, final Handler p1, final String p2, final boolean p3, final MetaInfo.Stream p4);
    
    Receiver getReceiver(final String p0);
    
    void startReceiver(final String p0, final Map<Object, Object> p1) throws Exception;
    
    Sender getConnectionToReceiver(final UUID p0, final String p1, final SocketType p2, final boolean p3, final boolean p4) throws NodeNotFoundException, InterruptedException;
    
    boolean stopReceiver(final String p0) throws Exception;
    
    void shutdown();
    
    boolean cleanupAllReceivers();
    
    Class getReceiverClass();
}
