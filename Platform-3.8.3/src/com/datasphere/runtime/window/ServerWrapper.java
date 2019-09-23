package com.datasphere.runtime.window;

import com.datasphere.runtime.channels.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.components.*;
import java.util.concurrent.*;
import com.datasphere.runtime.*;

interface ServerWrapper
{
    Channel createChannel(final FlowComponent p0);
    
    void destroyKeyFactory(final MetaInfo.Window p0) throws Exception;
    
    void subscribe(final Publisher p0, final Subscriber p1) throws Exception;
    
    void unsubscribe(final Publisher p0, final Subscriber p1) throws Exception;
    
    ScheduledExecutorService getScheduler();
    
    Server getServer();
}
