package com.datasphere.runtime.channels;

import java.io.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.components.*;

public interface Channel extends Closeable, Monitorable
{
    void publish(final ITaskEvent p0) throws Exception;
    
    void addSubscriber(final Link p0) throws Exception;
    
    void removeSubscriber(final Link p0);
    
    void addCallback(final NewSubscriberAddedCallback p0);
    
    int getSubscribersCount();
    
    public interface NewSubscriberAddedCallback
    {
        void notifyMe(final Link p0);
    }
}
