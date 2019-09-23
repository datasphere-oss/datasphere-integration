package com.datasphere.messaging;

import java.util.*;

public interface Receiver
{
    void start(final Map<Object, Object> p0) throws Exception;
    
    boolean stop() throws Exception;
}
