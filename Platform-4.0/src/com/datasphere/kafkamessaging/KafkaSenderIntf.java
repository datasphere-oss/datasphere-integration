package com.datasphere.kafkamessaging;

import com.datasphere.runtime.containers.*;
import com.datasphere.recovery.*;
import java.util.*;

public interface KafkaSenderIntf
{
    void stop();
    
    boolean send(final ITaskEvent p0) throws Exception;
    
    Position getComponentCheckpoint();
    
    void close();
    
    Map getStats();
}
