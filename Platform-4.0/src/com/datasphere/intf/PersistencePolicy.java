package com.datasphere.intf;

import com.datasphere.hd.*;
import java.util.*;

public interface PersistencePolicy
{
    boolean addHD(final HD p0) throws Exception;
    
    Set<HD> getUnpersistedHDs();
    
    void flush();
    
    void close();
}
