package com.datasphere.persistence;

import org.apache.log4j.*;

import com.datasphere.intf.*;
import com.datasphere.hd.*;
import java.util.*;

public class NoPersistencePolicy implements PersistencePolicy
{
    private static Logger logger;
    
    @Override
    public boolean addHD(final HD w) {
        if (NoPersistencePolicy.logger.isDebugEnabled()) {
            NoPersistencePolicy.logger.debug((Object)("NoPersistencePolicy ignoring hd: " + w));
        }
        return false;
    }
    
    @Override
    public Set<HD> getUnpersistedHDs() {
        return null;
    }
    
    @Override
    public void flush() {
    }
    
    @Override
    public void close() {
    }
    
    static {
        NoPersistencePolicy.logger = Logger.getLogger((Class)NoPersistencePolicy.class);
    }
}
