package com.datasphere.persistence;

import org.apache.log4j.*;

import com.datasphere.hd.*;
import com.datasphere.intf.*;
import com.datasphere.runtime.components.*;
import java.util.*;

public class ImmediatePersistencePolicy implements PersistencePolicy
{
    private static Logger logger;
    private final PersistenceLayer persistenceLayer;
    private final HStore ws;
    
    public ImmediatePersistencePolicy(final PersistenceLayer persistenceLayer, final HStore ws) {
        this.persistenceLayer = persistenceLayer;
        this.ws = ws;
    }
    
    @Override
    public boolean addHD(final HD w) throws Exception {
        if (this.persistenceLayer != null) {
            final PersistenceLayer.Range[] ranges = this.persistenceLayer.persist(w);
            if (ranges == null || ranges.length == 0 || !ranges[0].isSuccessful()) {
                this.ws.notifyAppMgr(EntityType.HDSTORE, this.ws.getMetaName(), this.ws.getMetaID(), new Exception("Failed to persist hd: " + w), "Persist hd", w);
            }
            return true;
        }
        ImmediatePersistencePolicy.logger.error((Object)"Unable to persist hd immediately because there is no persistence layer");
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
        ImmediatePersistencePolicy.logger = Logger.getLogger((Class)ImmediatePersistencePolicy.class);
    }
}
