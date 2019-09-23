package com.datasphere.persistence;

import org.apache.log4j.*;
import java.util.*;

import com.datasphere.intf.*;
import com.datasphere.runtime.*;

public class PersistenceFactory
{
    private static Logger logger;
    
    public static PersistenceLayer createPersistenceLayer(String target_database, final PersistingPurpose purpose, final String puname, final Map<String, Object> props) {
        if (target_database == null) {
            target_database = "Auto";
        }
        if (target_database.equalsIgnoreCase("sqlmx")) {
            return new HibernatePersistenceLayerImpl(puname, props);
        }
        if (target_database.equalsIgnoreCase("ONDB")) {
            return new ONDBPersistenceLayerImpl(puname, props);
        }
        if (purpose == PersistingPurpose.MONITORING) {
            return new DefaultJPAPersistenceLayerImpl(puname, props);
        }
        if (purpose == PersistingPurpose.RECOVERY) {
            return new AppCheckpointPersistenceLayer(puname, props);
        }
        if (purpose == PersistingPurpose.HDSTORE) {
            return new HStorePersistenceLayerImpl(puname, props);
        }
        return null;
    }
    
    public static PersistenceLayer createPersistenceLayerWithRetry(final String target_database, final PersistingPurpose purpose, final String puname, final Map<String, Object> props) {
        return new RetryPersistenceLayer(createPersistenceLayer(target_database, purpose, puname, props));
    }
    
    public static PersistencePolicy createPersistencePolicy(final Long intervalMicroseconds, final PersistenceLayer persistenceLayer, final BaseServer srv, final Map<String, Object> props, final HStore ws) {
        if (intervalMicroseconds == null || intervalMicroseconds < 0L) {
            return new NoPersistencePolicy();
        }
        if (intervalMicroseconds == 0L) {
            return new ImmediatePersistencePolicy(persistenceLayer, ws);
        }
        final long intervalMilliseconds = intervalMicroseconds / 1000L;
        return new PeriodicPersistencePolicy(intervalMilliseconds, persistenceLayer, srv, props, ws);
    }
    
    public static LRUList<HStore.LRUNode> createExpungeList(final String expungePolicyDescription) {
        return new LRUList<HStore.LRUNode>();
    }
    
    static {
        PersistenceFactory.logger = Logger.getLogger((Class)PersistenceFactory.class);
    }
    
    public enum PersistingPurpose
    {
        HDSTORE, 
        MONITORING, 
        RECOVERY;
    }
}
