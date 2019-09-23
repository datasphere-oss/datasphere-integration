package com.datasphere.persistence;

import org.apache.log4j.*;

import com.datasphere.intf.*;

import org.apache.commons.lang.*;

import javax.persistence.*;
import java.util.*;

public class HiveJPAPersistenceLayer extends DefaultRuntimeJPAPersistenceLayerImpl
{
    private static Logger logger;
    
    public HiveJPAPersistenceLayer() {
    }
    
    public HiveJPAPersistenceLayer(final String persistenceUnit, final Map<String, Object> properties) {
        super(persistenceUnit, properties);
    }
    
    @Override
    public int delete(final Object object) {
        throw new NotImplementedException();
    }
    
    @Override
    public PersistenceLayer.Range[] persist(final Object object) {
        if (object == null) {
            throw new IllegalArgumentException("Cannot persist null object");
        }
        if (HiveJPAPersistenceLayer.logger.isTraceEnabled()) {
            this.T1 = System.currentTimeMillis();
        }
        int noofinserts = 1;
        EntityTransaction txn = null;
        EntityManager entityManager = null;
        synchronized (this.factory) {
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                if (object instanceof List) {
                    noofinserts = ((List)object).size();
                    for (final Object o : (List)object) {
                        try {
                            entityManager.persist(o);
                        }
                        catch (Exception ex) {
                            HiveJPAPersistenceLayer.logger.error((Object)("Error persisting object : " + o.getClass().getCanonicalName()), (Throwable)ex);
                        }
                    }
                }
                else {
                    try {
                        entityManager.persist(object);
                    }
                    catch (Exception ex2) {
                        HiveJPAPersistenceLayer.logger.error((Object)("Error persisting object : " + object.getClass().getCanonicalName()), (Throwable)ex2);
                    }
                }
                entityManager.flush();
            }
            finally {
                if (txn == null || txn.isActive()) {}
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        if (HiveJPAPersistenceLayer.logger.isTraceEnabled()) {
            this.T2 = System.currentTimeMillis();
        }
        if (HiveJPAPersistenceLayer.logger.isTraceEnabled()) {
            HiveJPAPersistenceLayer.logger.trace((Object)("persistence unit name  = " + this.persistenceUnitName + ", no-of-inserts = " + noofinserts + " and time taken : " + (this.T2 - this.T1) + " milliseconds"));
        }
        return null;
    }
    
    static {
        HiveJPAPersistenceLayer.logger = Logger.getLogger((Class)HiveJPAPersistenceLayer.class);
    }
}
