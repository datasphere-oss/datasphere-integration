package com.datasphere.usagemetrics.persistence;

import org.apache.log4j.*;
import com.datasphere.metaRepository.*;
import javax.persistence.*;
import java.util.*;

public class EntityStore
{
    public static final int PERSIST_ATTEMPTS_MAX = 3;
    private static final long PERSIST_FAILURE_PAUSE = 1000L;
    private static final Logger logger;
    private final EntityManagerFactory ENTITY_MANAGER_FACTORY;
    
    public EntityStore() {
        this.ENTITY_MANAGER_FACTORY = getEntityManagerFactory();
    }
    
    public boolean persistRow(final Object object) {
        EntityManager entityManager;
        int persistAttemptsLeft;
        boolean persistSucceeded;
        for (entityManager = this.ENTITY_MANAGER_FACTORY.createEntityManager(), persistAttemptsLeft = 3, persistSucceeded = false; persistAttemptsLeft > 0 && !persistSucceeded; --persistAttemptsLeft, persistSucceeded = persistRow(object, entityManager, persistAttemptsLeft)) {}
        entityManager.close();
        return persistSucceeded;
    }
    
    private static boolean persistRow(final Object object, final EntityManager entityManager, final int attemptsLeft) {
        final EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        try {
            entityManager.persist(object);
            transaction.commit();
            return true;
        }
        catch (RuntimeException persistenceException) {
            safeRollback(transaction);
            pauseOnError();
            return false;
        }
    }
    
    private static void safeRollback(final EntityTransaction transaction) {
        try {
            transaction.rollback();
        }
        catch (RuntimeException ex) {}
    }
    
    private static void pauseOnError() {
        try {
            Thread.sleep(1000L);
        }
        catch (InterruptedException ex) {}
    }
    
    private static void logPersistenceWarning(final RuntimeException persistenceException, final int attemptsLeft) {
        final Priority priority = (Priority)((attemptsLeft > 0) ? Level.DEBUG : Level.WARN);
        final long timerSeconds = 60L;
        final String warningMessage = String.format("Failed to persist usage metrics after %d attempts. Will retry in %d seconds.", 3, timerSeconds);
        EntityStore.logger.log(priority, (Object)warningMessage, (Throwable)persistenceException);
    }
    
    private static EntityManagerFactory getEntityManagerFactory() {
        if (MetaDataDBOps.DBLocation == null) {
            return Persistence.createEntityManagerFactory("UsageMetrics");
        }
        final EntityManager entityManager = MetaDataDBOps.getManager();
        final Map<String, Object> entityProperties = getEntityProperties(entityManager);
        return Persistence.createEntityManagerFactory("UsageMetrics", (Map)entityProperties);
    }
    
    private static Map<String, Object> getEntityProperties(final EntityManager entityManager) {
        final Map<String, Object> globalProperties = (Map<String, Object>)entityManager.getProperties();
        final Map<String, Object> entityProperties = new HashMap<String, Object>(globalProperties.size());
        addGlobalProperties(entityProperties, globalProperties);
        addCustomProperties(entityProperties);
        return entityProperties;
    }
    
    private static void addGlobalProperties(final Map<String, Object> factoryProperties, final Map<String, Object> globalProperties) {
        for (final Map.Entry<String, Object> entry : globalProperties.entrySet()) {
            final String key = entry.getKey();
            if (!key.startsWith("eclipselink.metadata-source")) {
                factoryProperties.put(key, entry.getValue());
            }
        }
    }
    
    private static void addCustomProperties(final Map<String, Object> newProperties) {
        newProperties.put("eclipselink.ddl-generation", "create-tables");
        newProperties.put("eclipselink.ddl-generation.output-mode", "database");
    }
    
    static {
        logger = Logger.getLogger((Class)EntityStore.class);
    }
}
