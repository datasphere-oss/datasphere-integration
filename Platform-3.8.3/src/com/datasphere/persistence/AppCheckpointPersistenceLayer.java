package com.datasphere.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.datasphere.intf.PersistenceLayer;
import com.datasphere.recovery.AppCheckpointSummary;
import com.datasphere.recovery.CheckpointPath;
import com.datasphere.recovery.PendingAppCheckpointSummary;
import com.datasphere.recovery.PendingCheckpointPath;
import com.datasphere.recovery.Position;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HD;
import com.datasphere.hd.HDKey;

public class AppCheckpointPersistenceLayer implements PersistenceLayer
{
    private static Logger logger;
    private EntityManagerFactory factory;
    private Map<String, Object> props;
    private final String persistenceUnitName;
    
    public AppCheckpointPersistenceLayer(final String persistenceUnit, final Map<String, Object> properties) {
        this.props = new HashMap<String, Object>();
        this.persistenceUnitName = persistenceUnit;
        if (properties != null) {
            (this.props = new HashMap<String, Object>()).putAll(properties);
        }
    }
    
    @Override
    public void init() {
        if (this.persistenceUnitName == null || this.persistenceUnitName.isEmpty()) {
            AppCheckpointPersistenceLayer.logger.warn((Object)"provide a unique name for persistence unit and properties before calling init method");
            return;
        }
        synchronized (this) {
            if (this.factory == null || !this.factory.isOpen()) {
                if (this.props == null || this.props.isEmpty()) {
                    this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName);
                }
                else {
                    this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName, (Map)this.props);
                }
            }
        }
    }
    
    @Override
    public void init(final String name) {
        this.init();
    }
    
    @Override
    public int delete(final Object object) {
        int noofdeletes = 0;
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                if (object == null) {
                    AppCheckpointPersistenceLayer.logger.warn((Object)"Got null object to delete so, simply skip everything from here");
                    return -1;
                }
                if (!(object instanceof List)) {
                    AppCheckpointPersistenceLayer.logger.warn((Object)("AppCheckpointPersistenceLayer.delete() expects a List<CheckpointPath> but received " + object.getClass().getCanonicalName()));
                    return -1;
                }
                final List<?> untypedList = (List<?>)object;
                final List<CheckpointPath> appCheckpoints = new ArrayList<CheckpointPath>();
                for (final Object item : untypedList) {
                    if (!(item instanceof CheckpointPath)) {
                        AppCheckpointPersistenceLayer.logger.warn((Object)"AppCheckpointPersistenceLayer.delete() received a list with items other than AppCheckpoints");
                        return -1;
                    }
                    appCheckpoints.add((CheckpointPath)item);
                }
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final String query = "DELETE FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid AND c.pathItems=:pathItems";
                for (final CheckpointPath appCheckpoint : appCheckpoints) {
                    final Map<String, Object> params = new HashMap<String, Object>();
                    final Query q = entityManager.createQuery(query);
                    q.setParameter("flowUuid", (Object)appCheckpoint.flowUuid);
                    q.setParameter("pathItems", (Object)appCheckpoint.pathItems.toString());
                    q.executeUpdate();
                    ++noofdeletes;
                }
                txn.commit();
                if (AppCheckpointPersistenceLayer.logger.isTraceEnabled()) {
                    AppCheckpointPersistenceLayer.logger.trace((Object)("CheckpointPath deleted from disk: " + noofdeletes));
                }
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        return noofdeletes;
    }
    
    @Override
    public Range[] persist(final Object object) {
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
            Logger.getLogger("Recovery").trace((Object)("AppCheckpointPersistenceLayer.persist(" + object + ")"));
        }
        synchronized (this.factory) {
            final EntityTransaction txn = null;
            final EntityManager entityManager = null;
            assert object != null;
            assert object instanceof List;
            final List<?> untypedList = (List<?>)object;
            assert untypedList.size() > 0;
            if (untypedList.get(0) instanceof CheckpointPath) {
                final List<CheckpointPath> appCheckpoints = new ArrayList<CheckpointPath>();
                for (final Object item : untypedList) {
                    assert item instanceof CheckpointPath;
                    appCheckpoints.add((CheckpointPath)item);
                }
                return this.persistCheckpointPaths(appCheckpoints);
            }
            if (untypedList.get(0) instanceof PendingCheckpointPath) {
                final List<PendingCheckpointPath> appCheckpoints2 = new ArrayList<PendingCheckpointPath>();
                for (final Object item : untypedList) {
                    assert item instanceof PendingCheckpointPath;
                    appCheckpoints2.add((PendingCheckpointPath)item);
                }
                return this.persistPendingCheckpointPaths(appCheckpoints2);
            }
            if (untypedList.get(0) instanceof AppCheckpointSummary) {
                final List<AppCheckpointSummary> appCheckpoints3 = new ArrayList<AppCheckpointSummary>();
                for (final Object item : untypedList) {
                    assert item instanceof AppCheckpointSummary;
                    appCheckpoints3.add((AppCheckpointSummary)item);
                }
                return this.persistAppCheckpointSummaries(appCheckpoints3);
            }
            if (untypedList.get(0) instanceof PendingAppCheckpointSummary) {
                final List<PendingAppCheckpointSummary> appCheckpoints4 = new ArrayList<PendingAppCheckpointSummary>();
                for (final Object item : untypedList) {
                    assert item instanceof PendingAppCheckpointSummary;
                    appCheckpoints4.add((PendingAppCheckpointSummary)item);
                }
                return this.persistPendingAppCheckpointSummaries(appCheckpoints4);
            }
            Logger.getLogger("Recovery").warn((Object)("AppCheckpointPersistenceLayer.persist(" + object + ") did not receive a list of CheckpointPaths or PendingCheckpointPath or AppCheckpointSummaries, list is of type " + untypedList.get(0).getClass()));
            return null;
        }
    }
    
    private Range[] persistCheckpointPaths(final List<CheckpointPath> appCheckpoints) {
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
            Logger.getLogger("Recovery").trace((Object)("AppCheckpointPersistenceLayer.persistCheckpointPaths(" + appCheckpoints + ")"));
        }
        int noofinserts = 0;
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final String query = "SELECT c FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid AND c.pathItems=:pathItems";
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("STARTING INSERTS: " + noofinserts));
                }
                for (final CheckpointPath appCheckpoint : appCheckpoints) {
                    final Map<String, Object> params = new HashMap<String, Object>();
                    params.put("flowUuid", appCheckpoint.flowUuid);
                    params.put("pathItems", appCheckpoint.pathItems.toString());
                    final List<?> result = this.runQuery(query, params, 1);
                    if (result == null || result.size() == 0) {
                        entityManager.persist((Object)appCheckpoint);
                        ++noofinserts;
                    }
                    else {
                        appCheckpoint.id = ((CheckpointPath)result.get(0)).id;
                        entityManager.merge((Object)appCheckpoint);
                        ++noofinserts;
                    }
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("INSERTS DONE: " + noofinserts));
                }
                txn.commit();
                if (Logger.getLogger("Recovery").isTraceEnabled()) {
                    Logger.getLogger("Recovery").trace((Object)("CheckpointPaths written to disk: " + noofinserts));
                }
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        final Range[] rangeArray = { new Range(0, noofinserts) };
        return rangeArray;
    }
    
    private Range[] persistPendingCheckpointPaths(final List<PendingCheckpointPath> appCheckpoints) {
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
            Logger.getLogger("Recovery").trace((Object)("AppCheckpointPersistenceLayer.persistPendingCheckpointPaths(" + appCheckpoints + ")"));
        }
        int noofinserts = 0;
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                for (final PendingCheckpointPath appCheckpoint : appCheckpoints) {
                    entityManager.persist((Object)appCheckpoint);
                    ++noofinserts;
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("INSERTS DONE: " + noofinserts));
                }
                txn.commit();
                if (Logger.getLogger("Recovery").isTraceEnabled()) {
                    Logger.getLogger("Recovery").trace((Object)("PendingCheckpointPaths written to disk: " + noofinserts));
                }
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        final Range[] rangeArray = { new Range(0, noofinserts) };
        return rangeArray;
    }
    
    public boolean promotePendingAppCheckpoint(final UUID appUuid, final String appUri, final long commandTimestamp) {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            Logger.getLogger("Recovery").debug((Object)"Attempting to Promote Pending App Checkpoint...");
        }
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                entityManager.createQuery("DELETE FROM AppCheckpoint c WHERE c.flowUuid=?1").setParameter(1, (Object)appUuid.getUUIDString()).executeUpdate();
                entityManager.createNativeQuery("INSERT INTO AppCheckpoint SELECT c.ID, c.flowUuid, c.pathItems, c.lowSourcePosition, c.highSourcePosition, c.atOrAfter, c.updated  FROM PendingAppCheckpoint c WHERE c.flowUuid=?1 AND c.commandTimestamp=?2").setParameter(1, (Object)appUuid.getUUIDString()).setParameter(2, (Object)commandTimestamp).executeUpdate();
                entityManager.createQuery("DELETE FROM PendingAppCheckpoint c WHERE c.flowUuid=?1 AND c.commandTimestamp>=?2").setParameter(1, (Object)appUuid.getUUIDString()).setParameter(2, (Object)commandTimestamp).executeUpdate();
                entityManager.createQuery("DELETE FROM AppCheckpointSummary c WHERE c.flowUri=?1").setParameter(1, (Object)appUri).executeUpdate();
                entityManager.createNativeQuery("INSERT INTO AppCheckpointSummary SELECT c.ID, c.flowUri, c.componentUri, c.sourceUri, c.lowSourcePosition, c.lowSourcePositionText, c.highSourcePosition, c.highSourcePositionText, c.atOrAfter, c.nodeUri, c.updated  FROM PendingAppCheckpointSummary c WHERE c.flowUri=?1 AND c.commandTimestamp=?2").setParameter(1, (Object)appUri).setParameter(2, (Object)commandTimestamp).executeUpdate();
                entityManager.createQuery("DELETE FROM PendingAppCheckpointSummary c WHERE c.flowUri=?1 AND c.commandTimestamp>=?2").setParameter(1, (Object)appUri).setParameter(2, (Object)commandTimestamp).executeUpdate();
                txn.commit();
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)"Successfully Promoted Pending App Checkpoint!");
                }
            }
            catch (Exception e) {
                AppCheckpointPersistenceLayer.logger.error((Object)("Could not promote App Checkpoint! " + e.getMessage()), (Throwable)e);
                return false;
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        return true;
    }
    
    private Range[] persistAppCheckpointSummaries(final List<AppCheckpointSummary> appCheckpoints) {
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
            Logger.getLogger("Recovery").trace((Object)("AppCheckpointPersistenceLayer.persistAppCheckpointSummaries(" + appCheckpoints + ")"));
        }
        int noofinserts = 0;
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final String query = "SELECT c FROM AppCheckpointSummary c WHERE c.flowUri=:flowUri AND c.componentUri=:componentUri AND c.sourceUri=:sourceUri";
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("STARTING INSERTS: " + noofinserts));
                }
                for (final AppCheckpointSummary appCheckpoint : appCheckpoints) {
                    final Map<String, Object> params = new HashMap<String, Object>();
                    params.put("flowUri", appCheckpoint.flowUri);
                    params.put("componentUri", appCheckpoint.componentUri);
                    params.put("sourceUri", appCheckpoint.sourceUri);
                    final List<?> result = this.runQuery(query, params, 1);
                    if (result == null || result.size() == 0) {
                        entityManager.persist((Object)appCheckpoint);
                        ++noofinserts;
                    }
                    else {
                        appCheckpoint.id = ((AppCheckpointSummary)result.get(0)).id;
                        entityManager.merge((Object)appCheckpoint);
                        ++noofinserts;
                    }
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("INSERTS DONE: " + noofinserts));
                }
                txn.commit();
                if (Logger.getLogger("Recovery").isTraceEnabled()) {
                    Logger.getLogger("Recovery").trace((Object)("CheckpointPaths written to disk: " + noofinserts));
                }
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        final Range[] rangeArray = { new Range(0, noofinserts) };
        return rangeArray;
    }
    
    private Range[] persistPendingAppCheckpointSummaries(final List<PendingAppCheckpointSummary> appCheckpoints) {
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
            Logger.getLogger("Recovery").trace((Object)("AppCheckpointPersistenceLayer.persistAppCheckpointSummaries(" + appCheckpoints + ")"));
        }
        int noofinserts = 0;
        synchronized (this.factory) {
            EntityTransaction txn = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final String query = "SELECT c FROM PendingAppCheckpointSummary c WHERE c.flowUri=:flowUri AND c.componentUri=:componentUri AND c.sourceUri=:sourceUri";
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("STARTING INSERTS: " + noofinserts));
                }
                for (final PendingAppCheckpointSummary appCheckpoint : appCheckpoints) {
                    final Map<String, Object> params = new HashMap<String, Object>();
                    params.put("flowUri", appCheckpoint.flowUri);
                    params.put("componentUri", appCheckpoint.componentUri);
                    params.put("sourceUri", appCheckpoint.sourceUri);
                    final List<?> result = this.runQuery(query, params, 1);
                    if (result == null || result.size() == 0) {
                        entityManager.persist((Object)appCheckpoint);
                        ++noofinserts;
                    }
                    else {
                        appCheckpoint.id = ((PendingAppCheckpointSummary)result.get(0)).id;
                        entityManager.merge((Object)appCheckpoint);
                        ++noofinserts;
                    }
                }
                if (Logger.getLogger("Recovery").isDebugEnabled()) {
                    Logger.getLogger("Recovery").debug((Object)("INSERTS DONE: " + noofinserts));
                }
                txn.commit();
                if (Logger.getLogger("Recovery").isTraceEnabled()) {
                    Logger.getLogger("Recovery").trace((Object)("CheckpointPaths written to disk: " + noofinserts));
                }
            }
            finally {
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.clear();
                    entityManager.close();
                }
            }
        }
        final Range[] rangeArray = { new Range(0, noofinserts) };
        return rangeArray;
    }
    
    @Override
    public void merge(final Object object) {
        if (object == null) {
            throw new IllegalArgumentException("Cannot persist null object");
        }
        EntityManager entityManager = null;
        EntityTransaction txn = null;
        try {
            entityManager = this.getEntityManager();
            txn = entityManager.getTransaction();
            txn.begin();
            entityManager.merge(object);
            txn.commit();
        }
        finally {
            if (txn != null && txn.isActive()) {
                txn.rollback();
            }
            if (entityManager != null) {
                entityManager.clear();
                entityManager.close();
            }
        }
    }
    
    @Override
    public Object get(final Class<?> objectClass, final Object objectId) {
        Object result = null;
        EntityManager entityManager = null;
        try {
            entityManager = this.getEntityManager();
            result = entityManager.find((Class)objectClass, objectId);
        }
        finally {
            entityManager.clear();
            entityManager.close();
        }
        return result;
    }
    
    @Override
    public void setStoreName(final String storeName, final String tableName) {
        AppCheckpointPersistenceLayer.logger.warn((Object)("Cannot change store name of AppChecpointPersistenceLayer from " + this.persistenceUnitName + " to " + storeName));
    }
    
    @Override
    public int executeUpdate(final String queryString, final Map<String, Object> params) {
        final EntityManager em = this.getEntityManager();
        EntityTransaction txn = null;
        synchronized (this.factory) {
            try {
                final Query query = em.createQuery(queryString);
                if (params != null) {
                    for (final String key : params.keySet()) {
                        query.setParameter(key, params.get(key));
                    }
                }
                txn = em.getTransaction();
                txn.begin();
                final int queryResults = query.executeUpdate();
                txn.commit();
                return queryResults;
            }
            catch (Throwable t) {
                AppCheckpointPersistenceLayer.logger.error((Object)t.getLocalizedMessage());
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
            }
        }
        return 0;
    }
    
    @Override
    public Object runNativeQuery(final String query) {
        EntityManager entityManager = null;
        Object result = null;
        try {
            entityManager = this.getEntityManager();
            final Query q = entityManager.createNativeQuery(query);
            result = q.getSingleResult();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            AppCheckpointPersistenceLayer.logger.error((Object)("AppCheckpointPersistenceLayer error while executing native query: " + query));
        }
        finally {
            entityManager.clear();
            entityManager.close();
        }
        return result;
    }
    
    @Override
    public List<?> runQuery(final String query, final Map<String, Object> params, final Integer maxResults) {
        List<?> queryResults = null;
        EntityManager entityManager = null;
        try {
            entityManager = this.getEntityManager();
            final Query q = entityManager.createQuery(query);
            if (maxResults != null && maxResults > 0) {
                q.setMaxResults((int)maxResults);
            }
            if (params != null) {
                for (final String key : params.keySet()) {
                    q.setParameter(key, params.get(key));
                }
            }
            queryResults = (List<?>)q.getResultList();
        }
        catch (IllegalArgumentException e) {
            AppCheckpointPersistenceLayer.logger.error((Object)e.getMessage(), (Throwable)e);
        }
        finally {
            if (entityManager != null) {
                entityManager.clear();
                entityManager.close();
            }
        }
        return queryResults;
    }
    
    @Override
    public void close() {
        if (this.factory != null) {
            if (this.factory.isOpen()) {
                this.factory.close();
            }
            this.factory = null;
        }
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for AppCheckpointPersistenceLayer");
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for AppCheckpointPersistenceLayer");
    }
    
    public synchronized EntityManager getEntityManager() {
        EntityManager entityManager;
        if (this.props == null || this.props.isEmpty()) {
            entityManager = this.factory.createEntityManager();
        }
        else {
            entityManager = this.factory.createEntityManager((Map)this.props);
        }
        return entityManager;
    }
    
    @Override
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final Map<String, Object> filter, final Set<HDKey> excludeKeys) {
        return null;
    }
    
    @Override
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final String hdKey, final Map<String, Object> filter) {
        return null;
    }
    
    @Override
    public HStore getHDStore() {
        return null;
    }
    
    @Override
    public void setHDStore(final HStore ws) {
    }
    
    static {
        AppCheckpointPersistenceLayer.logger = Logger.getLogger((Class)AppCheckpointPersistenceLayer.class);
    }
}
