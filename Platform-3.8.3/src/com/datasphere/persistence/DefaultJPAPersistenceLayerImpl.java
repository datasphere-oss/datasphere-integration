package com.datasphere.persistence;

import org.apache.log4j.*;
import org.apache.commons.lang.*;
import javax.persistence.*;

import com.datasphere.intf.*;
import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.hd.*;

public class DefaultJPAPersistenceLayerImpl implements PersistenceLayer
{
    private static Logger logger;
    protected String persistenceUnitName;
    protected Map<String, Object> props;
    protected EntityManagerFactory factory;
    protected HStore ws;
    protected long T1;
    protected long T2;
    
    public DefaultJPAPersistenceLayerImpl() {
        this.persistenceUnitName = null;
        this.props = null;
        this.ws = null;
        this.T1 = 0L;
        this.T2 = 0L;
    }
    
    public DefaultJPAPersistenceLayerImpl(final String persistenceUnit, final Map<String, Object> properties) {
        this.persistenceUnitName = null;
        this.props = null;
        this.ws = null;
        this.T1 = 0L;
        this.T2 = 0L;
        this.persistenceUnitName = persistenceUnit;
        if (properties != null) {
            (this.props = new HashMap<String, Object>()).putAll(properties);
        }
    }
    
    public void setPersistenceUnitName(final String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
    }
    
    @Override
    public void init() {
        if (this.persistenceUnitName == null || this.persistenceUnitName.isEmpty()) {
            DefaultJPAPersistenceLayerImpl.logger.warn((Object)"provide a unique name for persistence unit and properties before calling init method");
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
        throw new NotImplementedException();
    }
    
    protected synchronized EntityManager getEntityManager() {
        try {
            EntityManager entityManager = null;
            if (this.props == null || this.props.isEmpty()) {
                entityManager = this.factory.createEntityManager();
            }
            else {
                entityManager = this.factory.createEntityManager((Map)this.props);
            }
            return entityManager;
        }
        catch (Exception ex) {
            throw new RuntimeException("Error establishing connection with persistence database for hd store: " + this.persistenceUnitName, ex);
        }
    }
    
    @Override
    public Range[] persist(final Object object) {
        if (object == null) {
            throw new IllegalArgumentException("Cannot persist null object");
        }
        if (DefaultJPAPersistenceLayerImpl.logger.isTraceEnabled()) {
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
                            DefaultJPAPersistenceLayerImpl.logger.error((Object)("Error persisting object : " + o.getClass().getCanonicalName()), (Throwable)ex);
                        }
                    }
                }
                else {
                    try {
                        entityManager.persist(object);
                    }
                    catch (Exception ex2) {
                        DefaultJPAPersistenceLayerImpl.logger.error((Object)("Error persisting object : " + object.getClass().getCanonicalName()), (Throwable)ex2);
                    }
                }
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
        if (DefaultJPAPersistenceLayerImpl.logger.isTraceEnabled()) {
            this.T2 = System.currentTimeMillis();
        }
        if (DefaultJPAPersistenceLayerImpl.logger.isTraceEnabled()) {
            DefaultJPAPersistenceLayerImpl.logger.trace((Object)("persistence unit name  = " + this.persistenceUnitName + ", no-of-inserts = " + noofinserts + " and time taken : " + (this.T2 - this.T1) + " milliseconds"));
        }
        return null;
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
    public void setStoreName(final String storeName, final String tableName) {
        this.setPersistenceUnitName(storeName);
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
            DefaultJPAPersistenceLayerImpl.logger.error((Object)("Problem running query " + query + " with params " + params), (Throwable)e);
        }
        finally {
            entityManager.clear();
            entityManager.close();
        }
        return queryResults;
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
            DefaultJPAPersistenceLayerImpl.logger.error((Object)("error executing native query : " + query));
        }
        finally {
            entityManager.clear();
            entityManager.close();
        }
        return result;
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
                DefaultJPAPersistenceLayerImpl.logger.error((Object)t.getLocalizedMessage());
                if (txn != null && txn.isActive()) {
                    txn.rollback();
                }
            }
        }
        return 0;
    }
    
    @Override
    public void close() {
        if (this.factory != null) {
            if (this.factory.isOpen()) {
                this.factory.close();
            }
            this.factory = null;
        }
        this.persistenceUnitName = null;
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for DefaultJPAPersistenceLayerImpl");
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for DefaultJPAPersistenceLayerImpl");
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
        return this.ws;
    }
    
    @Override
    public void setHDStore(final HStore ws) {
        this.ws = ws;
    }
    
    static {
        DefaultJPAPersistenceLayerImpl.logger = Logger.getLogger((Class)DefaultJPAPersistenceLayerImpl.class);
    }
}
