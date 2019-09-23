package com.datasphere.persistence;

import org.apache.log4j.*;
import org.eclipse.persistence.exceptions.*;
import java.sql.*;
import javax.persistence.*;

import com.datasphere.intf.*;
import com.datasphere.recovery.*;
import java.util.*;
import com.datasphere.hd.*;

public class RetryPersistenceLayer implements PersistenceLayer
{
    private final PersistenceLayer persistenceLayer;
    private static Logger logger;
    private final int retryCount;
    private final int retryInterval;
    
    public RetryPersistenceLayer(final PersistenceLayer persistenceLayer) {
        this.persistenceLayer = persistenceLayer;
        this.retryCount = Integer.valueOf(System.getProperty("com.datasphere.config.persist.maxRetries", "0"));
        this.retryInterval = Integer.valueOf(System.getProperty("com.datasphere.config.persist.retryInterval", "10"));
    }
    
    @Override
    public void init() {
        this.persistenceLayer.init();
    }
    
    @Override
    public void init(final String storeName) {
        this.persistenceLayer.init(storeName);
    }
    
    @Override
    public int delete(final Object object) {
        return this.persistenceLayer.delete(object);
    }
    
    @Override
    public Range[] persist(final Object object) {
        int retryAttempt = 0;
        try {
            final Range[] ranges = this.persistenceLayer.persist(object);
            return ranges;
        }
        catch (Exception e) {
            RetryPersistenceLayer.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    if (retryAttempt < this.retryCount) {
                        RetryPersistenceLayer.logger.warn((Object)("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds."));
                        try {
                            Thread.sleep(this.retryInterval * 1000);
                        }
                        catch (InterruptedException e2) {
                            throw e;
                        }
                        ++retryAttempt;
                    }
                    else {
                        RetryPersistenceLayer.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error  " + e.getMessage()));
                        System.exit(1);
                    }
                }
            }
            else if (e instanceof PersistenceException) {
                if (retryAttempt < this.retryCount) {
                    RetryPersistenceLayer.logger.warn((Object)("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds."));
                    try {
                        Thread.sleep(this.retryInterval * 1000);
                    }
                    catch (InterruptedException e2) {
                        throw e;
                    }
                    ++retryAttempt;
                }
                else {
                    RetryPersistenceLayer.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error " + e.getMessage()));
                    System.exit(1);
                }
            }
            else {
                if (e instanceof RollbackException) {
                    RetryPersistenceLayer.logger.error((Object)("Rolling back attempt to write status data to table: " + e.getMessage()));
                    throw e;
                }
                RetryPersistenceLayer.logger.error((Object)("Unable to write status data to table: " + e.getMessage()));
                throw e;
            }
            return this.persistenceLayer.persist(object);
        }
    }
    
    @Override
    public void merge(final Object object) {
        this.persistenceLayer.merge(object);
    }
    
    @Override
    public void setStoreName(final String storeName, final String tableName) {
        this.persistenceLayer.setStoreName(storeName, tableName);
    }
    
    @Override
    public Object get(final Class<?> objectClass, final Object objectId) {
        return this.persistenceLayer.get(objectClass, objectId);
    }
    
    @Override
    public List<?> runQuery(final String query, final Map<String, Object> params, final Integer maxResults) {
        int retryAttempt = 0;
        try {
            final List<?> results = this.persistenceLayer.runQuery(query, params, maxResults);
            return results;
        }
        catch (Exception e) {
            RetryPersistenceLayer.logger.error((Object)("Unable to write status data to table because of a communication failure: " + e.getMessage()));
            if (e instanceof DatabaseException) {
                if (((DatabaseException)e).getDatabaseErrorCode() == 40000 && ((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException) {
                    if (retryAttempt < this.retryCount) {
                        RetryPersistenceLayer.logger.warn((Object)("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds."));
                        try {
                            Thread.sleep(this.retryInterval * 1000);
                        }
                        catch (InterruptedException e2) {
                            throw e;
                        }
                        ++retryAttempt;
                    }
                    else {
                        RetryPersistenceLayer.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error  " + e.getMessage()));
                        System.exit(1);
                    }
                }
            }
            else if (e instanceof PersistenceException) {
                if (retryAttempt < this.retryCount) {
                    RetryPersistenceLayer.logger.warn((Object)("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds."));
                    try {
                        Thread.sleep(this.retryInterval * 1000);
                    }
                    catch (InterruptedException e2) {
                        throw e;
                    }
                    ++retryAttempt;
                }
                else {
                    RetryPersistenceLayer.logger.error((Object)("Shutting down server becasue it could not connect to dataabase with error " + e.getMessage()));
                    System.exit(1);
                }
            }
            else {
                if (e instanceof RollbackException) {
                    RetryPersistenceLayer.logger.error((Object)("Rolling back attempt to write status data to table: " + e.getMessage()));
                    throw e;
                }
                RetryPersistenceLayer.logger.error((Object)("Unable to write status data to table: " + e.getMessage()));
                throw e;
            }
            return this.persistenceLayer.runQuery(query, params, maxResults);
        }
    }
    
    @Override
    public Object runNativeQuery(final String query) {
        return this.persistenceLayer.runNativeQuery(query);
    }
    
    @Override
    public int executeUpdate(final String query, final Map<String, Object> params) {
        return this.persistenceLayer.executeUpdate(query, params);
    }
    
    @Override
    public void close() {
        this.persistenceLayer.close();
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        return this.persistenceLayer.getWSPosition(namespaceName, hdStoreName);
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        return this.persistenceLayer.clearWSPosition(namespaceName, hdStoreName);
    }
    
    @Override
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final Map<String, Object> filter, final Set<HDKey> excludeKeys) {
        return this.persistenceLayer.getResults(objectClass, filter, excludeKeys);
    }
    
    @Override
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final String hdKey, final Map<String, Object> filter) {
        return this.persistenceLayer.getResults(objectClass, hdKey, filter);
    }
    
    @Override
    public HStore getHDStore() {
        return this.persistenceLayer.getHDStore();
    }
    
    @Override
    public void setHDStore(final HStore ws) {
        this.persistenceLayer.setHDStore(ws);
    }
    
    public PersistenceLayer getPersistenceLayer() {
        return this.persistenceLayer;
    }
    
    static {
        RetryPersistenceLayer.logger = Logger.getLogger((Class)RetryPersistenceLayer.class);
    }
}
