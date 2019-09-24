package com.datasphere.pool;

import org.apache.log4j.*;
import java.util.concurrent.locks.*;
import java.util.*;

public abstract class DefaultDBResourcePool<T> implements DBResourcePool<T>
{
    public static final Logger logger;
    protected Properties props;
    protected String drivername;
    protected String url;
    protected String username;
    protected String password;
    protected int max;
    protected int min;
    protected long idleTime;
    protected int counter;
    protected RetryPolicy retryPolicy;
    private Hashtable<T, Long> inUse;
    private Hashtable<T, Long> available;
    protected JDBCUtil.DB db;
    protected ClassLoader classLoader;
    
    public DefaultDBResourcePool() {
        this(1);
    }
    
    public DefaultDBResourcePool(final int max) {
        this.props = null;
        this.drivername = null;
        this.url = null;
        this.username = null;
        this.password = null;
        this.max = 1;
        this.min = 1;
        this.idleTime = 3600L;
        this.counter = 0;
        this.retryPolicy = null;
        this.db = null;
        this.classLoader = null;
        this.db = JDBCUtil.getHelpClass(null);
        this.max = max;
        this.retryPolicy = new RetryPolicy();
        this.inUse = new Hashtable<T, Long>();
        this.available = new Hashtable<T, Long>();
    }
    
    @Override
    public void setProperties(final Properties props) {
        this.props = props;
    }
    
    @Override
    public void setURL(final String url) {
        this.url = url;
    }
    
    @Override
    public void setUserName(final String username) {
        this.username = username;
    }
    
    @Override
    public void setPassword(final String passwd) {
        this.password = passwd;
    }
    
    @Override
    public void setDriverName(final String drivername) {
        this.drivername = drivername;
    }
    
    @Override
    public void setDriverFor(final DB_TYPE dbtype) {
        this.drivername = JDBCUtil.getDriverClassName(dbtype);
    }
    
    @Override
    public void setMaxConnections(final int max) {
        this.max = max;
    }
    
    @Override
    public void setMinConnections(final int min) {
        this.min = min;
    }
    
    @Override
    public Properties getProperties() {
        return this.props;
    }
    
    @Override
    public boolean validateProp() {
        if (this.url == null || this.url.isEmpty() || this.username == null || this.username.isEmpty() || this.password == null || this.password.isEmpty() || this.drivername == null || this.drivername.isEmpty()) {
            if (DefaultDBResourcePool.logger.isInfoEnabled()) {
                DefaultDBResourcePool.logger.info((Object)("set properties first." + this.db.getHelp()));
            }
            return true;
        }
        return false;
    }
    
    @Override
    public abstract T createResource() throws ResourceException;
    
    @Override
    public abstract boolean closeResource(final T p0) throws ResourceException;
    
    @Override
    public void setRetryPolicy(final RetryPolicy policy) {
        this.retryPolicy = policy;
    }
    
    @Override
    public synchronized int size() {
        return this.inUse.size() + this.available.size();
    }
    
    @Override
    public void setClassLoader(final ClassLoader loader) {
        this.classLoader = loader;
    }
    
    protected T createResourceWithRetry() throws ResourceException {
        if (this.retryPolicy == null) {
            return this.createResource();
        }
        final int maxRetries = this.retryPolicy.getMaxRetries();
        final int retryInterval = this.retryPolicy.getRetryInterval();
        boolean isConnected = false;
        int index = 1;
        T t1 = null;
        while (index < maxRetries + 1) {
            if (DefaultDBResourcePool.logger.isInfoEnabled()) {
                DefaultDBResourcePool.logger.info((Object)("Getting resource: " + (this.counter + 1) + ". Attempt : " + index));
            }
            try {
                t1 = this.createResource();
                isConnected = true;
            }
            catch (Exception ex) {
                if (index == maxRetries) {
                    throw new ResourceException(ex);
                }
            }
            if (isConnected) {
                break;
            }
            if (index < maxRetries) {
                this.waitBeforeRetry(retryInterval);
            }
            ++index;
        }
        return t1;
    }
    
    private void waitBeforeRetry(final int retryInterval) throws ResourceException {
        boolean stillWait = true;
        final long t1 = System.currentTimeMillis();
        while (stillWait) {
            LockSupport.parkNanos((long)(retryInterval * 0.1 * 1.0E9));
            final long t2 = System.currentTimeMillis();
            if (t2 - t1 > retryInterval * 1000) {
                stillWait = false;
            }
        }
    }
    
    private synchronized T getResourceWithoutWait() throws ResourceException {
        final long now = System.currentTimeMillis();
        T t = null;
        if (this.available.size() > 0) {
            final Enumeration<T> enmr = this.available.keys();
            if (enmr.hasMoreElements()) {
                t = enmr.nextElement();
                this.available.remove(t);
                this.inUse.put(t, now);
                if (DefaultDBResourcePool.logger.isTraceEnabled()) {
                    DefaultDBResourcePool.logger.trace((Object)("reusing existing resource: in use: " + this.inUse.size() + " , free: " + this.available.size()));
                }
            }
        }
        else if (this.inUse.size() + this.available.size() < this.max) {
            t = this.createResourceWithRetry();
            this.inUse.put(t, now);
            if (DefaultDBResourcePool.logger.isTraceEnabled()) {
                DefaultDBResourcePool.logger.trace((Object)("created a new resource. Now  in use: " + this.inUse.size() + " , free: " + this.available.size()));
            }
        }
        return t;
    }
    
    @Override
    public T getResource() throws ResourceException {
        T t1 = null;
        boolean wait = true;
        while (wait) {
            if (this.max == -1) {
                DefaultDBResourcePool.logger.warn((Object)"Pool is closed. New resource can't be requested after closing pool. Create new pool");
                return null;
            }
            t1 = this.getResourceWithoutWait();
            if (t1 == null) {
                LockSupport.parkNanos(1000000000L);
            }
            else {
                wait = false;
            }
        }
        return t1;
    }
    
    @Override
    public synchronized boolean returnResource(final T t, final boolean isBad) throws ResourceException {
        if (t == null) {
            return false;
        }
        this.inUse.remove(t);
        this.available.put(t, System.currentTimeMillis());
        if (DefaultDBResourcePool.logger.isTraceEnabled()) {
            DefaultDBResourcePool.logger.trace((Object)("Returned resource. Now use:" + this.inUse.size() + " , free: " + this.available.size()));
        }
        return true;
    }
    
    @Override
    public synchronized boolean closeAllResource(final boolean forceClose) throws ResourceException {
        if (this.max == -1) {
            DefaultDBResourcePool.logger.warn((Object)"It seems connection pool is already closed. Create a new pool if needed.");
            return true;
        }
        if (!forceClose && this.inUse.size() > 0) {
            DefaultDBResourcePool.logger.warn((Object)("There are still " + this.inUse.size() + " resources are in use. Return them before closing all Or use force close."));
            return false;
        }
        if (this.available.size() > 0) {
            final Enumeration<T> enmr = this.available.keys();
            while (enmr.hasMoreElements()) {
                final T t = enmr.nextElement();
                this.closeResource(t);
                this.available.remove(t);
            }
        }
        if (DefaultDBResourcePool.logger.isInfoEnabled()) {
            DefaultDBResourcePool.logger.info((Object)("Done closing free resources: Available resources: " + this.available.size()));
        }
        this.available.clear();
        this.available = null;
        if (this.inUse.size() > 0) {
            final Enumeration<T> enmr = this.inUse.keys();
            while (enmr.hasMoreElements()) {
                final T t = enmr.nextElement();
                this.closeResource(t);
                this.inUse.remove(t);
            }
        }
        if (DefaultDBResourcePool.logger.isInfoEnabled()) {
            DefaultDBResourcePool.logger.info((Object)("Done forced closing in use resources. In-use resources: " + this.inUse.size()));
        }
        this.inUse.clear();
        this.inUse = null;
        this.max = -1;
        return true;
    }
    
    static {
        logger = Logger.getLogger((Class)DefaultDBResourcePool.class);
    }
}
