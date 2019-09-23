package com.datasphere.persistence;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Root;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.datasphere.intf.PersistenceLayer;
import com.datasphere.recovery.AbstractCheckpointPath;
import com.datasphere.recovery.CheckpointPath;
import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.hd.HD;
import com.datasphere.hdstore.Utility;

public class HStorePersistenceLayerImpl extends DefaultRuntimeJPAPersistenceLayerImpl
{
    private static Logger logger;
    private String storeName;
    private String contextTableName;
    private EntityManagerPool emPool;
    private int batchWritingSize;
    
    public HStorePersistenceLayerImpl(final String persistenceUnit, final Map<String, Object> properties) {
        super(persistenceUnit, properties);
        this.storeName = null;
        this.contextTableName = null;
        this.emPool = null;
        this.storeName = persistenceUnit;
        try {
            this.batchWritingSize = Utility.extractInt(properties.get("BATCH_WRITING_SIZE"));
        }
        catch (Exception e) {
            this.batchWritingSize = 10000;
        }
    }
    
    @Override
    protected synchronized EntityManager getEntityManager() {
        try {
            EntityManager entityManager;
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
    public int delete(final Object object) {
        throw new NotImplementedException();
    }
    
    @Override
    public PersistenceLayer.Range[] persist(final Object object) {
        int numWritten = 0;
        PersistenceLayer.Range[] successRanges = null;
        int batchsize = this.batchWritingSize;
        int numThreads = 0;
        if (object == null) {
            throw new IllegalArgumentException("Cannot persist null object");
        }
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            this.T1 = System.currentTimeMillis();
        }
        int noofinserts = 1;
        this.emPool = new EntityManagerPool(this, this.props);
        synchronized (this.factory) {
            this.factory.getCache().evictAll();
            if (object instanceof List) {
                boolean doCommit = false;
                final ArrayList<Integer> emEntries = new ArrayList<Integer>();
                noofinserts = ((List)object).size();
                int emIndex = -1;
                while ((emIndex = this.emPool.getAvailableEMEntry()) == -1) {
                    try {
                        Thread.sleep(10L);
                    }
                    catch (InterruptedException ex2) {}
                }
                emEntries.add(emIndex);
                while ((emIndex = this.emPool.getAvailableEMEntry()) != -1) {
                    emEntries.add(emIndex);
                }
                numThreads = Math.min(noofinserts / batchsize + 1, emEntries.size());
                if (emEntries.size() > numThreads) {
                    for (emIndex = emEntries.size() - 1; emIndex > numThreads - 1; --emIndex) {
                        this.emPool.releaseEmAt(emEntries.get(emIndex));
                    }
                }
                batchsize = noofinserts / numThreads;
                final PersistParallel[] ppArray = new PersistParallel[numThreads];
                final Thread[] t = new Thread[numThreads];
                successRanges = new PersistenceLayer.Range[numThreads];
                final CountDownLatch ctl = new CountDownLatch(numThreads);
                final HD w = (HD)((List)object).get(0);
                if (w.getPosition() == null) {
                    doCommit = true;
                }
                if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
                    HStorePersistenceLayerImpl.logger.debug((Object)("no of hds= " + noofinserts + ", num of parallel threads= " + numThreads + ", batchsize= " + batchsize + ", is recovery on? " + !doCommit));
                }
                for (int i = 0; i < numThreads; ++i) {
                    final int startIndex = i * batchsize;
                    int endIndex = 0;
                    if (i == numThreads - 1) {
                        endIndex = noofinserts;
                    }
                    else {
                        endIndex = (((i + 1) * batchsize > noofinserts) ? noofinserts : ((i + 1) * batchsize));
                    }
                    synchronized (object) {
                        final List subList = ((List)object).subList(startIndex, endIndex);
                        final EntityManager entityManager = this.emPool.getEmAt(emEntries.get(i));
                        entityManager.clear();
                        ppArray[i] = new PersistParallel(subList.toArray(), entityManager, emEntries.get(i), ctl, doCommit);
                        ppArray[i].subList = subList;
                        successRanges[i] = new PersistenceLayer.Range(startIndex, endIndex);
                    }
                    (t[i] = new Thread(ppArray[i], "PersistParallelThread@" + this.storeName)).start();
                }
                try {
                    ctl.await();
                }
                catch (InterruptedException e) {
                    HStorePersistenceLayerImpl.logger.warn((Object)("hd write thread was interrupted due to " + e.getLocalizedMessage()));
                }
                if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
                    HStorePersistenceLayerImpl.logger.debug((Object)"All PersistParallel threads finished processing.");
                }
                this.checkForExceptions(ppArray);
                for (int ik = 0; ik < numThreads; ++ik) {
                    if (ppArray[ik].thException == null) {
                        successRanges[ik].setSuccessful(true);
                    }
                    else if (!doCommit) {
                        break;
                    }
                }
                for (int i = 0; i < numThreads; ++i) {
                    if (ppArray[i].thException == null) {
                        if (!doCommit) {
                            putPath(ppArray[i].entityManager, ppArray[i].writeCheckpoint.toPosition());
                            ppArray[i].txn.commit();
                        }
                        this.emPool.releaseEmAt(ppArray[i].indexUsed);
                        for (int k = 0; k < ppArray[i].objList.length; ++k) {
                            final HD o = (HD)ppArray[i].objList[k];
                            o.designatePersisted();
                        }
                        numWritten += ppArray[i].objList.length;
                    }
                    else {
                        if (!doCommit) {
                            for (int ij = i; ij < numThreads; ++ij) {
                                if (!doCommit && ppArray[ij].txn != null && ppArray[ij].txn.isActive()) {
                                    ppArray[ij].txn.rollback();
                                }
                                this.emPool.releaseEmAt(ppArray[ij].indexUsed);
                            }
                            break;
                        }
                        if (ppArray[i].txn != null && ppArray[i].txn.isActive()) {
                            ppArray[i].txn.rollback();
                        }
                        this.emPool.releaseEmAt(ppArray[i].indexUsed);
                    }
                }
            }
            else {
                final int j = this.emPool.getAvailableEMEntry();
                final EntityManager em = this.emPool.getEmAt(j);
                em.clear();
                EntityTransaction txn = null;
                try {
                    txn = em.getTransaction();
                    txn.begin();
                    em.persist(object);
                    if (object instanceof HD) {
                        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
                            HStorePersistenceLayerImpl.logger.debug((Object)("Writing hd to disk: " + object));
                        }
                        final HD hdObject = (HD)object;
                        final Position position = hdObject.getPosition();
                        if (position != null) {
                            putPath(em, position);
                        }
                        hdObject.designatePersisted();
                    }
                    txn.commit();
                    ++numWritten;
                }
                catch (Exception ex) {
                    HStorePersistenceLayerImpl.logger.error((Object)("Error persisting object : " + object.getClass().getCanonicalName()), (Throwable)ex);
                    if (txn != null && txn.isActive()) {
                        txn.rollback();
                    }
                }
                this.emPool.releaseEmAt(j);
            }
            this.factory.getCache().evictAll();
        }
        this.closeEmPool();
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            this.T2 = System.currentTimeMillis();
        }
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            HStorePersistenceLayerImpl.logger.debug((Object)("ws = " + this.persistenceUnitName + ", inserts = " + numWritten + " and time : " + (this.T2 - this.T1) + " milli sec, rate(inserts per sec) : " + Math.floor(numWritten * 1000 / (this.T2 - this.T1))));
        }
        return successRanges;
    }
    
    private void checkForExceptions(final PersistParallel[] ppArray) {
        for (int ik = 0; ik < ppArray.length; ++ik) {
            if (ppArray[ik].thException != null) {
                if (HStorePersistenceLayerImpl.logger.isInfoEnabled()) {
                    HStorePersistenceLayerImpl.logger.info((Object)("Found exception : " + ppArray[ik].thException.getClass().getCanonicalName() + ". so setting isCrashed to true"));
                }
                this.ws.isCrashed = true;
                this.ws.crashCausingException = ppArray[ik].thException;
                return;
            }
        }
        this.ws.isCrashed = false;
        this.ws.crashCausingException = null;
    }
    
    private void closeEmPool() {
        if (this.emPool != null) {
            if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
                HStorePersistenceLayerImpl.logger.debug((Object)(this.emPool.numEMs + " - closing all entity managers."));
            }
            for (int i = 0; i < this.emPool.numEMs; ++i) {
                if (this.emPool.emArray[i] != null && this.emPool.emArray[i].isOpen()) {
                    this.emPool.emArray[i].clear();
                    this.emPool.emArray[i].close();
                    this.emPool.emArray[i] = null;
                }
            }
        }
        this.emPool = null;
    }
    
    @Override
    public void setStoreName(final String storeName, final String tableName) {
        this.storeName = storeName;
        this.contextTableName = tableName;
    }
    
    @Override
    public int executeUpdate(final String queryString, final Map<String, Object> params) {
        EntityManager entityManager = null;
        EntityTransaction txn = null;
        int queryResults = -1;
        synchronized (this.factory) {
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final Query query = entityManager.createNativeQuery(queryString);
                if (params != null) {
                    for (final String key : params.keySet()) {
                        query.setParameter(key, params.get(key));
                    }
                }
                entityManager.getTransaction().begin();
                queryResults = query.executeUpdate();
                txn.commit();
            }
            finally {
                if (txn.isActive()) {
                    txn.rollback();
                }
                entityManager.clear();
                entityManager.close();
            }
            return queryResults;
        }
    }
    
    @Override
    public Object runNativeQuery(final String queryStr) {
        synchronized (this.factory) {
            final EntityManager entityManager = this.getEntityManager();
            final Query query = entityManager.createNativeQuery(queryStr, (Class)HD.class);
            HD w = null;
            try {
                w = (HD)query.getSingleResult();
                final Field[] declaredFields;
                final Field[] fields = declaredFields = w.context.getClass().getDeclaredFields();
                for (final Field f : declaredFields) {
                    w.context.put(f.getName(), f.get(w.context));
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                entityManager.clear();
                entityManager.close();
            }
            return w;
        }
    }
    
    @Override
    public List<?> runQuery(final String queryString, final Map<String, Object> params, final Integer maxResults) {
        synchronized (this.factory) {
            List<?> queryResults = null;
            EntityManager entityManager = null;
            try {
                entityManager = this.getEntityManager();
                final Query query = entityManager.createQuery(queryString);
                if (maxResults != null && maxResults > 0) {
                    query.setMaxResults((int)maxResults);
                }
                if (params != null) {
                    for (final String key : params.keySet()) {
                        query.setParameter(key, params.get(key));
                    }
                }
                queryResults = (List<?>)query.getResultList();
                for (final Object oo : queryResults) {
                    if (oo instanceof HD) {
                        final HD w = (HD)oo;
                        final Field[] declaredFields;
                        final Field[] fields = declaredFields = w.context.getClass().getDeclaredFields();
                        for (final Field f : declaredFields) {
                            w.context.put(f.getName(), f.get(w.context));
                        }
                    }
                }
            }
            catch (IllegalArgumentException ex) {}
            catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            finally {
                entityManager.clear();
                entityManager.close();
            }
            return queryResults;
        }
    }
    
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final HQuery query) {
        final String tableName = this.contextTableName;
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            HStorePersistenceLayerImpl.logger.debug((Object)("trying to get results from table :" + tableName + ", and create objects of type : " + objectClass.getCanonicalName()));
        }
        String qry = null;
        synchronized (this.factory) {
            try {
                final EntityManager entityManager = this.getEntityManager();
                final String jdbcUrl = (String)entityManager.getProperties().get("JDBC_URL");
                boolean supportsArrayIn = true;
                if (jdbcUrl.toLowerCase().contains(":derby:")) {
                    supportsArrayIn = false;
                }
                if (!query.willGetLatestPerKey() || query.willGetEvents()) {
                    qry = "SELECT W FROM " + tableName + " W " + query.getWhere("W", false);
                    final Query dbQuery = entityManager.createQuery(qry);
                    return new JPAIterable<T>(entityManager, dbQuery, false, !query.willGetEvents());
                }
                if (supportsArrayIn) {
                    final String subQry = "SELECT W.partitionKey, MAX(W.hdTs) FROM " + tableName + " W " + query.getWhere("W", true) + " GROUP BY W.partitionKey";
                    qry = "SELECT * FROM " + tableName + " WA WHERE (WA.partitionKey, WA.hdTs) IN (" + subQry + ")";
                    final Query dbQuery2 = entityManager.createNativeQuery(qry, (Class)objectClass);
                    return new JPAIterable<T>(entityManager, dbQuery2, true, true);
                }
                final String subQry = "SELECT W.key, MAX(W.hdTs) FROM " + tableName + " W " + query.getWhere("W", false) + " GROUP BY W.key";
                final Query subDbQuery = entityManager.createQuery(subQry);
                final List<Object[]> subs = (List<Object[]>)subDbQuery.getResultList();
                if (subs != null && !subs.isEmpty()) {
                    final List<T> results = new ArrayList<T>();
                    final CriteriaBuilder cb = entityManager.getCriteriaBuilder();
                    final CriteriaQuery<Object> cq = (CriteriaQuery<Object>)cb.createQuery();
                    final Root<T> e = (Root<T>)cq.from((Class)objectClass);
                    cq.where((Expression)cb.equal((Expression)e.get("key"), (Expression)cb.parameter((Class)String.class, "key")));
                    cq.where((Expression)cb.equal((Expression)e.get("hdTs"), (Expression)cb.parameter((Class)Long.class, "hdTs")));
                    final Query dbQuery3 = (Query)entityManager.createQuery((CriteriaQuery)cq);
                    dbQuery3.setHint("eclipselink.fetch-group.name", (Object)"noEvents");
                    for (final Object[] sub : subs) {
                        dbQuery3.setParameter("key", sub[0]);
                        dbQuery3.setParameter("hdTs", sub[1]);
                        final List<T> res = (List<T>)dbQuery3.getResultList();
                        if (res != null && !res.isEmpty()) {
                            T latest = null;
                            for (final T w : res) {
                                if (query.matches(w)) {
                                    if (latest == null) {
                                        latest = w;
                                    }
                                    else {
                                        if (w.getUuid().compareTo(latest.getUuid()) <= 0) {
                                            continue;
                                        }
                                        latest = w;
                                    }
                                }
                            }
                            results.add(latest);
                        }
                    }
                    return results;
                }
                return Collections.emptyList();
            }
            catch (Throwable t) {
                HStorePersistenceLayerImpl.logger.error((Object)("Problem executing db query: " + qry), t);
                throw t;
            }
        }
    }
    
    public <T extends HD> Iterable<T> getResults(final Class<T> objectClass, final String hdKey, final HQuery query) {
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            HStorePersistenceLayerImpl.logger.debug((Object)("trying to get results for store :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName() + ", with key :" + hdKey));
        }
        final List<T> results = null;
        synchronized (this.factory) {
            final EntityManager entityManager = this.getEntityManager();
            final CriteriaBuilder cb = entityManager.getCriteriaBuilder();
            final CriteriaQuery<Object> cq = (CriteriaQuery<Object>)cb.createQuery();
            final Root<T> e = (Root<T>)cq.from((Class)objectClass);
            cq.where((Expression)cb.equal((Expression)e.get("mapKey"), (Expression)cb.parameter((Class)String.class, "mapKey")));
            final Query dbQuery = (Query)entityManager.createQuery((CriteriaQuery)cq);
            dbQuery.setParameter("mapKey", (Object)hdKey);
            return new JPAIterable<T>(entityManager, dbQuery, false, false);
        }
    }
    
    @Override
    public void close() {
        if (this.factory != null) {
            synchronized (this.factory) {
                this.closeEmPool();
                if (this.factory != null) {
                    if (this.factory.isOpen()) {
                        this.factory.close();
                    }
                    this.factory = null;
                    if (HStorePersistenceLayerImpl.logger.isInfoEnabled()) {
                        HStorePersistenceLayerImpl.logger.info((Object)("Closed persistence layer object for pu : " + this.persistenceUnitName));
                    }
                }
            }
        }
        this.persistenceUnitName = null;
    }
    
    private static void putPath(final EntityManager em, final Position writeCheckpoint) {
        if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
            HStorePersistenceLayerImpl.logger.debug((Object)("Persist HDStoreCheckpoint: " + writeCheckpoint));
            com.datasphere.utility.Utility.prettyPrint(writeCheckpoint);
        }
        final HashMap<String, CheckpointPath> seenPaths = new HashMap<String, CheckpointPath>();
        for (final Path path : writeCheckpoint.values()) {
            CheckpointPath checkpoint = (CheckpointPath)em.find((Class)CheckpointPath.class, (Object)AbstractCheckpointPath.getHash(path.getPathItems()));
            if (checkpoint == null) {
                if ((checkpoint = seenPaths.get(path.getPathItems().toString())) == null) {
                    checkpoint = new CheckpointPath(null, path.getPathItems(), path.getLowSourcePosition(), path.getHighSourcePosition(), "^");
                    em.persist((Object)checkpoint);
                    seenPaths.put(path.getPathItems().toString(), checkpoint);
                }
                else {
                    checkpoint.lowSourcePosition = path.getLowSourcePosition();
                    em.merge((Object)checkpoint);
                }
            }
            else {
                checkpoint.lowSourcePosition = path.getLowSourcePosition();
                em.merge((Object)checkpoint);
            }
        }
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        final String fullTableName = namespaceName + "_" + hdStoreName;
        this.init(fullTableName);
        final String query = "SELECT cp FROM HDStoreCheckpoint cp";
        final EntityManager em = this.getEntityManager();
        em.getTransaction().begin();
        final Query q = em.createQuery(query);
        em.getTransaction().commit();
        final List<?> result = (List<?>)q.getResultList();
        if (result == null) {
            return new Position();
        }
        final Set<Path> paths = new HashSet<Path>();
        for (final Object o : result) {
            final CheckpointPath hdStoreCheckpoint = (CheckpointPath)o;
            final Path path = new Path(hdStoreCheckpoint.pathItems, hdStoreCheckpoint.lowSourcePosition, hdStoreCheckpoint.highSourcePosition, hdStoreCheckpoint.atOrAfter);
            paths.add(path);
        }
        final Position hdStorePosition = new Position((Set)paths);
        return hdStorePosition;
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        final String fullTableName = namespaceName + "_" + hdStoreName;
        this.init(fullTableName);
        final String query = "DELETE FROM HDStoreCheckpoint";
        final EntityManager em = this.factory.createEntityManager((Map)this.props);
        em.getTransaction().begin();
        final Query q = em.createQuery(query);
        final int numDeleted = q.executeUpdate();
        em.getTransaction().commit();
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            Logger.getLogger("Recovery").debug((Object)("HDStore cleared " + numDeleted + " checkpoint paths"));
        }
        return true;
    }
    
    static {
        HStorePersistenceLayerImpl.logger = Logger.getLogger((Class)HStorePersistenceLayerImpl.class);
    }
    
    private class EntityManagerPool
    {
        final HStorePersistenceLayerImpl parent;
        int numEMs;
        private EntityManager[] emArray;
        private boolean[] emsAvailable;
        
        EntityManagerPool(final HStorePersistenceLayerImpl par, final Map<String, Object> p) {
            this.numEMs = 10;
            this.parent = par;
            try {
                this.numEMs = Integer.parseInt((String)p.get("MAX_THREAD_NUM"));
            }
            catch (Exception ex) {}
            this.emArray = new EntityManager[this.numEMs];
            this.emsAvailable = new boolean[this.numEMs];
            for (int i = 0; i < this.numEMs; ++i) {
                this.emsAvailable[i] = true;
            }
        }
        
        public int getAvailableEMEntry() {
            synchronized (this.parent) {
                for (int i = 0; i < this.numEMs; ++i) {
                    if (this.emsAvailable[i]) {
                        this.emsAvailable[i] = false;
                        return i;
                    }
                }
            }
            return -1;
        }
        
        public EntityManager getEmAt(final int i) {
            synchronized (this.parent) {
                if (this.emArray[i] == null) {
                    if (HStorePersistenceLayerImpl.this.props == null || HStorePersistenceLayerImpl.this.props.isEmpty()) {
                        this.emArray[i] = HStorePersistenceLayerImpl.this.factory.createEntityManager();
                    }
                    else {
                        this.emArray[i] = HStorePersistenceLayerImpl.this.factory.createEntityManager((Map)HStorePersistenceLayerImpl.this.props);
                    }
                }
            }
            return this.emArray[i];
        }
        
        public void releaseEmAt(final int i) {
            synchronized (this.parent) {
                if (this.emArray[i] != null && this.emArray[i].isOpen()) {
                    this.emArray[i].clear();
                    this.emArray[i].close();
                }
                this.emsAvailable[i] = true;
            }
        }
    }
    
    private class PersistParallel implements Runnable
    {
        public int indexUsed;
        public EntityManager entityManager;
        public EntityTransaction txn;
        Object[] objList;
        CountDownLatch cLatch;
        Exception thException;
        public final PathManager writeCheckpoint;
        public List subList;
        public boolean doCommit;
        
        public PersistParallel(final Object[] arr, final EntityManager em, final int index, final CountDownLatch ctl, final boolean commit) {
            this.indexUsed = -1;
            this.entityManager = null;
            this.txn = null;
            this.objList = null;
            this.cLatch = null;
            this.thException = null;
            this.writeCheckpoint = new PathManager();
            this.subList = null;
            this.doCommit = false;
            this.indexUsed = index;
            this.entityManager = em;
            this.objList = arr.clone();
            this.cLatch = ctl;
            this.doCommit = commit;
        }
        
        @Override
        public void run() {
            Object o = null;
            try {
                (this.txn = this.entityManager.getTransaction()).begin();
                for (int i = 0; i < this.objList.length; ++i) {
                    o = this.objList[i];
                    this.entityManager.persist(o);
                    if (o instanceof HD) {
                        if (HStorePersistenceLayerImpl.logger.isTraceEnabled()) {
                            HStorePersistenceLayerImpl.logger.trace((Object)("Writing hd to disk: " + o));
                        }
                        final HD hdObject = (HD)o;
                        this.writeCheckpoint.mergeHigherPositions(hdObject.getPosition());
                    }
                }
                if (HStorePersistenceLayerImpl.logger.isDebugEnabled()) {
                    HStorePersistenceLayerImpl.logger.debug((Object)("about to commit txn : indexUsed= " + this.indexUsed));
                }
                if (this.doCommit) {
                    this.txn.commit();
                }
            }
            catch (Exception ex) {
                HStorePersistenceLayerImpl.logger.warn((Object)("Error persisting object : " + o.getClass().getCanonicalName() + "\nReason: " + ex.getLocalizedMessage()));
                this.thException = ex;
                if (this.doCommit && this.txn != null && this.txn.isActive()) {
                    HStorePersistenceLayerImpl.logger.warn((Object)"Exception persisting. So rolling back transaction.");
                    this.txn.rollback();
                }
            }
            finally {
                this.cLatch.countDown();
            }
        }
    }
}
