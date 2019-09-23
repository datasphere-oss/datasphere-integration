package com.datasphere.persistence;

import org.apache.log4j.*;
import javax.persistence.*;
import com.datasphere.runtime.meta.*;
import java.util.*;
import com.datasphere.kafka.*;
import com.datasphere.ser.*;

public class KWCheckpointPersistenceLayer extends DefaultJPAPersistenceLayerImpl
{
    private static Logger logger;
    
    public KWCheckpointPersistenceLayer() {
    }
    
    public KWCheckpointPersistenceLayer(final String persistenceUnit, final Map<String, Object> properties) {
        super(persistenceUnit, properties);
    }
    
    @Override
    public List<KWCheckpoint> runQuery(final String queryStr, final Map<String, Object> params, final Integer maxResults) {
        List<KWCheckpoint> queryResults = null;
        EntityManager entityManager = null;
        EntityTransaction txn = null;
        synchronized (this.factory) {
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final Query q = entityManager.createQuery(queryStr);
                if (maxResults != null && maxResults > 0) {
                    q.setMaxResults((int)maxResults);
                }
                if (params != null) {
                    for (final String key : params.keySet()) {
                        q.setParameter(key, params.get(key));
                    }
                }
                queryResults = (List<KWCheckpoint>)q.getResultList();
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
        return queryResults;
    }
    
    public int removeEntriesFromKWCheckpoint(final MetaInfo.MetaObject metaObject) {
        int noOfDeletes = 0;
        EntityManager entityManager = null;
        EntityTransaction txn = null;
        final MetaInfo.Target target = (MetaInfo.Target)metaObject;
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(target.properties);
        final String topicname = (String)localPropertyMap.get("topic");
        final String key = target.nsName + "_" + target.name + "_" + topicname + "%";
        synchronized (this.factory) {
            try {
                entityManager = this.getEntityManager();
                txn = entityManager.getTransaction();
                txn.begin();
                final String queryStr = "DELETE FROM com.datasphere.kafka.KWCheckpoint kwcp WHERE kwcp.checkpointkey like :key";
                final Query q = entityManager.createQuery(queryStr);
                q.setParameter("key", (Object)key);
                noOfDeletes = q.executeUpdate();
                txn.commit();
            }
            catch (Exception e) {
                KWCheckpointPersistenceLayer.logger.error((Object)("Problem dropping entries for componenet " + target.nsName + "." + target.name + " from KWCheckpoint table. " + e));
            }
            finally {
                if (txn != null & txn.isActive()) {
                    txn.rollback();
                }
                if (entityManager != null) {
                    entityManager.close();
                    entityManager = null;
                }
            }
        }
        return noOfDeletes;
    }
    
    public int upgradeKWCheckpointTable(final MetaInfo.MetaObject metaObject) {
        int noOfUpdates = 0;
        final MetaInfo.Target target = (MetaInfo.Target)metaObject;
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(target.properties);
        final String topicname = localPropertyMap.get("topic") + "%";
        final String selectQueryStr = "SELECT kwcp FROM com.datasphere.kafka.KWCheckpoint kwcp WHERE kwcp.checkpointkey like :topicname";
        final Map<String, Object> params = new HashMap<String, Object>();
        params.put("topicname", topicname);
        final List<KWCheckpoint> results = this.runQuery(selectQueryStr, params, null);
        if (results != null && results.size() > 0) {
            if (KWCheckpointPersistenceLayer.logger.isInfoEnabled()) {
                KWCheckpointPersistenceLayer.logger.info((Object)("Will be updating the rows respective to the topic " + topicname + "."));
            }
            synchronized (this.factory) {
                EntityTransaction txn = null;
                EntityManager entityManager = null;
                try {
                    entityManager = this.getEntityManager();
                    txn = entityManager.getTransaction();
                    txn.begin();
                    for (final KWCheckpoint kwCheckpoint : results) {
                        final String key = kwCheckpoint.checkpointkey;
                        final String newKey = target.nsName + "_" + target.name + "_" + key;
                        final String queryStr = "UPDATE com.datasphere.kafka.KWCheckpoint kwcp SET kwcp.checkpointkey=:newKey WHERE kwcp.checkpointkey= :key";
                        final Query q = entityManager.createQuery(queryStr);
                        q.setParameter("newKey", (Object)newKey);
                        q.setParameter("key", (Object)key);
                        final int updates = q.executeUpdate();
                        if (updates > 0) {
                            noOfUpdates += updates;
                            if (!KWCheckpointPersistenceLayer.logger.isInfoEnabled()) {
                                continue;
                            }
                            KWCheckpointPersistenceLayer.logger.info((Object)("Updated " + key + " to " + newKey));
                        }
                    }
                    entityManager.flush();
                    txn.commit();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    KWCheckpointPersistenceLayer.logger.error((Object)("Problem while updating entries for componenet " + target.nsName + "." + target.name + " in KWCheckpoint table. " + e));
                }
                finally {
                    if (txn != null & txn.isActive()) {
                        txn.rollback();
                    }
                    if (entityManager != null) {
                        entityManager.close();
                        entityManager = null;
                    }
                }
            }
        }
        return noOfUpdates;
    }
    
    public String getAllKWCheckpointData() {
        final String selectQueryStr = "SELECT kwcp FROM com.datasphere.kafka.KWCheckpoint kwcp";
        final List<KWCheckpoint> results = this.runQuery(selectQueryStr, null, null);
        if (results != null && results.size() > 0) {
            if (KWCheckpointPersistenceLayer.logger.isInfoEnabled()) {
                KWCheckpointPersistenceLayer.logger.info((Object)("Will be exporting " + results.size() + " entries from KWCheckpoin table."));
            }
            final StringBuilder result = new StringBuilder();
            result.append("[\n");
            boolean firstCall = true;
            for (final KWCheckpoint kwchkpt : results) {
                if (firstCall) {
                    result.append("{\n");
                    firstCall = false;
                }
                else {
                    result.append(",\n{\n");
                }
                result.append("\"checkpointkey\":\"" + kwchkpt.checkpointkey + "\",");
                final byte[] chkPtBytes = kwchkpt.checkpointdata;
                final OffsetPosition offsetPositionChkPt = (OffsetPosition)KryoSingleton.read(chkPtBytes, false);
                final String checkpointValue = offsetPositionChkPt.toJson();
                result.append("\"checkpointvalue\":{\n" + checkpointValue + "\n}");
                result.append("\n}\n");
            }
            result.append("]");
            return result.toString();
        }
        return null;
    }
    
    public void putAllKWCheckpoint(final List<KWCheckpoint> kwCheckpoints) {
        if (kwCheckpoints != null && !kwCheckpoints.isEmpty()) {
            EntityManager entityManager = null;
            EntityTransaction txn = null;
            synchronized (this.factory) {
                try {
                    entityManager = this.getEntityManager();
                    txn = entityManager.getTransaction();
                    txn.begin();
                    for (final KWCheckpoint kwChkPt : kwCheckpoints) {
                        super.persist(kwChkPt);
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
        }
    }
    
    static {
        KWCheckpointPersistenceLayer.logger = Logger.getLogger((Class)KWCheckpointPersistenceLayer.class);
    }
}
