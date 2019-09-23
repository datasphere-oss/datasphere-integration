package com.datasphere.hdstore.elasticsearch;

import org.apache.log4j.*;
import com.datasphere.hdstore.base.*;
import java.util.concurrent.*;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.action.admin.cluster.state.*;
import com.datasphere.metaRepository.*;
import com.hazelcast.core.*;
import com.datasphere.persistence.*;
import java.util.*;

public class IndexOperationsManager
{
    private SortedMap<String, AliasOrIndex> map_alias;
    private ConcurrentMap creation_time_of_indices_map;
    private final HDStoreManager manager;
    private static final Class<DataType> thisClass;
    private static final Logger logger;
    private static final String ROLLINGINDEXLOCK = "RollingIndexLock";
    
    public IndexOperationsManager(final HDStoreManagerBase manager) {
        this.map_alias = null;
        this.creation_time_of_indices_map = new ConcurrentHashMap();
        this.manager = (HDStoreManager)manager;
    }
    
    public long getCreationTimeOfIndexLocally(final String indexName) {
        return this.getCreationTimeOfIndicesMap().get(indexName);
    }
    
    public synchronized ConcurrentMap<String, Long> getCreationTimeOfIndicesMap() {
        return (ConcurrentMap<String, Long>)this.creation_time_of_indices_map;
    }
    
    public synchronized void updateLocalAliasMap() {
        this.map_alias = this.getAliasMapFromES();
    }
    
    public SortedMap<String, AliasOrIndex> getAliasMapLocally() {
        return this.map_alias;
    }
    
    public void addAliasToIndex(final String indexName, final String aliasName) {
        this.manager.getClient().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
    }
    
    public void removeAliasFromIndex(final String indexName, final String aliasName) {
        this.manager.getClient().admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute().actionGet();
    }
    
    public long getDbExpireTime(final Map<String, Object> properties) {
        final String TTL = (String)properties.get("hd_elasticsearch.time_to_live");
        final String[] part = TTL.split("(?<=\\d)(?=\\D)");
        long time = Integer.parseInt(part[0]);
        if (part.length > 1) {
            final String s;
            final String count = s = part[1];
            switch (s) {
                case "ms": {
                    time = time;
                    break;
                }
                case "s": {
                    time *= 1000L;
                    break;
                }
                case "m": {
                    time = time * 1000L * 60L;
                    break;
                }
                case "h": {
                    time = time * 1000L * 60L * 60L;
                    break;
                }
                case "d": {
                    time = time * 1000L * 60L * 60L * 24L;
                    break;
                }
                case "w": {
                    time = time * 1000L * 60L * 60L * 24L * 7L;
                    break;
                }
            }
        }
        return time;
    }
    
    public boolean checkIfTTLForIndexExpired(final String indexName, final Map<String, Object> properties) {
        final long created_time = this.getCreationTimeOfIndexLocally(indexName);
        final long now = System.currentTimeMillis();
        final long diff = now - created_time;
        return diff * 2L >= this.getDbExpireTime(properties);
    }
    
    public String getMostOldIndex(final List<String> index_list) {
        if (index_list.size() == 1) {
            return index_list.get(0);
        }
        int j = 0;
        for (int i = 1; i < index_list.size(); ++i) {
            if (this.getCreationTimeOfIndexLocally(index_list.get(i)) - this.getCreationTimeOfIndexLocally(index_list.get(j)) < 0L) {
                j = i;
            }
        }
        return index_list.get(j);
    }
    
    public String getMostRecentIndex(final List<String> index_list) {
        if (index_list.size() == 1) {
            return index_list.get(0);
        }
        int j = 0;
        for (int i = 1; i < index_list.size(); ++i) {
            if (this.getCreationTimeOfIndexLocally(index_list.get(i)) - this.getCreationTimeOfIndexLocally(index_list.get(j)) > 0L) {
                j = i;
            }
        }
        return index_list.get(j);
    }
    
    public synchronized void populateIndexCreationTimeMap(final SortedMap<String, AliasOrIndex> map_alias) {
        this.creation_time_of_indices_map.clear();
        for (final String alias : map_alias.keySet()) {
            final AliasOrIndex aliasOrIndex = map_alias.get(alias);
            for (final IndexMetaData index : aliasOrIndex.getIndices()) {
                final long creation_time_of_index = index.getCreationDate();
                this.creation_time_of_indices_map.putIfAbsent(index.getIndex().getName(), creation_time_of_index);
            }
        }
    }
    
    public long getCreationTimeOfIndexFromES(final String indexName) {
        return ((ClusterStateResponse)this.manager.getClient().admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().index(indexName).getCreationDate();
    }
    
    public List<String> getIndicesFromAliasName(final String aliasName, final SortedMap<String, AliasOrIndex> map_alias) {
        final List<String> index_list = new ArrayList<String>();
        for (final String alias_target : map_alias.keySet()) {
            if (alias_target.equalsIgnoreCase(aliasName)) {
                final AliasOrIndex aliasOrIndex = map_alias.get(alias_target);
                final Iterator<IndexMetaData> itr_index = aliasOrIndex.getIndices().iterator();
                while (itr_index.hasNext()) {
                    index_list.add(itr_index.next().getIndex().getName());
                }
            }
        }
        return index_list;
    }
    
    public SortedMap<String, AliasOrIndex> getAliasMapFromES() {
        return (SortedMap<String, AliasOrIndex>)((ClusterStateResponse)this.manager.getClient().admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().getAliasAndIndexLookup();
    }
    
    public void checkForRollingIndex(final HDStore hdStore) {
        if (hdStore.getProperties().containsKey("hd_elasticsearch.time_to_live")) {
            final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
            final String actualName = hdStore.getActualName();
            ILock rollingIndexLock = null;
            String indexName = hdStore.getIndexName();
            if (this.checkIfTTLForIndexExpired(indexName, hdStore.getProperties())) {
                try {
                    rollingIndexLock = HazelcastSingleton.get().getLock("RollingIndexLock");
                    rollingIndexLock.lock();
                    this.updateLocalAliasMap();
                    final SortedMap<String, AliasOrIndex> map_alias = this.getAliasMapLocally();
                    this.populateIndexCreationTimeMap(map_alias);
                    final List<String> index_list_search = this.getIndicesFromAliasName("search_alias_" + actualName, map_alias);
                    indexName = this.getMostRecentIndex(index_list_search);
                    if (this.checkIfTTLForIndexExpired(indexName, hdStore.getProperties())) {
                        final boolean orphanIndexExists = this.checkForAnyOrphanIndexesAndClean(actualName, manager);
                        if (orphanIndexExists) {
                            IndexOperationsManager.logger.warn((Object)"There was an orphan index which just got cleaned up as part of cleaning process - White Blood Cell code of HD Store");
                        }
                        final String oldIndexName = this.getMostOldIndex(index_list_search);
                        this.performRollingIndex(hdStore, oldIndexName, map_alias);
                    }
                }
                finally {
                    if (rollingIndexLock != null) {
                        rollingIndexLock.unlock();
                    }
                }
            }
        }
    }
    
    private void performRollingIndex(final HDStore hdStore, final String oldIndexName, final SortedMap<String, AliasOrIndex> map_alias) {
        final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
        final String actualName = hdStore.getActualName();
        final int indices_count = this.getIndicesFromAliasName("search_alias_" + actualName, map_alias).size();
        final long now = System.currentTimeMillis();
        final String newIndexName = actualName + "%" + Long.toString(now);
        hdStore.setIndexName(newIndexName);
        final Map<String, Object> properties = hdStore.getProperties();
        if (properties != null && properties.containsKey("hd_elasticsearch.time_to_live")) {
            properties.put("elasticsearch.time_to_live", properties.get("hd_elasticsearch.time_to_live"));
            properties.remove("hd_elasticsearch.time_to_live");
        }
        if (indices_count > 2) {
            this.removeAliasFromIndex(oldIndexName, "search_alias_" + actualName);
            manager.remove(oldIndexName);
        }
        manager.create(newIndexName, properties);
        hdStore.setDataType(hdStore.getDataTypeName(), hdStore.getDataTypeSchema(), null);
    }
    
    private boolean checkForAnyOrphanIndexesAndClean(final String actualName, final HDStoreManager manager) {
        final String[] indices = manager.getNames();
        final List<String> index_list = new ArrayList<String>(Arrays.asList(indices));
        final SortedMap<String, AliasOrIndex> map_alias = this.getAliasMapLocally();
        for (final String alias : map_alias.keySet()) {
            if (!alias.startsWith("search_alias_")) {
                continue;
            }
            final AliasOrIndex aliasOrIndex = map_alias.get(alias);
            final Iterator<IndexMetaData> itr_index = aliasOrIndex.getIndices().iterator();
            while (itr_index.hasNext()) {
                final String indexName = itr_index.next().getIndex().getName();
                if (!index_list.contains(indexName)) {
                    IndexOperationsManager.logger.error((Object)"This should not happen, index list and alias map should be in sync wrt indices");
                }
                else {
                    index_list.remove(indexName);
                }
            }
        }
        for (int i = 0; i < index_list.size(); ++i) {
            final String indexName2 = manager.getActualNameWithoutTimeStamp(index_list.get(i));
            if (indexName2.equalsIgnoreCase(actualName)) {
                return manager.remove(index_list.get(i));
            }
        }
        return false;
    }
    
    static {
        thisClass = DataType.class;
        logger = Logger.getLogger((Class)IndexOperationsManager.thisClass);
    }
}
