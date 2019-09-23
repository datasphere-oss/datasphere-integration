package com.datasphere.usagemetrics.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasphere.anno.EntryPoint;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.usagemetrics.cache.Cache;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class Usage
{
    private static final MDRepository metaDataRepository;
    private static final Usage instance;
    
    public static Usage getInstance() {
        return Usage.instance;
    }
    
    public static Collection<MetaInfo.MetaObject> getSources(final String namespace, final String application, final AuthToken token) throws MetaDataRepositoryException {
        if (namespace == null || application == null) {
            return getAllSources(token);
        }
        return getAllSourcesForApplication(namespace, application, token);
    }
    
    private static Collection<MetaInfo.MetaObject> getAllSources(final AuthToken token) throws MetaDataRepositoryException {
        return (Collection<MetaInfo.MetaObject>)Usage.metaDataRepository.getByEntityType(EntityType.SOURCE, token);
    }
    
    private static Collection<MetaInfo.MetaObject> getAllSourcesForApplication(final String namespace, final String application, final AuthToken token) throws MetaDataRepositoryException {
        final StreamSourceMap sources = new StreamSourceMap(token);
        final List<MetaInfo.MetaObject> objects = new ArrayList<MetaInfo.MetaObject>();
        final MetaInfo.Flow app = (MetaInfo.Flow)Usage.metaDataRepository.getMetaObjectByName(EntityType.APPLICATION, namespace, application, null, token);
        if (app != null && app.objects != null) {
            for (final Map.Entry<EntityType, LinkedHashSet<UUID>> entry : app.objects.entrySet()) {
                for (final UUID uuid : entry.getValue()) {
                    final MetaInfo.MetaObject metaObject = Usage.metaDataRepository.getMetaObjectByUUID(uuid, token);
                    if (metaObject != null) {
                        objects.add(metaObject);
                    }
                }
            }
        }
        for (final MetaInfo.MetaObject object : objects) {
            findSources(object, sources, token);
        }
        return (sources).values();
    }
    
    @EntryPoint(usedBy = 1)
    public List<UsageMetrics> getCachedClusterUsageMetrics(final AuthToken token) throws MetaDataRepositoryException {
        final Collection<MetaInfo.MetaObject> sources = getSources(null, null, token);
        return getUsageMetrics(sources);
    }
    
    @EntryPoint(usedBy = 1)
    public List<UsageMetrics> getCachedApplicationUsageMetrics(final String namespace, final String application, final AuthToken token) throws MetaDataRepositoryException {
        final Collection<MetaInfo.MetaObject> sources = getSources(namespace, application, token);
        return getUsageMetrics(sources);
    }
    
    private static void findSources(final Iterable<MetaInfo.MetaObject> objects, final StreamSourceMap sources, final AuthToken token) throws MetaDataRepositoryException {
        for (final MetaInfo.MetaObject object : objects) {
            findSources(object, sources, token);
        }
    }
    
    private static void findSources(final MetaInfo.MetaObject object, final StreamSourceMap sources, final AuthToken token) throws MetaDataRepositoryException {
        if (object == null) {
            return;
        }
        if (object.getType() == EntityType.SOURCE) {
            sources.put(object.uuid, object);
            return;
        }
        if (object.getType() == EntityType.STREAM) {
            final MetaInfo.MetaObject source = sources.findSourceByOutputStream(object.uuid);
            if (source != null) {
                sources.put(source.uuid, source);
            }
            else {
                final List<MetaInfo.MetaObject> objects = sources.findCQsByOutputStream(object.uuid);
                findSources(objects, sources, token);
            }
            return;
        }
        final List<UUID> uuids = (object.getType() == EntityType.CQ) ? getCQParentUUIDs(object) : object.getDependencies();
        findSourcesForUUIDs(uuids, sources, token);
    }
    
    private static List<UUID> getCQParentUUIDs(final MetaInfo.MetaObject object) {
        final MetaInfo.CQ cq = (MetaInfo.CQ)object;
        return (cq.plan != null) ? cq.plan.getDataSources() : null;
    }
    
    private static void findSourcesForUUIDs(final Iterable<UUID> uuids, final StreamSourceMap sources, final AuthToken token) throws MetaDataRepositoryException {
        if (uuids == null) {
            return;
        }
        for (final UUID uuid : uuids) {
            if (uuid != null) {
                final MetaInfo.MetaObject dependency = Usage.metaDataRepository.getMetaObjectByUUID(uuid, token);
                findSources(dependency, sources, token);
            }
        }
    }
    
    private static List<UsageMetrics> getUsageMetrics(final Collection<MetaInfo.MetaObject> objects) {
        final List<UsageMetrics> result = new ArrayList<UsageMetrics>(objects.size());
        for (final MetaInfo.MetaObject object : objects) {
            final MetaInfo.Source source = (MetaInfo.Source)object;
            addUsageMetrics(source, result);
        }
        return result;
    }
    
    private static void addUsageMetrics(final MetaInfo.Source source, final Collection<UsageMetrics> result) {
        final com.datasphere.usagemetrics.cache.UsageMetrics cachedMetrics = Cache.getCachedMetrics(source.uuid);
        final UsageMetrics usageMetrics = new UsageMetrics(source.nsName, source.name, cachedMetrics.sourceBytes);
        result.add(usageMetrics);
    }
    
    static {
        metaDataRepository = MetadataRepository.getINSTANCE();
        instance = new Usage();
    }
    
    private static class StreamSourceMap extends HashMap<UUID, MetaInfo.MetaObject>
    {
        private Map<UUID, MetaInfo.Source> sourceMap;
        private Map<UUID, List<MetaInfo.MetaObject>> cqMap;
        
        StreamSourceMap(final AuthToken token) throws MetaDataRepositoryException {
            this.createSourceMap(token);
            this.createCQMap(token);
        }
        
        private void createSourceMap(final AuthToken token) throws MetaDataRepositoryException {
            final Set<MetaInfo.MetaObject> objects = (Set<MetaInfo.MetaObject>)Usage.metaDataRepository.getByEntityType(EntityType.SOURCE, token);
            this.sourceMap = new HashMap<UUID, MetaInfo.Source>(objects.size());
            for (final MetaInfo.MetaObject object : objects) {
                final MetaInfo.Source source = (MetaInfo.Source)object;
                this.sourceMap.put(source.outputStream, source);
            }
        }
        
        private void createCQMap(final AuthToken token) throws MetaDataRepositoryException {
            final Set<MetaInfo.MetaObject> objects = (Set<MetaInfo.MetaObject>)Usage.metaDataRepository.getByEntityType(EntityType.CQ, token);
            this.cqMap = new HashMap<UUID, List<MetaInfo.MetaObject>>(objects.size());
            for (final MetaInfo.MetaObject object : objects) {
                final MetaInfo.CQ cq = (MetaInfo.CQ)object;
                List<MetaInfo.MetaObject> cqList = this.cqMap.get(cq.stream);
                if (cqList == null) {
                    cqList = new ArrayList<MetaInfo.MetaObject>(1);
                    this.cqMap.put(cq.stream, cqList);
                }
                cqList.add(cq);
            }
        }
        
        MetaInfo.MetaObject findSourceByOutputStream(final UUID streamUUID) {
            return this.sourceMap.get(streamUUID);
        }
        
        List<MetaInfo.MetaObject> findCQsByOutputStream(final UUID streamUUID) {
            return this.cqMap.get(streamUUID);
        }
    }
}
