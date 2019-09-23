package com.datasphere.metaRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;

public class VersionManager
{
    private static VersionManager INSTANCE;
    private boolean isMapLoaded;
    private static Logger logger;
    private IMap<String, List<Integer>> objectVersions;
    
    public static VersionManager getInstance() {
        if (!VersionManager.INSTANCE.isMapLoaded) {
            VersionManager.INSTANCE.initMaps();
        }
        return VersionManager.INSTANCE;
    }
    
    private void initMaps() {
        if (!this.isMapLoaded) {
            if (VersionManager.logger.isDebugEnabled()) {
                VersionManager.logger.debug((Object)"Loading IMaps first time from DB");
            }
            final HazelcastInstance hz = HazelcastSingleton.get();
            this.objectVersions = hz.getMap("#objectVersions");
            this.isMapLoaded = true;
        }
    }
    
    private String simplePut(final String nvUrl, final MetaInfo.MetaObject metaObject) {
        List<Integer> versions = (List<Integer>)this.objectVersions.get((Object)nvUrl);
        if (versions == null) {
            versions = new ArrayList<Integer>();
        }
        if (!versions.contains(metaObject.version)) {
            versions.add(metaObject.version);
            this.objectVersions.put(nvUrl, versions);
        }
        return nvUrl + ":" + metaObject.version;
    }
    
    private String incrementVersionAndPut(final String nvUrl, final MetaInfo.MetaObject obj) {
        final List<Integer> versions = (List<Integer>)this.objectVersions.get(nvUrl);
        Integer maxVersion;
        if (!versions.isEmpty()) {
            maxVersion = Collections.max((Collection<? extends Integer>)versions);
        }
        else {
            maxVersion = 0;
        }
        ++maxVersion;
        final String vUrl = MDConstants.makeURLWithVersion(nvUrl, maxVersion);
        versions.add(maxVersion);
        this.objectVersions.put(nvUrl, versions);
        obj.version = maxVersion;
        return vUrl;
    }
    
    private void putFirstVersion(final String nvUrl, final MetaInfo.MetaObject mObject) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Either URI or MetaObject was null", nvUrl, mObject);
        final List<Integer> vv = new ArrayList<Integer>();
        vv.add(1);
        this.objectVersions.put(nvUrl, vv);
        mObject.version = 1;
    }
    
    public boolean put(final MetaInfo.MetaObject metaObject) throws MetaDataRepositoryException {
        final String nvUrl = MDConstants.makeURLWithoutVersion(metaObject);
        if (VersionManager.logger.isDebugEnabled()) {
            VersionManager.logger.debug(("nvUrl : " + nvUrl));
        }
        String vUrl = null;
        if (metaObject.version == 0) {
            if (this.objectVersions.containsKey(nvUrl)) {
                if (metaObject.type.isNotVersionable()) {
                    VersionManager.logger.error((metaObject.type + ":" + metaObject.name + " exists, Cannot create without dropping."));
                }
                else {
                    vUrl = this.incrementVersionAndPut(nvUrl, metaObject);
                    metaObject.setUri(vUrl);
                }
            }
            else {
                vUrl = MDConstants.makeURLWithVersion(nvUrl, 1);
                metaObject.setUri(vUrl);
                this.putFirstVersion(nvUrl, metaObject);
            }
            return metaObject.getUri() != null;
        }
        if (this.objectVersions.containsKey(nvUrl)) {
            if (metaObject.type.isNotVersionable()) {
                VersionManager.logger.info((metaObject.type + ":" + metaObject.name + " exists, Cannot create without dropping."));
            }
            else {
                vUrl = this.simplePut(nvUrl, metaObject);
                if (metaObject.getUri() == null) {
                    metaObject.setUri(vUrl);
                }
            }
        }
        else {
            vUrl = this.simplePut(nvUrl, metaObject);
            if (metaObject.getUri() == null) {
                metaObject.setUri(vUrl);
            }
        }
        return metaObject.getUri() != null;
    }
    
    public Integer getLatestVersionOfMetaObject(final EntityType eType, final String namespace, final String name) {
        final String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
        Integer maxVersion = null;
        if (nvUrl != null) {
            final List<Integer> versionList = (List<Integer>)this.objectVersions.get(nvUrl);
            if (VersionManager.logger.isDebugEnabled()) {
                VersionManager.logger.debug(("version list : " + versionList));
            }
            if (versionList != null) {
                maxVersion = Collections.max((Collection<? extends Integer>)versionList);
            }
        }
        return maxVersion;
    }
    
    public List<Integer> getAllVersionsOfMetaObject(final EntityType eType, final String namespace, final String name) {
        final String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
        List<Integer> versionList = null;
        if (nvUrl != null) {
            versionList = (List<Integer>)this.objectVersions.get(nvUrl);
        }
        return versionList;
    }
    
    public void removeVersion(final EntityType eType, final String namespace, final String name, final Integer version) {
        final String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
        if (nvUrl != null) {
            final List<Integer> versionList = (List<Integer>)this.objectVersions.get(nvUrl);
            if (VersionManager.logger.isDebugEnabled()) {
                VersionManager.logger.debug(("version list : " + versionList));
            }
            if (versionList != null) {
                if (version != null) {
                    if (versionList.contains(version)) {
                        versionList.remove(version);
                    }
                }
                else {
                    final Integer maxVersion = Collections.max((Collection<? extends Integer>)versionList);
                    versionList.remove(maxVersion);
                }
                if (versionList.isEmpty()) {
                    this.objectVersions.remove(nvUrl);
                }
                else {
                    this.objectVersions.put(nvUrl, versionList);
                }
            }
        }
    }
    
    public boolean removeAllVersions(final EntityType eType, final String namespace, final String name) {
        final String nvUrl = MDConstants.makeUrlWithoutVersion(namespace, eType, name);
        if (nvUrl != null) {
            final List<Integer> versionList = (List<Integer>)this.objectVersions.get((Object)nvUrl);
            if (VersionManager.logger.isDebugEnabled()) {
                VersionManager.logger.debug((Object)("version list : " + versionList));
            }
            if (versionList != null) {
                this.objectVersions.remove((Object)nvUrl);
            }
        }
        return this.objectVersions.containsKey((Object)nvUrl);
    }
    
    public boolean clear() {
        this.objectVersions.clear();
        return this.objectVersions.isEmpty();
    }
    
    static {
        VersionManager.INSTANCE = new VersionManager();
        VersionManager.logger = Logger.getLogger((Class)VersionManager.class);
    }
}
