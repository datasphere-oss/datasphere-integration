package com.datasphere.io.file.persistence.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.metaRepository.FileMetadataExtensionRepository;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.meta.cdc.filters.FileMetadataFilter;
import com.datasphere.security.WASecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.source.lib.rollingpolicy.property.RollOverProperty;

public class FileMetadataPersistenceManager
{
    private FileMetadataExtensionRepository fileMetadataExtensionRepository;
    private String componentName;
    private Map<String, Object> adapterProperties;
    private Map<String, FileMetadataExtension> directoryFileMetadataExtensionCache;
    private UUID componentUUID;
    private String distributionID;
    
    public FileMetadataPersistenceManager(final String componentName, final UUID componentUUID, final String distributionID, final Map<String, Object> adapterProperties) {
        this.fileMetadataExtensionRepository = FileMetadataExtensionRepository.getInstance();
        this.componentName = null;
        this.adapterProperties = null;
        this.directoryFileMetadataExtensionCache = null;
        this.componentUUID = null;
        this.componentName = componentName;
        this.adapterProperties = adapterProperties;
        this.directoryFileMetadataExtensionCache = new HashMap<String, FileMetadataExtension>();
        this.componentUUID = componentUUID;
        this.distributionID = distributionID;
    }
    
    public RollOverProperty getRolloverProperty(final String directoryName) throws IOException {
        final Map<String, Object> localPropertyMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localPropertyMap.putAll(this.adapterProperties);
        FileMetadataExtension fileMetadataExtension = null;
        final Set<FileMetadataExtension> list = (Set<FileMetadataExtension>)this.fileMetadataExtensionRepository.queryFileMetadataExtension(WASecurityManager.TOKEN, new FileMetadataFilter(this.componentName, this.componentUUID, directoryName, this.distributionID));
        if (list != null && list.size() > 0) {
            fileMetadataExtension = list.iterator().next();
        }
        int sequenceNo = 0;
        if (fileMetadataExtension != null) {
            sequenceNo = (int)(Object)fileMetadataExtension.getSequenceNumber();
            final String status = fileMetadataExtension.getStatus();
            if (status.equalsIgnoreCase(FileMetadataExtension.Status.CREATED.toString())) {
                fileMetadataExtension.setStatus(FileMetadataExtension.Status.CRASHED.toString());
                this.fileMetadataExtensionRepository.putFileMetadataExtension(WASecurityManager.TOKEN, fileMetadataExtension);
            }
            if (sequenceNo != -1) {
                int sequenceStart = 0;
                final String sequenceStartString = (String)localPropertyMap.get("sequencestart");
                if (sequenceStartString != null && !sequenceStartString.isEmpty()) {
                    sequenceStart = Integer.parseInt(sequenceStartString);
                }
                int incrementSequenceBy = 1;
                final String incrementSequenceByString = (String)localPropertyMap.get("incrementsequenceby");
                if (incrementSequenceByString != null) {
                    incrementSequenceBy = Integer.parseInt(incrementSequenceByString);
                }
                final String fileLimit = (String)localPropertyMap.get("filelimit");
                if (fileLimit != null && !fileLimit.isEmpty()) {
                    final int limit = Integer.parseInt(fileLimit);
                    if (limit > 0) {
                        if (sequenceNo == limit - 1) {
                            sequenceNo = sequenceStart;
                        }
                    }
                    else {
                        sequenceNo += incrementSequenceBy;
                    }
                }
                else {
                    sequenceNo += incrementSequenceBy;
                }
                localPropertyMap.put("sequencestart", String.valueOf(sequenceNo));
            }
        }
        try {
            return new RollOverProperty(localPropertyMap);
        }
        catch (AdapterException e) {
            throw new IOException((Throwable)e);
        }
    }
    
    public void createFileMetadataEntry(final FileMetadataExtension fileMetadataExtension) {
        this.fileMetadataExtensionRepository.putFileMetadataExtension(WASecurityManager.TOKEN, fileMetadataExtension);
    }
    
    public void updateFileMetadataEntry(final FileMetadataExtension fileMetadataExtension) {
        this.fileMetadataExtensionRepository.putFileMetadataExtension(WASecurityManager.TOKEN, fileMetadataExtension);
    }
    
    public FileMetadataExtension getFileMetadataExtension(final String key) {
        FileMetadataExtension fileMetadataExtension = this.directoryFileMetadataExtensionCache.get(key);
        if (fileMetadataExtension != null) {
            return fileMetadataExtension;
        }
        fileMetadataExtension = new FileMetadataExtension();
        this.directoryFileMetadataExtensionCache.put(key, fileMetadataExtension);
        return fileMetadataExtension;
    }
    
    public void clear() {
        if (this.directoryFileMetadataExtensionCache != null) {
            this.directoryFileMetadataExtensionCache.clear();
            this.directoryFileMetadataExtensionCache = null;
        }
    }
}
