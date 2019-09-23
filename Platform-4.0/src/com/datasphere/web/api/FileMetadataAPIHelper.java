package com.datasphere.web.api;

import com.datasphere.uuid.*;
import java.util.*;
import com.datasphere.runtime.fileMetaExtension.*;
import com.datasphere.utility.*;
import com.datasphere.runtime.components.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.meta.cdc.filters.*;
import com.datasphere.runtime.meta.*;

public class FileMetadataAPIHelper
{
    private static final String[] ALLOWED_TEMPLATES;
    
    public Set<FileMetadataExtension> getFileMetada(final AuthToken token, final String fullyQualifiedComponentName, final String type) throws MetaDataRepositoryException {
        MDConstants.checkNullParams("Entity Type cannot be null: ", type, token);
        final FileMetadataExtensionRepository repo = FileMetadataExtensionRepository.getInstance();
        final String domain = Utility.splitDomain(fullyQualifiedComponentName);
        final String name = Utility.splitName(fullyQualifiedComponentName);
        final String objectID = domain + "." + name;
        final EntityType entityType = EntityType.valueOf(type);
        final MetadataRepository MDR = MetadataRepository.getINSTANCE();
        final MetaInfo.MetaObject obj = MDR.getMetaObjectByName(entityType, domain, name, -1, token);
        if (obj == null) {
            throw new MetaDataRepositoryException("Object not found :" + fullyQualifiedComponentName);
        }
        final FileTrailFilter filter = new FileTrailFilter(objectID, null, null, null, 100, true, obj.getUuid());
        return repo.queryFileMetadataExtension(token, filter);
    }
    
    public String[] getMetadataEnabledTemplates() {
        return FileMetadataAPIHelper.ALLOWED_TEMPLATES;
    }
    
    public boolean isFileMetadataLineageEnabled() {
        final String flag = System.getProperty("com.datasphere.config.trackFileLineageMetadata", "false");
        return flag.equalsIgnoreCase("true");
    }
    
    static {
        ALLOWED_TEMPLATES = new String[] { "FileReader", "HDFSReader", "FileWriter", "HDFSWriter", "AzureBlobWriter", "S3Writer" };
    }
}
