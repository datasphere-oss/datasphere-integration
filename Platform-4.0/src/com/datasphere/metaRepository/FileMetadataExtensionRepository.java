package com.datasphere.metaRepository;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;

import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.runtime.fileMetaExtension.FileMetadataExtension;
import com.datasphere.runtime.meta.cdc.filters.FileMetadataFilter;
import com.datasphere.runtime.meta.cdc.filters.FileTrailFilter;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class FileMetadataExtensionRepository
{
    private static String CLIENT_NOT_ALLOWED_ERROR;
    private FileMetadataDBOperations FileMetadataDBObject;
    
    public FileMetadataExtensionRepository() {
        this.FileMetadataDBObject = null;
        this.FileMetadataDBObject = FileMetadataDBOperations.getFileMetadataDBInstance(HazelcastSingleton.get().getMap("#startUpMap"));
    }
    
    public static FileMetadataExtensionRepository getInstance() {
        return SingletonHolder.INSTANCE;
    }
    
    public void putFileMetadataExtension(final AuthToken authToken, final Collection<FileMetadataExtension> fileMetadataExtensions) {
        if (HazelcastSingleton.isClientMember()) {
            DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), new FileMetadataExtensionPutCollection(fileMetadataExtensions));
        }
        else {
            this.FileMetadataDBObject.store(fileMetadataExtensions);
        }
    }
    
    public void putFileMetadataExtension(final AuthToken authToken, final FileMetadataExtension fileMetadataExtension) {
        if (HazelcastSingleton.isClientMember()) {
            DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), new FileMetadataExtensionPut(fileMetadataExtension));
        }
        else {
            this.FileMetadataDBObject.store(fileMetadataExtension);
        }
    }
    
    public boolean dropFileMetadataExtension(final AuthToken authToken, final String SourceURI) {
        if (HazelcastSingleton.isClientMember()) {
            return DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Boolean>)new FileMetadataExtensionDrop(SourceURI));
        }
        return this.FileMetadataDBObject.removeFileMetadata(SourceURI);
    }
    
    public Set<FileMetadataExtension> queryFileMetadataExtension(final AuthToken authToken, final FileMetadataFilter filter) {
        if (HazelcastSingleton.isClientMember()) {
            return DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Set<FileMetadataExtension>>)new FileMetadataExtensionQueryByFilter(filter));
        }
        return this.FileMetadataDBObject.query(filter);
    }
    
    public int getTotalRowsInFileMetadataRepo(final String parentComponent, final UUID uuid) {
        if (HazelcastSingleton.isClientMember()) {
            return DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Integer>)new FileMetadataExtensionGetCount(parentComponent, uuid));
        }
        return this.FileMetadataDBObject.getTotalRowsForTheParentComponent(parentComponent, uuid);
    }
    
    public Set<FileMetadataExtension> queryFileMetadataExtension(final AuthToken authToken, final FileTrailFilter filter) {
        if (HazelcastSingleton.isClientMember()) {
            return DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), (Callable<Set<FileMetadataExtension>>)new FileMetadataExtensionQueryByTrailFilter(filter));
        }
        return this.FileMetadataDBObject.query(filter);
    }
    
    static {
        FileMetadataExtensionRepository.CLIENT_NOT_ALLOWED_ERROR = "Clients are not allowed to use the API.";
    }
    
    private static class SingletonHolder
    {
        private static final FileMetadataExtensionRepository INSTANCE;
        
        static {
            INSTANCE = new FileMetadataExtensionRepository();
        }
    }
    
    public static class FileMetadataExtensionPutCollection implements RemoteCall<Void>
    {
        Collection<FileMetadataExtension> fileMetadataExtensions;
        
        public FileMetadataExtensionPutCollection(final Collection<FileMetadataExtension> fileMetadataExtensions) {
            this.fileMetadataExtensions = fileMetadataExtensions;
        }
        
        @Override
        public Void call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            instance.putFileMetadataExtension(HSecurityManager.TOKEN, this.fileMetadataExtensions);
            return null;
        }
    }
    
    public static class FileMetadataExtensionPut implements RemoteCall<Void>
    {
        FileMetadataExtension fileMetadataExtension;
        
        public FileMetadataExtensionPut(final FileMetadataExtension fileMetadataExtension) {
            this.fileMetadataExtension = fileMetadataExtension;
        }
        
        @Override
        public Void call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            instance.putFileMetadataExtension(HSecurityManager.TOKEN, this.fileMetadataExtension);
            return null;
        }
    }
    
    public static class FileMetadataExtensionDrop implements RemoteCall<Boolean>
    {
        String SourceURI;
        
        public FileMetadataExtensionDrop(final String SourceURI) {
            this.SourceURI = SourceURI;
        }
        
        @Override
        public Boolean call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            return instance.dropFileMetadataExtension(HSecurityManager.TOKEN, this.SourceURI);
        }
    }
    
    public static class FileMetadataExtensionQueryByFilter implements RemoteCall<Set<FileMetadataExtension>>
    {
        FileMetadataFilter filter;
        
        public FileMetadataExtensionQueryByFilter(final FileMetadataFilter filter) {
            this.filter = filter;
        }
        
        @Override
        public Set<FileMetadataExtension> call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            return instance.queryFileMetadataExtension(HSecurityManager.TOKEN, this.filter);
        }
    }
    
    public static class FileMetadataExtensionQueryByTrailFilter implements RemoteCall<Set<FileMetadataExtension>>
    {
        FileTrailFilter filter;
        
        public FileMetadataExtensionQueryByTrailFilter(final FileTrailFilter filter) {
            this.filter = filter;
        }
        
        @Override
        public Set<FileMetadataExtension> call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            return instance.queryFileMetadataExtension(HSecurityManager.TOKEN, this.filter);
        }
    }
    
    public static class FileMetadataExtensionGetCount implements RemoteCall<Integer>
    {
        String componentName;
        UUID uuid;
        
        public FileMetadataExtensionGetCount(final String componentName, final UUID uuid) {
            this.componentName = componentName;
            this.uuid = uuid;
        }
        
        @Override
        public Integer call() throws Exception {
            final FileMetadataExtensionRepository instance = FileMetadataExtensionRepository.getInstance();
            return instance.getTotalRowsInFileMetadataRepo(this.componentName, this.uuid);
        }
    }
}
