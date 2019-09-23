package com.datasphere.runtime.meta;

import org.apache.log4j.*;
import com.datasphere.runtime.components.*;
import com.datasphere.uuid.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;

public class ImplicitApplicationBuilder
{
    private static Logger logger;
    private Set<MetaInfo.MetaObject> components;
    private MetaInfo.Namespace namespace;
    private AuthToken authToken;
    private String applicationName;
    
    public ImplicitApplicationBuilder() {
        this.components = new LinkedHashSet<MetaInfo.MetaObject>();
    }
    
    public ImplicitApplicationBuilder addObject(final MetaInfo.MetaObject object) {
        this.getComponents().add(object);
        return this;
    }
    
    public ImplicitApplicationBuilder init(final MetaInfo.Namespace namespace, final String applicationName, final AuthToken authToken) {
        this.namespace = namespace;
        this.applicationName = applicationName;
        this.authToken = authToken;
        return this;
    }
    
    public String getApplicationName() {
        return this.applicationName;
    }
    
    public MetaInfo.Namespace getNamespace() {
        return this.namespace;
    }
    
    public MetaInfo.Flow build() {
        if (this.components.isEmpty() || this.namespace == null || this.applicationName == null || this.authToken == null) {
            throw new RuntimeException("Build has missing settings.");
        }
        MetaInfo.Flow application = new MetaInfo.Flow();
        application.getMetaInfoStatus().setAdhoc(true).setAnonymous(true);
        application.construct(EntityType.APPLICATION, this.getApplicationName(), this.getNamespace(), null, 0, 0L);
        for (final MetaInfo.MetaObject metaObject : this.getComponents()) {
            metaObject.addReverseIndexObjectDependencies(application.getUuid());
            try {
                MetadataRepository.getINSTANCE().updateMetaObject(metaObject, HSecurityManager.TOKEN);
            }
            catch (MetaDataRepositoryException e) {
                ImplicitApplicationBuilder.logger.warn((Object)e.getMessage());
            }
            application.addObject(metaObject.getType(), metaObject.getUuid());
        }
        try {
            MetadataRepository.getINSTANCE().putMetaObject(application, this.getAuthToken());
            application = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(application.getUuid(), this.getAuthToken());
        }
        catch (MetaDataRepositoryException e2) {
            ImplicitApplicationBuilder.logger.warn((Object)e2.getMessage());
        }
        return application;
    }
    
    public Set<MetaInfo.MetaObject> getComponents() {
        return this.components;
    }
    
    public AuthToken getAuthToken() {
        return this.authToken;
    }
    
    @Override
    public String toString() {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString((Object)this);
        }
        catch (JsonProcessingException e) {
            ImplicitApplicationBuilder.logger.warn((Object)e.getMessage());
            return null;
        }
    }
    
    static {
        ImplicitApplicationBuilder.logger = Logger.getLogger((Class)ImplicitApplicationBuilder.class);
    }
}
