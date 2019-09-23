package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "persistence-unit-defaults", propOrder = { "description", "schema", "catalog", "delimitedIdentifiers", "access", "accessMethods", "cascadePersist", "tenantDiscriminatorColumn", "entityListeners" })
public class PersistenceUnitDefaults
{
    protected String description;
    protected String schema;
    protected String catalog;
    @XmlElement(name = "delimited-identifiers")
    protected EmptyType delimitedIdentifiers;
    protected AccessType access;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlElement(name = "cascade-persist")
    protected EmptyType cascadePersist;
    @XmlElement(name = "tenant-discriminator-column")
    protected List<TenantDiscriminatorColumn> tenantDiscriminatorColumn;
    @XmlElement(name = "entity-listeners")
    protected EntityListeners entityListeners;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String value) {
        this.schema = value;
    }
    
    public String getCatalog() {
        return this.catalog;
    }
    
    public void setCatalog(final String value) {
        this.catalog = value;
    }
    
    public EmptyType getDelimitedIdentifiers() {
        return this.delimitedIdentifiers;
    }
    
    public void setDelimitedIdentifiers(final EmptyType value) {
        this.delimitedIdentifiers = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public AccessMethods getAccessMethods() {
        return this.accessMethods;
    }
    
    public void setAccessMethods(final AccessMethods value) {
        this.accessMethods = value;
    }
    
    public EmptyType getCascadePersist() {
        return this.cascadePersist;
    }
    
    public void setCascadePersist(final EmptyType value) {
        this.cascadePersist = value;
    }
    
    public List<TenantDiscriminatorColumn> getTenantDiscriminatorColumn() {
        if (this.tenantDiscriminatorColumn == null) {
            this.tenantDiscriminatorColumn = new ArrayList<TenantDiscriminatorColumn>();
        }
        return this.tenantDiscriminatorColumn;
    }
    
    public EntityListeners getEntityListeners() {
        return this.entityListeners;
    }
    
    public void setEntityListeners(final EntityListeners value) {
        this.entityListeners = value;
    }
}
