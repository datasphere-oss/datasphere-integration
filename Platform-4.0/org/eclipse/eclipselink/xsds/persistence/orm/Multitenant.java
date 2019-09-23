package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "multitenant", propOrder = { "tenantDiscriminatorColumn", "tenantTableDiscriminator" })
public class Multitenant
{
    @XmlElement(name = "tenant-discriminator-column")
    protected List<TenantDiscriminatorColumn> tenantDiscriminatorColumn;
    @XmlElement(name = "tenant-table-discriminator")
    protected TenantTableDiscriminator tenantTableDiscriminator;
    @XmlAttribute(name = "type")
    protected MultitenantType type;
    @XmlAttribute(name = "include-criteria")
    protected Boolean includeCriteria;
    
    public List<TenantDiscriminatorColumn> getTenantDiscriminatorColumn() {
        if (this.tenantDiscriminatorColumn == null) {
            this.tenantDiscriminatorColumn = new ArrayList<TenantDiscriminatorColumn>();
        }
        return this.tenantDiscriminatorColumn;
    }
    
    public TenantTableDiscriminator getTenantTableDiscriminator() {
        return this.tenantTableDiscriminator;
    }
    
    public void setTenantTableDiscriminator(final TenantTableDiscriminator value) {
        this.tenantTableDiscriminator = value;
    }
    
    public MultitenantType getType() {
        return this.type;
    }
    
    public void setType(final MultitenantType value) {
        this.type = value;
    }
    
    public Boolean isIncludeCriteria() {
        return this.includeCriteria;
    }
    
    public void setIncludeCriteria(final Boolean value) {
        this.includeCriteria = value;
    }
}
