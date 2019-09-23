package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "tenant-table-discriminator")
public class TenantTableDiscriminator
{
    @XmlAttribute(name = "context-property")
    protected String contextProperty;
    @XmlAttribute(name = "type")
    protected TenantTableDiscriminatorType type;
    
    public String getContextProperty() {
        return this.contextProperty;
    }
    
    public void setContextProperty(final String value) {
        this.contextProperty = value;
    }
    
    public TenantTableDiscriminatorType getType() {
        return this.type;
    }
    
    public void setType(final TenantTableDiscriminatorType value) {
        this.type = value;
    }
}
