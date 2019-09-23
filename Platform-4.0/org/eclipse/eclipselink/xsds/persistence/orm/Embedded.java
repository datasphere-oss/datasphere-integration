package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "embedded", propOrder = { "attributeOverride", "associationOverride", "property", "accessMethods", "field" })
public class Embedded
{
    @XmlElement(name = "attribute-override")
    protected List<AttributeOverride> attributeOverride;
    @XmlElement(name = "association-override")
    protected List<AssociationOverride> associationOverride;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    protected Field field;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    
    public List<AttributeOverride> getAttributeOverride() {
        if (this.attributeOverride == null) {
            this.attributeOverride = new ArrayList<AttributeOverride>();
        }
        return this.attributeOverride;
    }
    
    public List<AssociationOverride> getAssociationOverride() {
        if (this.associationOverride == null) {
            this.associationOverride = new ArrayList<AssociationOverride>();
        }
        return this.associationOverride;
    }
    
    public List<Property> getProperty() {
        if (this.property == null) {
            this.property = new ArrayList<Property>();
        }
        return this.property;
    }
    
    public AccessMethods getAccessMethods() {
        return this.accessMethods;
    }
    
    public void setAccessMethods(final AccessMethods value) {
        this.accessMethods = value;
    }
    
    public Field getField() {
        return this.field;
    }
    
    public void setField(final Field value) {
        this.field = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
}
