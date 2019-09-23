package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "transformation", propOrder = { "readTransformer", "writeTransformer", "property", "accessMethods" })
public class Transformation
{
    @XmlElement(name = "read-transformer", required = true)
    protected ReadTransformer readTransformer;
    @XmlElement(name = "write-transformer")
    protected List<WriteTransformer> writeTransformer;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "optional")
    protected Boolean optional;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "mutable")
    protected Boolean mutable;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    
    public ReadTransformer getReadTransformer() {
        return this.readTransformer;
    }
    
    public void setReadTransformer(final ReadTransformer value) {
        this.readTransformer = value;
    }
    
    public List<WriteTransformer> getWriteTransformer() {
        if (this.writeTransformer == null) {
            this.writeTransformer = new ArrayList<WriteTransformer>();
        }
        return this.writeTransformer;
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
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public FetchType getFetch() {
        return this.fetch;
    }
    
    public void setFetch(final FetchType value) {
        this.fetch = value;
    }
    
    public Boolean isOptional() {
        return this.optional;
    }
    
    public void setOptional(final Boolean value) {
        this.optional = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public Boolean isMutable() {
        return this.mutable;
    }
    
    public void setMutable(final Boolean value) {
        this.mutable = value;
    }
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
}
