package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "sequence-generator", propOrder = { "description" })
public class SequenceGenerator
{
    protected String description;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "sequence-name")
    protected String sequenceName;
    @XmlAttribute(name = "catalog")
    protected String catalog;
    @XmlAttribute(name = "schema")
    protected String schema;
    @XmlAttribute(name = "initial-value")
    protected Integer initialValue;
    @XmlAttribute(name = "allocation-size")
    protected Integer allocationSize;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getSequenceName() {
        return this.sequenceName;
    }
    
    public void setSequenceName(final String value) {
        this.sequenceName = value;
    }
    
    public String getCatalog() {
        return this.catalog;
    }
    
    public void setCatalog(final String value) {
        this.catalog = value;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String value) {
        this.schema = value;
    }
    
    public Integer getInitialValue() {
        return this.initialValue;
    }
    
    public void setInitialValue(final Integer value) {
        this.initialValue = value;
    }
    
    public Integer getAllocationSize() {
        return this.allocationSize;
    }
    
    public void setAllocationSize(final Integer value) {
        this.allocationSize = value;
    }
}
