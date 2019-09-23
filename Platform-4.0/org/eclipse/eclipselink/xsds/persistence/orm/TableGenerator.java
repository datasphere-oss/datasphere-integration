package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "table-generator", propOrder = { "description", "uniqueConstraint" })
public class TableGenerator
{
    protected String description;
    @XmlElement(name = "unique-constraint")
    protected List<UniqueConstraint> uniqueConstraint;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "table")
    protected String table;
    @XmlAttribute(name = "catalog")
    protected String catalog;
    @XmlAttribute(name = "schema")
    protected String schema;
    @XmlAttribute(name = "creation-suffix")
    protected String creationSuffix;
    @XmlAttribute(name = "pk-column-name")
    protected String pkColumnName;
    @XmlAttribute(name = "value-column-name")
    protected String valueColumnName;
    @XmlAttribute(name = "pk-column-value")
    protected String pkColumnValue;
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
    
    public List<UniqueConstraint> getUniqueConstraint() {
        if (this.uniqueConstraint == null) {
            this.uniqueConstraint = new ArrayList<UniqueConstraint>();
        }
        return this.uniqueConstraint;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getTable() {
        return this.table;
    }
    
    public void setTable(final String value) {
        this.table = value;
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
    
    public String getCreationSuffix() {
        return this.creationSuffix;
    }
    
    public void setCreationSuffix(final String value) {
        this.creationSuffix = value;
    }
    
    public String getPkColumnName() {
        return this.pkColumnName;
    }
    
    public void setPkColumnName(final String value) {
        this.pkColumnName = value;
    }
    
    public String getValueColumnName() {
        return this.valueColumnName;
    }
    
    public void setValueColumnName(final String value) {
        this.valueColumnName = value;
    }
    
    public String getPkColumnValue() {
        return this.pkColumnValue;
    }
    
    public void setPkColumnValue(final String value) {
        this.pkColumnValue = value;
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
