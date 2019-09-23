package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "tenant-discriminator-column")
public class TenantDiscriminatorColumn
{
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "context-property")
    protected String contextProperty;
    @XmlAttribute(name = "discriminator-type")
    protected DiscriminatorType discriminatorType;
    @XmlAttribute(name = "column-definition")
    protected String columnDefinition;
    @XmlAttribute(name = "length")
    protected Integer length;
    @XmlAttribute(name = "table")
    protected String table;
    @XmlAttribute(name = "primary-key")
    protected Boolean primaryKey;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getContextProperty() {
        return this.contextProperty;
    }
    
    public void setContextProperty(final String value) {
        this.contextProperty = value;
    }
    
    public DiscriminatorType getDiscriminatorType() {
        return this.discriminatorType;
    }
    
    public void setDiscriminatorType(final DiscriminatorType value) {
        this.discriminatorType = value;
    }
    
    public String getColumnDefinition() {
        return this.columnDefinition;
    }
    
    public void setColumnDefinition(final String value) {
        this.columnDefinition = value;
    }
    
    public Integer getLength() {
        return this.length;
    }
    
    public void setLength(final Integer value) {
        this.length = value;
    }
    
    public String getTable() {
        return this.table;
    }
    
    public void setTable(final String value) {
        this.table = value;
    }
    
    public Boolean isPrimaryKey() {
        return this.primaryKey;
    }
    
    public void setPrimaryKey(final Boolean value) {
        this.primaryKey = value;
    }
}
