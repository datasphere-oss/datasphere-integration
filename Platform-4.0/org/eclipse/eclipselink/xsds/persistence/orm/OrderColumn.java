package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "order-column")
public class OrderColumn
{
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "nullable")
    protected Boolean nullable;
    @XmlAttribute(name = "insertable")
    protected Boolean insertable;
    @XmlAttribute(name = "updatable")
    protected Boolean updatable;
    @XmlAttribute(name = "column-definition")
    protected String columnDefinition;
    @XmlAttribute(name = "correction-type")
    protected OrderColumnCorrectionType correctionType;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public Boolean isNullable() {
        return this.nullable;
    }
    
    public void setNullable(final Boolean value) {
        this.nullable = value;
    }
    
    public Boolean isInsertable() {
        return this.insertable;
    }
    
    public void setInsertable(final Boolean value) {
        this.insertable = value;
    }
    
    public Boolean isUpdatable() {
        return this.updatable;
    }
    
    public void setUpdatable(final Boolean value) {
        this.updatable = value;
    }
    
    public String getColumnDefinition() {
        return this.columnDefinition;
    }
    
    public void setColumnDefinition(final String value) {
        this.columnDefinition = value;
    }
    
    public OrderColumnCorrectionType getCorrectionType() {
        return this.correctionType;
    }
    
    public void setCorrectionType(final OrderColumnCorrectionType value) {
        this.correctionType = value;
    }
}
