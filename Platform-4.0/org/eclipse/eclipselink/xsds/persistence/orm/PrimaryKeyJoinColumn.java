package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "primary-key-join-column")
public class PrimaryKeyJoinColumn
{
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "referenced-column-name")
    protected String referencedColumnName;
    @XmlAttribute(name = "column-definition")
    protected String columnDefinition;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getReferencedColumnName() {
        return this.referencedColumnName;
    }
    
    public void setReferencedColumnName(final String value) {
        this.referencedColumnName = value;
    }
    
    public String getColumnDefinition() {
        return this.columnDefinition;
    }
    
    public void setColumnDefinition(final String value) {
        this.columnDefinition = value;
    }
}
