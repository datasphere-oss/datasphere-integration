package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "discriminator-column")
public class DiscriminatorColumn
{
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "discriminator-type")
    protected DiscriminatorType discriminatorType;
    @XmlAttribute(name = "column-definition")
    protected String columnDefinition;
    @XmlAttribute(name = "length")
    protected Integer length;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
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
}
