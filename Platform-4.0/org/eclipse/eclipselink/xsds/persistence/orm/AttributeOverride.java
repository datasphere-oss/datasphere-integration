package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "attribute-override", propOrder = { "description", "column" })
public class AttributeOverride
{
    protected String description;
    @XmlElement(required = true)
    protected Column column;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public Column getColumn() {
        return this.column;
    }
    
    public void setColumn(final Column value) {
        this.column = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
