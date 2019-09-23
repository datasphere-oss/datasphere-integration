package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "field-result")
public class FieldResult
{
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "column", required = true)
    protected String column;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getColumn() {
        return this.column;
    }
    
    public void setColumn(final String value) {
        this.column = value;
    }
}
