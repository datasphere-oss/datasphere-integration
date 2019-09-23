package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "type-converter")
public class TypeConverter
{
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "data-type")
    protected String dataType;
    @XmlAttribute(name = "object-type")
    protected String objectType;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getDataType() {
        return this.dataType;
    }
    
    public void setDataType(final String value) {
        this.dataType = value;
    }
    
    public String getObjectType() {
        return this.objectType;
    }
    
    public void setObjectType(final String value) {
        this.objectType = value;
    }
}
