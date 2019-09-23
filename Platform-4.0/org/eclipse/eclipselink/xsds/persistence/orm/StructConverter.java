package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "struct-converter")
public class StructConverter
{
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "converter", required = true)
    protected String converter;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getConverter() {
        return this.converter;
    }
    
    public void setConverter(final String value) {
        this.converter = value;
    }
}
