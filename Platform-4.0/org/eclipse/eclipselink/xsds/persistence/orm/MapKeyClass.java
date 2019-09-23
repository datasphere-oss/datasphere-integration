package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "map-key-class")
public class MapKeyClass
{
    @XmlAttribute(name = "class", required = true)
    protected String clazz;
    
    public String getClazz() {
        return this.clazz;
    }
    
    public void setClazz(final String value) {
        this.clazz = value;
    }
}
