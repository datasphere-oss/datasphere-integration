package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "conversion-value")
public class ConversionValue
{
    @XmlAttribute(name = "data-value", required = true)
    protected String dataValue;
    @XmlAttribute(name = "object-value", required = true)
    protected String objectValue;
    
    public String getDataValue() {
        return this.dataValue;
    }
    
    public void setDataValue(final String value) {
        this.dataValue = value;
    }
    
    public String getObjectValue() {
        return this.objectValue;
    }
    
    public void setObjectValue(final String value) {
        this.objectValue = value;
    }
}
