package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "object-type-converter", propOrder = { "conversionValue", "defaultObjectValue" })
public class ObjectTypeConverter
{
    @XmlElement(name = "conversion-value", required = true)
    protected List<ConversionValue> conversionValue;
    @XmlElement(name = "default-object-value")
    protected String defaultObjectValue;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "data-type")
    protected String dataType;
    @XmlAttribute(name = "object-type")
    protected String objectType;
    
    public List<ConversionValue> getConversionValue() {
        if (this.conversionValue == null) {
            this.conversionValue = new ArrayList<ConversionValue>();
        }
        return this.conversionValue;
    }
    
    public String getDefaultObjectValue() {
        return this.defaultObjectValue;
    }
    
    public void setDefaultObjectValue(final String value) {
        this.defaultObjectValue = value;
    }
    
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
