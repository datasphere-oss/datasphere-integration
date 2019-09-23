package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "access-methods")
public class AccessMethods
{
    @XmlAttribute(name = "get-method", required = true)
    protected String getMethod;
    @XmlAttribute(name = "set-method", required = true)
    protected String setMethod;
    
    public String getGetMethod() {
        return this.getMethod;
    }
    
    public void setGetMethod(final String value) {
        this.getMethod = value;
    }
    
    public String getSetMethod() {
        return this.setMethod;
    }
    
    public void setSetMethod(final String value) {
        this.setMethod = value;
    }
}
