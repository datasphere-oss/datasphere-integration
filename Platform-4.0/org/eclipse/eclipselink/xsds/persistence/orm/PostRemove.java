package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "post-remove", propOrder = { "description" })
public class PostRemove
{
    protected String description;
    @XmlAttribute(name = "method-name", required = true)
    protected String methodName;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public String getMethodName() {
        return this.methodName;
    }
    
    public void setMethodName(final String value) {
        this.methodName = value;
    }
}
