package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "read-transformer")
public class ReadTransformer
{
    @XmlAttribute(name = "transformer-class")
    protected String transformerClass;
    @XmlAttribute(name = "method")
    protected String method;
    
    public String getTransformerClass() {
        return this.transformerClass;
    }
    
    public void setTransformerClass(final String value) {
        this.transformerClass = value;
    }
    
    public String getMethod() {
        return this.method;
    }
    
    public void setMethod(final String value) {
        this.method = value;
    }
}
