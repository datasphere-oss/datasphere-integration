package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "write-transformer", propOrder = { "column" })
public class WriteTransformer
{
    @XmlElement(required = true)
    protected Column column;
    @XmlAttribute(name = "transformer-class")
    protected String transformerClass;
    @XmlAttribute(name = "method")
    protected String method;
    
    public Column getColumn() {
        return this.column;
    }
    
    public void setColumn(final Column value) {
        this.column = value;
    }
    
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
