package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "plsql-table")
public class PlsqlTable
{
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "compatible-type")
    protected String compatibleType;
    @XmlAttribute(name = "java-type")
    protected String javaType;
    @XmlAttribute(name = "nested-type")
    protected String nestedType;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getCompatibleType() {
        return this.compatibleType;
    }
    
    public void setCompatibleType(final String value) {
        this.compatibleType = value;
    }
    
    public String getJavaType() {
        return this.javaType;
    }
    
    public void setJavaType(final String value) {
        this.javaType = value;
    }
    
    public String getNestedType() {
        return this.nestedType;
    }
    
    public void setNestedType(final String value) {
        this.nestedType = value;
    }
}
