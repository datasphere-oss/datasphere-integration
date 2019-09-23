package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "plsql-record", propOrder = { "field" })
public class PlsqlRecord
{
    protected List<PlsqlParameter> field;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "compatible-type")
    protected String compatibleType;
    @XmlAttribute(name = "java-type")
    protected String javaType;
    
    public List<PlsqlParameter> getField() {
        if (this.field == null) {
            this.field = new ArrayList<PlsqlParameter>();
        }
        return this.field;
    }
    
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
}
