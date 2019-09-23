package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "constructor-result", propOrder = { "columnResult" })
public class ConstructorResult
{
    @XmlElement(name = "column-result")
    protected List<ColumnResult> columnResult;
    @XmlAttribute(name = "target-class", required = true)
    protected String targetClass;
    
    public List<ColumnResult> getColumnResult() {
        if (this.columnResult == null) {
            this.columnResult = new ArrayList<ColumnResult>();
        }
        return this.columnResult;
    }
    
    public String getTargetClass() {
        return this.targetClass;
    }
    
    public void setTargetClass(final String value) {
        this.targetClass = value;
    }
}
