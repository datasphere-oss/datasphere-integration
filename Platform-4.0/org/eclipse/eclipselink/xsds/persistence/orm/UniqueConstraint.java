package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "unique-constraint", propOrder = { "columnName" })
public class UniqueConstraint
{
    @XmlElement(name = "column-name", required = true)
    protected List<String> columnName;
    @XmlAttribute(name = "name")
    protected String name;
    
    public List<String> getColumnName() {
        if (this.columnName == null) {
            this.columnName = new ArrayList<String>();
        }
        return this.columnName;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
