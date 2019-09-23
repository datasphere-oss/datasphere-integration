package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "struct", propOrder = { "field" })
public class Struct
{
    protected List<String> field;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    
    public List<String> getField() {
        if (this.field == null) {
            this.field = new ArrayList<String>();
        }
        return this.field;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
