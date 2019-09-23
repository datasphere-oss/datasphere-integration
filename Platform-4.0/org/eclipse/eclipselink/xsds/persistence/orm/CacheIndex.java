package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cache-index", propOrder = { "columnName" })
public class CacheIndex
{
    @XmlElement(name = "column-name")
    protected List<String> columnName;
    @XmlAttribute(name = "updateable")
    protected Boolean updateable;
    
    public List<String> getColumnName() {
        if (this.columnName == null) {
            this.columnName = new ArrayList<String>();
        }
        return this.columnName;
    }
    
    public Boolean isUpdateable() {
        return this.updateable;
    }
    
    public void setUpdateable(final Boolean value) {
        this.updateable = value;
    }
}
