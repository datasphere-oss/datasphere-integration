package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "optimistic-locking", propOrder = { "selectedColumn" })
public class OptimisticLocking
{
    @XmlElement(name = "selected-column")
    protected List<Column> selectedColumn;
    @XmlAttribute(name = "type")
    protected OptimisticLockingType type;
    @XmlAttribute(name = "cascade")
    protected Boolean cascade;
    
    public List<Column> getSelectedColumn() {
        if (this.selectedColumn == null) {
            this.selectedColumn = new ArrayList<Column>();
        }
        return this.selectedColumn;
    }
    
    public OptimisticLockingType getType() {
        return this.type;
    }
    
    public void setType(final OptimisticLockingType value) {
        this.type = value;
    }
    
    public Boolean isCascade() {
        return this.cascade;
    }
    
    public void setCascade(final Boolean value) {
        this.cascade = value;
    }
}
