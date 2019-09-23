package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "entity-listeners", propOrder = { "entityListener" })
public class EntityListeners
{
    @XmlElement(name = "entity-listener")
    protected List<EntityListener> entityListener;
    
    public List<EntityListener> getEntityListener() {
        if (this.entityListener == null) {
            this.entityListener = new ArrayList<EntityListener>();
        }
        return this.entityListener;
    }
}
