package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "entity-result", propOrder = { "fieldResult" })
public class EntityResult
{
    @XmlElement(name = "field-result")
    protected List<FieldResult> fieldResult;
    @XmlAttribute(name = "entity-class", required = true)
    protected String entityClass;
    @XmlAttribute(name = "discriminator-column")
    protected String discriminatorColumn;
    
    public List<FieldResult> getFieldResult() {
        if (this.fieldResult == null) {
            this.fieldResult = new ArrayList<FieldResult>();
        }
        return this.fieldResult;
    }
    
    public String getEntityClass() {
        return this.entityClass;
    }
    
    public void setEntityClass(final String value) {
        this.entityClass = value;
    }
    
    public String getDiscriminatorColumn() {
        return this.discriminatorColumn;
    }
    
    public void setDiscriminatorColumn(final String value) {
        this.discriminatorColumn = value;
    }
}
