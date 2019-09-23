package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "sql-result-set-mapping", propOrder = { "description", "entityResult", "constructorResult", "columnResult" })
public class SqlResultSetMapping
{
    protected String description;
    @XmlElement(name = "entity-result")
    protected List<EntityResult> entityResult;
    @XmlElement(name = "constructor-result")
    protected List<ConstructorResult> constructorResult;
    @XmlElement(name = "column-result")
    protected List<ColumnResult> columnResult;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public List<EntityResult> getEntityResult() {
        if (this.entityResult == null) {
            this.entityResult = new ArrayList<EntityResult>();
        }
        return this.entityResult;
    }
    
    public List<ConstructorResult> getConstructorResult() {
        if (this.constructorResult == null) {
            this.constructorResult = new ArrayList<ConstructorResult>();
        }
        return this.constructorResult;
    }
    
    public List<ColumnResult> getColumnResult() {
        if (this.columnResult == null) {
            this.columnResult = new ArrayList<ColumnResult>();
        }
        return this.columnResult;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
