package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "collection-table", propOrder = { "joinColumn", "uniqueConstraint" })
public class CollectionTable
{
    @XmlElement(name = "join-column")
    protected List<JoinColumn> joinColumn;
    @XmlElement(name = "unique-constraint")
    protected List<UniqueConstraint> uniqueConstraint;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "catalog")
    protected String catalog;
    @XmlAttribute(name = "schema")
    protected String schema;
    @XmlAttribute(name = "creation-suffix")
    protected String creationSuffix;
    
    public List<JoinColumn> getJoinColumn() {
        if (this.joinColumn == null) {
            this.joinColumn = new ArrayList<JoinColumn>();
        }
        return this.joinColumn;
    }
    
    public List<UniqueConstraint> getUniqueConstraint() {
        if (this.uniqueConstraint == null) {
            this.uniqueConstraint = new ArrayList<UniqueConstraint>();
        }
        return this.uniqueConstraint;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getCatalog() {
        return this.catalog;
    }
    
    public void setCatalog(final String value) {
        this.catalog = value;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String value) {
        this.schema = value;
    }
    
    public String getCreationSuffix() {
        return this.creationSuffix;
    }
    
    public void setCreationSuffix(final String value) {
        this.creationSuffix = value;
    }
}
