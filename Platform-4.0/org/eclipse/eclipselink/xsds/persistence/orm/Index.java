package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "index", propOrder = { "columnName" })
public class Index
{
    @XmlElement(name = "column-name")
    protected List<String> columnName;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "catalog")
    protected String catalog;
    @XmlAttribute(name = "schema")
    protected String schema;
    @XmlAttribute(name = "table")
    protected String table;
    @XmlAttribute(name = "unique")
    protected Boolean unique;
    
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
    
    public String getTable() {
        return this.table;
    }
    
    public void setTable(final String value) {
        this.table = value;
    }
    
    public Boolean isUnique() {
        return this.unique;
    }
    
    public void setUnique(final Boolean value) {
        this.unique = value;
    }
}
