package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "primary-key", propOrder = { "column" })
public class PrimaryKey
{
    protected List<Column> column;
    @XmlAttribute(name = "validation")
    protected IdValidation validation;
    @XmlAttribute(name = "cache-key-type")
    protected CacheKeyType cacheKeyType;
    
    public List<Column> getColumn() {
        if (this.column == null) {
            this.column = new ArrayList<Column>();
        }
        return this.column;
    }
    
    public IdValidation getValidation() {
        return this.validation;
    }
    
    public void setValidation(final IdValidation value) {
        this.validation = value;
    }
    
    public CacheKeyType getCacheKeyType() {
        return this.cacheKeyType;
    }
    
    public void setCacheKeyType(final CacheKeyType value) {
        this.cacheKeyType = value;
    }
}
