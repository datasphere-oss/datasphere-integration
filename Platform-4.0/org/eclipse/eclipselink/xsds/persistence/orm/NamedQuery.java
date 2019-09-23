package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-query", propOrder = { "description", "query", "lockMode", "hint" })
public class NamedQuery
{
    protected String description;
    @XmlElement(required = true)
    protected String query;
    @XmlElement(name = "lock-mode")
    protected LockModeType lockMode;
    protected List<QueryHint> hint;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public String getQuery() {
        return this.query;
    }
    
    public void setQuery(final String value) {
        this.query = value;
    }
    
    public LockModeType getLockMode() {
        return this.lockMode;
    }
    
    public void setLockMode(final LockModeType value) {
        this.lockMode = value;
    }
    
    public List<QueryHint> getHint() {
        if (this.hint == null) {
            this.hint = new ArrayList<QueryHint>();
        }
        return this.hint;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
