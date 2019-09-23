package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-native-query", propOrder = { "description", "query", "hint" })
public class NamedNativeQuery
{
    protected String description;
    @XmlElement(required = true)
    protected String query;
    protected List<QueryHint> hint;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "result-class")
    protected String resultClass;
    @XmlAttribute(name = "result-set-mapping")
    protected String resultSetMapping;
    
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
    
    public String getResultClass() {
        return this.resultClass;
    }
    
    public void setResultClass(final String value) {
        this.resultClass = value;
    }
    
    public String getResultSetMapping() {
        return this.resultSetMapping;
    }
    
    public void setResultSetMapping(final String value) {
        this.resultSetMapping = value;
    }
}
