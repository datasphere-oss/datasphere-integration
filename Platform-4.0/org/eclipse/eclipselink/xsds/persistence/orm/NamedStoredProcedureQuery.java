package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-stored-procedure-query", propOrder = { "resultClass", "resultSetMapping", "hint", "parameter" })
public class NamedStoredProcedureQuery
{
    @XmlElement(name = "result-class")
    protected List<String> resultClass;
    @XmlElement(name = "result-set-mapping")
    protected List<String> resultSetMapping;
    protected List<QueryHint> hint;
    protected List<StoredProcedureParameter> parameter;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "procedure-name", required = true)
    protected String procedureName;
    @XmlAttribute(name = "returns-result-set")
    protected Boolean returnsResultSet;
    @XmlAttribute(name = "multiple-result-sets")
    protected Boolean multipleResultSets;
    @XmlAttribute(name = "call-by-index")
    protected Boolean callByIndex;
    
    public List<String> getResultClass() {
        if (this.resultClass == null) {
            this.resultClass = new ArrayList<String>();
        }
        return this.resultClass;
    }
    
    public List<String> getResultSetMapping() {
        if (this.resultSetMapping == null) {
            this.resultSetMapping = new ArrayList<String>();
        }
        return this.resultSetMapping;
    }
    
    public List<QueryHint> getHint() {
        if (this.hint == null) {
            this.hint = new ArrayList<QueryHint>();
        }
        return this.hint;
    }
    
    public List<StoredProcedureParameter> getParameter() {
        if (this.parameter == null) {
            this.parameter = new ArrayList<StoredProcedureParameter>();
        }
        return this.parameter;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getProcedureName() {
        return this.procedureName;
    }
    
    public void setProcedureName(final String value) {
        this.procedureName = value;
    }
    
    public Boolean isReturnsResultSet() {
        return this.returnsResultSet;
    }
    
    public void setReturnsResultSet(final Boolean value) {
        this.returnsResultSet = value;
    }
    
    public Boolean isMultipleResultSets() {
        return this.multipleResultSets;
    }
    
    public void setMultipleResultSets(final Boolean value) {
        this.multipleResultSets = value;
    }
    
    public Boolean isCallByIndex() {
        return this.callByIndex;
    }
    
    public void setCallByIndex(final Boolean value) {
        this.callByIndex = value;
    }
}
