package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-plsql-stored-procedure-query", propOrder = { "hint", "parameter" })
public class NamedPlsqlStoredProcedureQuery
{
    protected List<QueryHint> hint;
    protected List<PlsqlParameter> parameter;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "result-class")
    protected String resultClass;
    @XmlAttribute(name = "result-set-mapping")
    protected String resultSetMapping;
    @XmlAttribute(name = "procedure-name", required = true)
    protected String procedureName;
    
    public List<QueryHint> getHint() {
        if (this.hint == null) {
            this.hint = new ArrayList<QueryHint>();
        }
        return this.hint;
    }
    
    public List<PlsqlParameter> getParameter() {
        if (this.parameter == null) {
            this.parameter = new ArrayList<PlsqlParameter>();
        }
        return this.parameter;
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
    
    public String getProcedureName() {
        return this.procedureName;
    }
    
    public void setProcedureName(final String value) {
        this.procedureName = value;
    }
}
