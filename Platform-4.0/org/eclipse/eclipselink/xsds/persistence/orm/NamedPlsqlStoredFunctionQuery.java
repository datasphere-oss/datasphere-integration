package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-plsql-stored-function-query", propOrder = { "hint", "parameter", "returnParameter" })
public class NamedPlsqlStoredFunctionQuery
{
    protected List<QueryHint> hint;
    protected List<PlsqlParameter> parameter;
    @XmlElement(name = "return-parameter", required = true)
    protected PlsqlParameter returnParameter;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "result-set-mapping")
    protected String resultSetMapping;
    @XmlAttribute(name = "function-name", required = true)
    protected String functionName;
    
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
    
    public PlsqlParameter getReturnParameter() {
        return this.returnParameter;
    }
    
    public void setReturnParameter(final PlsqlParameter value) {
        this.returnParameter = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getResultSetMapping() {
        return this.resultSetMapping;
    }
    
    public void setResultSetMapping(final String value) {
        this.resultSetMapping = value;
    }
    
    public String getFunctionName() {
        return this.functionName;
    }
    
    public void setFunctionName(final String value) {
        this.functionName = value;
    }
}
