package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "named-stored-function-query", propOrder = { "hint", "parameter", "returnParameter" })
public class NamedStoredFunctionQuery
{
    protected List<QueryHint> hint;
    protected List<StoredProcedureParameter> parameter;
    @XmlElement(name = "return-parameter", required = true)
    protected StoredProcedureParameter returnParameter;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "result-set-mapping")
    protected String resultSetMapping;
    @XmlAttribute(name = "function-name", required = true)
    protected String functionName;
    @XmlAttribute(name = "call-by-index")
    protected Boolean callByIndex;
    
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
    
    public StoredProcedureParameter getReturnParameter() {
        return this.returnParameter;
    }
    
    public void setReturnParameter(final StoredProcedureParameter value) {
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
    
    public Boolean isCallByIndex() {
        return this.callByIndex;
    }
    
    public void setCallByIndex(final Boolean value) {
        this.callByIndex = value;
    }
}
