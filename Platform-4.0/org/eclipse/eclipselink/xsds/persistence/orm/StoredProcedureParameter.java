package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.math.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "stored-procedure-parameter")
public class StoredProcedureParameter
{
    @XmlAttribute(name = "direction")
    protected DirectionType direction;
    @XmlAttribute(name = "mode")
    protected ParameterMode mode;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "query-parameter", required = true)
    protected String queryParameter;
    @XmlAttribute(name = "optional")
    protected Boolean optional;
    @XmlAttribute(name = "type")
    protected String type;
    @XmlAttribute(name = "jdbc-type")
    protected BigInteger jdbcType;
    @XmlAttribute(name = "jdbc-type-name")
    protected String jdbcTypeName;
    
    public DirectionType getDirection() {
        return this.direction;
    }
    
    public void setDirection(final DirectionType value) {
        this.direction = value;
    }
    
    public ParameterMode getMode() {
        return this.mode;
    }
    
    public void setMode(final ParameterMode value) {
        this.mode = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getQueryParameter() {
        return this.queryParameter;
    }
    
    public void setQueryParameter(final String value) {
        this.queryParameter = value;
    }
    
    public Boolean isOptional() {
        return this.optional;
    }
    
    public void setOptional(final Boolean value) {
        this.optional = value;
    }
    
    public String getType() {
        return this.type;
    }
    
    public void setType(final String value) {
        this.type = value;
    }
    
    public BigInteger getJdbcType() {
        return this.jdbcType;
    }
    
    public void setJdbcType(final BigInteger value) {
        this.jdbcType = value;
    }
    
    public String getJdbcTypeName() {
        return this.jdbcTypeName;
    }
    
    public void setJdbcTypeName(final String value) {
        this.jdbcTypeName = value;
    }
}
