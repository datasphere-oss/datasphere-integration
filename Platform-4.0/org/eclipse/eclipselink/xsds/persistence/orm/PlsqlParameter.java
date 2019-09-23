package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.math.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "plsql-parameter")
public class PlsqlParameter
{
    @XmlAttribute(name = "direction")
    protected DirectionType direction;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "query-parameter")
    protected String queryParameter;
    @XmlAttribute(name = "optional")
    protected Boolean optional;
    @XmlAttribute(name = "database-type")
    protected String databaseType;
    @XmlAttribute(name = "length")
    protected BigInteger length;
    @XmlAttribute(name = "scale")
    protected BigInteger scale;
    @XmlAttribute(name = "precision")
    protected BigInteger precision;
    
    public DirectionType getDirection() {
        return this.direction;
    }
    
    public void setDirection(final DirectionType value) {
        this.direction = value;
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
    
    public String getDatabaseType() {
        return this.databaseType;
    }
    
    public void setDatabaseType(final String value) {
        this.databaseType = value;
    }
    
    public BigInteger getLength() {
        return this.length;
    }
    
    public void setLength(final BigInteger value) {
        this.length = value;
    }
    
    public BigInteger getScale() {
        return this.scale;
    }
    
    public void setScale(final BigInteger value) {
        this.scale = value;
    }
    
    public BigInteger getPrecision() {
        return this.precision;
    }
    
    public void setPrecision(final BigInteger value) {
        this.precision = value;
    }
}
