package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "value-partition")
public class ValuePartition
{
    @XmlAttribute(name = "value", required = true)
    protected String value;
    @XmlAttribute(name = "connection-pool", required = true)
    protected String connectionPool;
    
    public String getValue() {
        return this.value;
    }
    
    public void setValue(final String value) {
        this.value = value;
    }
    
    public String getConnectionPool() {
        return this.connectionPool;
    }
    
    public void setConnectionPool(final String value) {
        this.connectionPool = value;
    }
}
