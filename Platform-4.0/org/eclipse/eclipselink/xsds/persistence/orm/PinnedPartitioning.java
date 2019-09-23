package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "pinned-partitioning")
public class PinnedPartitioning
{
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "connection-pool", required = true)
    protected String connectionPool;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getConnectionPool() {
        return this.connectionPool;
    }
    
    public void setConnectionPool(final String value) {
        this.connectionPool = value;
    }
}
