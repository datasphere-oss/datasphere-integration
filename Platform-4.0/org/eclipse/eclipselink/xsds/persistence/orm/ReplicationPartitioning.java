package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "replication-partitioning", propOrder = { "connectionPool" })
public class ReplicationPartitioning
{
    @XmlElement(name = "connection-pool")
    protected List<String> connectionPool;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    
    public List<String> getConnectionPool() {
        if (this.connectionPool == null) {
            this.connectionPool = new ArrayList<String>();
        }
        return this.connectionPool;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
}
