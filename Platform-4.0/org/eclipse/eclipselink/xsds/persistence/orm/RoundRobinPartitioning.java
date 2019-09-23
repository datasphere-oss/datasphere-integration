package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "round-robin-partitioning", propOrder = { "connectionPool" })
public class RoundRobinPartitioning
{
    @XmlElement(name = "connection-pool")
    protected List<String> connectionPool;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "replicate-writes")
    protected Boolean replicateWrites;
    
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
    
    public Boolean isReplicateWrites() {
        return this.replicateWrites;
    }
    
    public void setReplicateWrites(final Boolean value) {
        this.replicateWrites = value;
    }
}
